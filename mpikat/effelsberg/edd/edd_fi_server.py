import tornado
import logging
import signal
import socket
import time
import errno
import threading
import coloredlogs
import numpy as np
import time
import ctypes as C
from datetime import datetime
from threading import Thread, Event
from optparse import OptionParser
from collections import namedtuple
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, Int, request, return_reply)

log = logging.getLogger("mpikat.edd_fi_server")

class StopEventException(Exception):
    pass

class FitsWriterNotConnected(Exception):
    pass

class FitsWriterConnectionManager(Thread):
    def __init__(self, ip, port):
        Thread.__init__(self)
        self._address = (ip, port)
        self._shutdown = Event()
        self._has_connection = Event()
        self._server_socket = None
        self._transmit_socket = None
        self._reset_server_socket()

    def _reset_server_socket(self):
        if self._server_socket:
            self._server_socket.close()
        log.debug("Creating the FITS writer TCP listening socket")
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.setblocking(False)
        log.debug("Binding to {}".format(self._address))
        self._server_socket.bind(self._address)
        self._server_socket.listen(1)

    def accept_connection(self):
        log.debug("Accepting connections on FW server socket")
        while not self._shutdown.is_set():
            try:
                transmit_socket, addr = self._server_socket.accept()
                self._has_connection.set()
                log.info("Received connection from {}".format(addr))
                return transmit_socket
            except socket.error as error:
                error_id = error.args[0]
                if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                    time.sleep(1)
                else:
                    log.exception("Unexpected error on socket accept: {}".format(str(error)))
                    raise error
            except Exception as error:
                log.exception("Unexpected error on socket accept: {}".format(str(error)))
                raise error

    def drop_connection(self):
        if self._transmit_socket:
            self._transmit_socket.shutdown(socket.SHUT_RDWR)
            self._transmit_socket.close()
            self._transmit_socket = None
            self._has_connection.clear()

    def get_transmit_socket(self, timeout=2):
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            if self._has_connection.is_set():
                return self._transmit_socket
            else:
                time.sleep(0.1)
        raise FitsWriterNotConnected

    def stop(self):
        self._shutdown.set()

    def run(self):
        while not self._shutdown.is_set():
            try:
                if not self._has_connection.is_set():
                    self._transmit_socket = self.accept_connection()
                else:
                    time.sleep(1)
            except Exception as error:
                log.exception(str(error))
                continue
        self.drop_connection()
        self._server_socket.close()


class FitsInterfaceServer(AsyncDeviceServer):
    """
    Class providing an interface between EDD processes and the
    Effelsberg FITS writer
    """
    VERSION_INFO = ("edd-fi-server-api", 1, 0)
    BUILD_INFO = ("edd-fi-server-implementation", 0, 1, "")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    PROTOCOL_INFO = ProtocolFlags(5, 0, set([
        ProtocolFlags.MULTI_CLIENT,
        ProtocolFlags.MESSAGE_IDS,
    ]))

    def __init__(self, interface, port, capture_interface, capture_port, fw_ip, fw_port):
        """
        @brief Initialization of the FitsInterfaceServer object

        @param  interface          Interface address to serve on
        @param  port               Port number to serve on
        @param  capture_interface  Interface to capture data on from instruments
        @param  capture_port       Port to capture data on from instruments
        @param  fw_ip              IP address of the FITS writer
        @param  fw_port            Port number to connected to on FITS writer
        """
        self._configured = False
        self._no_active_beams = None
        self._nchannels = None
        self._integ_time = None
        self._blank_phase = None
        self._capture_interface = capture_interface
        self._capture_port = capture_port
        self._fw_connection_manager = FitsWriterConnectionManager(fw_ip, fw_port)
        self._capture_thread = None
        self._shutdown = False
        super(FitsInterfaceServer, self).__init__(interface, port)

    def start(self):
        """
        @brief   Start the server
        """
        self._fw_connection_manager.start()
        super(FitsInterfaceServer, self).start()

    def stop(self):
        """
        @brief   Stop the server
        """
        self._shutdown = True
        self._stop_capture()
        self._fw_connection_manager.stop()
        super(FitsInterfaceServer, self).stop()

    @property
    def nbeams(self):
        return self._active_beams_sensor.value()

    @nbeams.setter
    def nbeams(self, value):
        self._active_beams_sensor.set_value(value)

    @property
    def nchannels(self):
        return self._nchannels_sensor.value()

    @nchannels.setter
    def nchannels(self, value):
        self._nchannels_sensor.set_value(value)

    @property
    def integration_time(self):
        return self._integration_time_sensor.value()

    @integration_time.setter
    def integration_time(self, value):
        self._integration_time_sensor.set_value(value)

    @property
    def nblank_phases(self):
        return self._nblank_phases_sensor.value()

    @nblank_phases.setter
    def nblank_phases(self, value):
        self._nblank_phases_sensor.set_value(value)

    def setup_sensors(self):
        """
        @brief   Setup monitoring sensors
        """
        self._device_status_sensor = Sensor.discrete(
            "device-status",
            description="Health status of FIServer",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status_sensor)
        self._active_beams_sensor = Sensor.float(
            "nbeams",
            description="Number of beams that are currently active",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._active_beams_sensor)
        self._nchannels_sensor = Sensor.float(
            "nchannels",
            description="Number of channels in each beam",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nchannels_sensor)
        self._integration_time_sensor = Sensor.float(
            "integration-time",
            description="The integration time for each beam",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._integration_time_sensor)
        self._nblank_phases_sensor = Sensor.integer(
            "nblank-phases",
            description="The number of blank phases",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nblank_phases_sensor)

    def _stop_capture(self):
        if self._capture_thread:
            log.debug("Cleaning up capture thread")
            self._capture_thread.stop()
            self._capture_thread.join()
            self._capture_thread = None
            log.debug("Capture thread cleaned")

    @request(Int(), Int(), Int(), Int())
    @return_reply()
    def request_configure(self, req, beams, channels, int_time, blank_phases):
        """
        @brief    Configure the FITS interface server

        @param   beams          The number of beams expected
        @param   channels       The number of channels expected
        @param   int_time       The integration time (milliseconds int)
        @param   blank_phases   The number of blank phases (1-4)

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """

        message = "nbeams={}, nchannels={}, integration_time={}, nblank_phases={}".format(
            beams, channels, int_time, blank_phases)
        log.info("Configuring FITS interface server with params: {}".format(message))
        self.nbeams = beams
        self.nchannels = channels
        self.integration_time = int_time
        self.nblank_phases = blank_phases
        self._fw_connection_manager.drop_connection()
        self._stop_capture()
        self._configured = True
        return ("ok",)

    @request()
    @return_reply()
    def request_start(self, req):
        """
        @brief    Start the FITS interface server capturing data

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        log.info("Received start request")
        if not self._configured:
            msg = "FITS interface server is not configured"
            log.error(msg)
            return ("fail", msg)
        try:
            fw_socket = self._fw_connection_manager.get_transmit_socket()
        except Exception as error:
            log.exception(str(error))
            return ("fail", str(error))
        log.info("Starting FITS interface capture")
        self._stop_capture()
        buffer_size = 4 * (self.nchannels + 2)
        handler = R2SpectrometerHandler(2, self.nchannels, self.integration_time,
            self.nblank_phases, fw_socket)
        self._capture_thread = CaptureData(self._capture_interface,
            self._capture_port, buffer_size, handler)
        self._capture_thread.start()
        return ("ok",)

    @request()
    @return_reply()
    def request_stop(self, req):
        """
        @brief    Stop the FITS interface server capturing data

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        log.info("Received stop request")
        if not self._configured:
            msg = "FITS interface server is not configured"
            log.error(msg)
            return ("fail", msg)
        log.info("Stopping FITS interface capture")
        self._stop_capture()
        self._fw_connection_manager.drop_connection()
        return ("ok",)


class CaptureData(Thread):
    """
    @brief     Captures formatted data from a UDP socket
    """
    def __init__(self, ip, port, buffer_size, handler):
        Thread.__init__(self, name=self.__class__.__name__)
        self._address = (ip, port)
        self._buffer_size = buffer_size
        self._stop_event = Event()
        self._handler = handler

    def _reset_socket(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setblocking(False)
        self._socket.bind(self._address)

    def stop(self):
        self._stop_event.set()

    def _flush(self):
        log.debug("Flushing capture socket")
        flush_count = 0
        while True:
            try:
                message, addr = self._socket.recvfrom(self._buffer_size)
                flush_count += 1
            except:
                break
        log.debug("Flushed {} messages".format(flush_count))

    def _capture(self):
        log.debug("Starting data capture")
        while not self._stop_event.is_set():
            try:
                data, addr = self._socket.recvfrom(self._buffer_size)
                log.debug("Received {} byte message from {}".format(len(data), addr))
                self._handler(data)
            except socket.error as error:
                error_id = error.args[0]
                if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                    time.sleep(0.01)
                    continue
                else:
                    raise error
        log.debug("Stopping data capture")

    def run(self):
        self._reset_socket()
        try:
            self._flush()
            self._capture()
        except Exception as error:
            log.exception("Error during capture: {}".format(str(error)))
        finally:
            self._socket.close()


class RoachPacket(object):
    def __init__(self, raw_data, nchannels):
        self._raw_data = raw_data
        self._nchannels = nchannels
        self._data = self._get_packet_struct().from_buffer_copy(self._raw_data)

    def _get_packet_struct(self):
        class RoachPacketFormat(C.BigEndianStructure):
            _fields_ = [
                ("header", C.c_ulonglong),
                ("data", C.c_uint32 * self._nchannels)
            ]
        return RoachPacketFormat

    @property
    def sequence_number(self):
        return self._data.header & 0x00ffffffffffffff

    @property
    def phase(self):
        return (self._data.header & 0xf000000000000000) >> 60

    @property
    def polarisation(self):
        return (self._data.header & 0x0f00000000000000) >> 56

    @property
    def data(self):
        return self._data.data

    def __repr__(self):
        return "<RoachPacket, seq={}, phase={}, pol={}>".format(
            self.sequence_number, self.phase, self.polarisation)


def isotime():
    return "{}UTC".format(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-2])

def build_fw_type(nsections, nchannels):
    class FWData(C.LittleEndianStructure):
        _fields_ = [
            ('section_id', C.c_uint32),
            ('nchannels', C.c_uint32),
            ('data', C.c_float*nchannels)
        ]

    class FWPacket(C.LittleEndianStructure):
        _fields_ = [
            ("data_type", C.c_char*4),
            ("channel_data_type", C.c_char*4),
            ("packet_size", C.c_uint32),
            ("backend_name", C.c_char*8),
            ("timestamp", C.c_char*28),
            ("integration_time", C.c_uint32),
            ("blank_phases", C.c_uint32),
            ("nsections", C.c_uint32),
            ("blocking_factor", C.c_uint32),
            ("sections", FWData * nsections)
        ]
    return FWPacket

def build_fw_object(nsections, nchannels, timestamp, integration_time, blank_phases):
    packet_format = build_fw_type(nsections, nchannels)
    packet = packet_format()
    packet.data_type = "EEEI"
    packet.channel_data_type = "F   "
    packet.packet_size = C.sizeof(packet_format)
    packet.backend_name = "EDD     "
    packet.timestamp = timestamp
    packet.integration_time = integration_time
    packet.blank_phases = blank_phases
    packet.nsections = nsections
    packet.blocking_factor = 1
    for ii in range(nsections):
        packet.sections[ii].section_id = ii + 1
        packet.sections[ii].nchannels = nchannels
        C.addressof(packet.sections[ii].data), 0, C.sizeof(packet.sections[ii].data)
    return packet


class R2SpectrometerHandler(object):
    """
    @brief Aggregates spectrometer data from polarization channel 1 and 2 for the given time
           before sending to the fits writer
    """
    def __init__(self, nsections, nchannels, integration_time, nphases, transmit_socket, max_age=1.0):
        self._nsections = nsections
        self._nchannels = nchannels
        self._integration_time = integration_time
        self._nphases = nphases
        self._transmit_socket = transmit_socket
        self._active_packets = {}
        self._max_age = max_age
        self._fw_packet_wrapper = namedtuple('FWPacketWrapper',
            ['timestamp', 'hits', 'packet'])

    def __call__(self, raw_data):
        packet = RoachPacket(raw_data, self._nchannels)
        log.debug("Aggregate received packet: {}".format(packet))
        key = packet.sequence_number - packet.polarisation
        if key not in self._active_packets:
            fw_packet = build_fw_object(self._nsections, self._nchannels, isotime(),
                self._integration_time, self._nphases)
            fw_packet.sections[packet.polarisation].data[:] = packet.data
            self._active_packets[key] = self._fw_packet_wrapper(time.time(), 1, fw_packet)
        else:
            self._active_packets[key]['hits'] += 1
            self._active_packets[key]['packet'].sections[packet.polarisation].data[:] = packet.data
        self.flush()

    def flush(self):
        now = time.time()
        for key in sorted(self._active_packets.iterkeys()):
            timestamp, hits, fw_packet = self._active_packets[key]
            if ((now - timestamp) > self._max_age) or (hits == self._nsections):
                self._transmit_socket.send(bytearray(fw_packet))
                del self._active_packets[key]



@tornado.gen.coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down FITS writer interface server")
    yield server.stop()
    ioloop.stop()

def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('', '--host', dest='host', type=str,
        help='Host interface to bind to', default='127.0.0.1')
    parser.add_option('-p', '--port', dest='port', type=long,
        help='Port number to bind to', default=5000)
    parser.add_option('', '--cap-ip', dest='cap_ip', type=str,
        help='Host interface to bind to for data capture', default='127.0.0.1')
    parser.add_option('', '--cap-port', dest='cap_port', type=long,
        help='Port number to bind to for data capture', default=5001)
    parser.add_option('', '--fw-ip', dest='fw_ip', type=str,
        help='FITS writer interface to bind to for data trasmission', default='127.0.0.1')
    parser.add_option('', '--fw-port', dest='fw_port', type=long,
        help='FITS writer port number to bind to for data transmission', default=5002)
    parser.add_option('', '--log-level', dest='log_level', type=str,
        help='Defauly logging level', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=log)
    log.setLevel(opts.log_level.upper())
    ioloop = tornado.ioloop.IOLoop.current()
    server = FitsInterfaceServer(opts.host, opts.port, opts.cap_ip, opts.cap_port, opts.fw_ip, opts.fw_port)
    # Hook up to SIGINT so that ctrl-C results in a clean shutdown
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))
    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))
    ioloop.add_callback(start_and_display)
    #ioloop.add_callback(server.start)
    ioloop.start()

if __name__ == "__main__":
    main()
