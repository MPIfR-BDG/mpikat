import tornado
import logging
import signal
import socket
import time
import errno
import struct
import coloredlogs
import inspect
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import base64
import Queue
import ctypes as C
from StringIO import StringIO
from threading import Thread, Event
from optparse import OptionParser
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags
from katcp.kattypes import (Int, request, return_reply)
matplotlib.use("Agg")

log = logging.getLogger("mpikat.paf_fi_server")

DEFAULT_BLOB = "iVBORw0KGgoAAAANSUhEUgAAAPAAAACgCAYAAAAy2+FlAAAABHNCSVQICAgIfAhkiAAAAAlwSFlzAAAGJgA\
ABiYBnxM6IwAAADl0RVh0U29mdHdhcmUAbWF0cGxvdGxpYiB2ZXJzaW9uIDIuMi4zLCBodHRwOi8vbWF0cGxvdGxpYi5vcmcvIx\
REBQAABt1JREFUeJzt3D9IW/sbx/FPfr3QFi3VgEPjUJoOgpCho6sdUhA6SALpkEFBcJIqCUQkUBUdC0JBunYp7RYsUYfaQIdIU\
woiWCr4B5UEWmqR4qCNnDvcKuTe37W38Zykj32/NsXD90Hy5hwjeXyO4zgCYNL/6j0AgOoRMGAYAQOGETBgGAEDhhEwYBgBA4YR\
MGAYAQOGETBgGAEDhhEwYBgBA4YRMGAYAQOGETBgGAEDhv1Rj0OLxaJWVlbU0NBQj+OBmtjf31d7e7sCgYBnZ9TlDryysqL19fV\
6HA3UzPr6ulZWVjw9oy534IaGBoVCIXV0dNTjeODc4G9gwDACBgwjYMAwAgYMI2DAMAIGDCNgwDACBgwjYMAwAgYMI2DAMAIGDC\
NgwLCqP41UKBQ0OzurcrmssbEx7e3taXJyUo7jKJVKye/3K51Oq7W1Vf39/W7ODOC7qgPOZDIaHx/Xo0ePVCqVtLi4qGg0qosXL\
2p+fl4XLlxQZ2enPnz4UHFdPp/X8vKyQqHQmYcHfneuPUI7jiOfz3fydaFQ0KtXr5TL5dw6AsDfVH0Hvnv3rsbHx1Uul5XNZhWJ\
RDQxMSHHcTQ8PKx79+5pc3NTc3NzFdfxIX7APT7HcZxaH5rP5yURM863WrzOeRcaMIyAAcMIGDCMgAHDCBgwjIABwwgYMIyAAcM\
IGDCMgAHDCBgwjIABwwgYMIyAAcMIGDCMgAHDCBgwzLOtlJlMRtvb22pqatLAwICbMwP4ruo7cCaTUTqdVktLi0qlkhYWFhSNRh\
WPxzU/P6+enh4lk0nt7OxUXHe8lRLA2Xm2lfLg4ECjo6NKJpNuHQHgb6oO+Hgr5adPn5TNZnX79m09e/ZMT548UTgcVl9fnxzH0\
cuXLyuu6+joYCc04BK2UgIeYSslgFMRMGAYAQOGETBgGAEDhhEwYBgBA4YRMGAYAQOGETBgGAEDhhEwYBgBA4YRMGAYAQOGETBg\
GAEDhnm2lXJmZka7u7tqbGxUX1+fmzMD+M6zrZSrq6saHBzUxsZGxXVspQTc49lWSgDeq/oR+ngrZblcVjabVSQS0cTEhBzH0fD\
wsA4PD/Xw4UPduHGj4joW2QHuYSsl4BG2UgI4FQEDhhEwYBgBA4YRMGAYAQOGETBgGAEDhhEwYBgBA4YRMGAYAQOGETBgGAEDhh\
EwYBgBA4YRMGBY1St1pqam9O3bNwWDQXV3d0uSXrx4offv38vn86m3t1fT09Pa2trS0NCQ2traXBsawF+qvgN//PhRiURCb9++P\
flePp9XMpnUly9f1NzcrJGREYXDYRWLxYqfYSsl4I7/fAd+/fq1Hj9+fPL11atX//Vnj9dsLS0taW1tTclk8gwjAvg3VS+1m5qa\
0uHhoW7evKlgMKijoyOVSqWTR+h4PK6uri7FYjGFw2GFQqGTa1lqh99BLV7nbKUEPMJWSgCnImDAMAIGDCNgwDACBgwjYMAwAgY\
MI2DAMAIGDCNgwDACBgwjYMAwAgYMI2DAMAIGDCNgwDACBgzzbCtlIpFQsVhUd3e3crmcLl265NrQAP7i2VbKo6MjPX36VHfu3K\
m4jq2UgHs820r57t07ff36VYuLi8rlcv8IGcDZebaVMpFISJIePHigVCpV8QjNUjv8DthKCRjGVkoApyJgwDACBgwjYMAwAgYMI\
2DAMAIGDCNgwDACBgwjYMAwAgYMI2DAsKo/0H8W+/v7Wl9fr8fRQM0sLy8rGAx6ekZdAm5vb/f8jOOlAaFQyPOzmIM5/p9gMOj5\
a70uAQcCAQUCgZqc9at8ZJE5KjGHO+ryeWAA7uBNLMCwujxCe+lX2Jb5oxl6e3s1PT2tra0tDQ0Nqa2tzdXzC4WCZmdnVS6XNTY\
2pr29PU1OTspxHKVSKc3MzGh3d1eNjY3q6+tz9eyfmSOTyWh7e1tNTU0aGBio2xx+v1/pdFqtra3q7+/3bA4vnLs7cLXbMms5Q3\
Nzs0ZGRhQOh1UsFl0/P5PJKJ1Oq6WlRaVSSQsLC4pGo4rH45qfn9fq6qoGBwe1sbHh+tk/M0dPT4+SyaR2dnbqOsfz58/V2dnp6\
QxeMX8H/hW2Zf7sDJK0tLSktbU1JZPJM5//I47jyOfzeX7Oz85xcHCg0dHRmvwOTpujUCjo8uXLWl1dNXcHPndvYp1lW2atZojH\
4+rq6lIsFlM4HHb9Xxlv3rzR3NycyuWyrl+/rkgkoomJCTmOo+HhYc3MzOjz58+6cuWKp4/QP5rj/v37unbtmm7duqVYLFa3Ofx\
+vzY3NzU3N0fAAGrn3P0NDPxOCBgwjIABwwgYMIyAAcMIGDCMgAHDCBgwjIABwwgYMIyAAcP+BIRuVF8qjgxoAAAAAElFTkSuQmCC"

sensor_queue = Queue.Queue()


class StopEventException(Exception):
    pass


class FitsWriterNotConnected(Exception):
    pass


class FitsWriterConnectionManager(Thread):
    """
    A class to handle TCP connections to the APEX FITS writer.

    This class implements a TCP/IP server that can accept connections from
    the FITS writer. Upon acceptance, the new communication socket is stored
    and made available to any system that produces FITS writer compatible
    data.
    """

    def __init__(self, ip, port):
        """
        @brief    Construct a new instance

        @param     ip    The IP address to serve on
        @param     port  The port to serve on
        """
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
        self._server_socket.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.setsockopt(
            socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self._server_socket.setblocking(False)
        log.debug("Binding to {}".format(self._address))
        self._server_socket.bind(self._address)
        self._server_socket.listen(1)

    def _accept_connection(self):
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
                    log.exception(
                        "Unexpected error on socket accept: {}".format(str(error)))
                    raise error
            except Exception as error:
                log.exception(
                    "Unexpected error on socket accept: {}".format(str(error)))
                raise error

    def drop_connection(self):
        """
        @breif   Drop any current FITS writer connection
        """
        if self._transmit_socket:
            try:
                self._transmit_socket.shutdown(socket.SHUT_RDWR)
                self._transmit_socket.close()
                self._transmit_socket = None
            except Exception as error:
                log.warning(("Caught exception while dropping transmit "
                             "socket: {}").format(str(error)))
            self._has_connection.clear()

    def get_transmit_socket(self, timeout=10):
        """
        @brief   Get the active FITS writer connection

        @param   timeout   The time to wait for a connection to become available
        """
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            if self._has_connection.is_set():
                return self._transmit_socket
            else:
                time.sleep(0.1)
        raise FitsWriterNotConnected

    def stop(self):
        """
        @brief   Stop the server
        """
        self._shutdown.set()

    def run(self):
        while not self._shutdown.is_set():
            try:
                if not self._has_connection.is_set():
                    self._transmit_socket = self._accept_connection()
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
    VERSION_INFO = ("paf-fi-server-api", 1, 0)
    BUILD_INFO = ("paf-fi-server-implementation", 0, 1, "")
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
        self._fw_connection_manager = FitsWriterConnectionManager(
            fw_ip, fw_port)
        self._capture_thread = None
        self._shutdown = False
        self._mode = None
        self._fw_soc = None
        #self._paf_handler = PafHandler(self._mode, self._fw_soc)
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
        self._mode_sensor = Sensor.string(
            "fi-mode",
            description="FITS interface mode",
            default="passive",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._mode_sensor)
        self._integration_time_sensor = Sensor.float(
            "integration-time",
            description="The integration time for each beam",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._integration_time_sensor)
        self._nchannels_sensor = Sensor.integer(
            "nchannels",
            description="Number of channels in each beam",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nchannels_sensor)
        self._pol_type_sensor = Sensor.integer(
            "pol-type",
            description="Data sent by paf backend",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._pol_type_sensor)
        self._nblank_phases_sensor = Sensor.integer(
            "nblank-phases",
            description="The number of blank phases",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nblank_phases_sensor)
        self._center_freq_sensor = Sensor.float(
            "center-frequency in MHz",
            description="Center frequency",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._center_freq_sensor)
        self._channel_bw_sensor = Sensor.float(
            "channel-bandwidth in MHz",
            description="Channel bandwidth in MHz",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._channel_bw_sensor)
        self._beam_pngs = {}
        for beam_id in range(36):
            self._beam_pngs[beam_id] = Sensor.string(
                "BEAM%02d_PNG" % beam_id,
                description="Data of beam {} sent from the back-end".format(
                    beam_id),
                default=DEFAULT_BLOB,
                initial_status=Sensor.UNKNOWN)
            self.add_sensor(self._beam_pngs[beam_id])
        self._update_sensors_callback = tornado.ioloop.PeriodicCallback(
            self.update_sensors, 10000)
        self._update_sensors_callback.start()

    def update_sensors(self):
        #current_qsize = sensor_queue.qsize()
        # for i in range(current_qsize):
        #    timestamp, metadata, data = sensor_queue.get()
        log.debug("Updating sensor values and making beam plots")
        has_data = False
        while True:
            try:
                timestamp, metadata, data = sensor_queue.get(False)
                has_data = True
            except Queue.Empty:
                break
        if not has_data:
            log.info("No updates available for sensors")
            return
        self._integration_time_sensor.set_value(metadata.integ_time)
        self._nchannels_sensor.set_value(metadata.nchannels)
        self._pol_type_sensor.set_value(metadata.pol_type)
        self._nblank_phases_sensor.set_value(data.blank_phases)
        self._center_freq_sensor.set_value(metadata.center_freq)
        self._channel_bw_sensor.set_value(metadata.channel_bw)
        #for i in range(36):
        #    self.plot_beam_data(i, data)

    def zton(self, x):
        x = np.asarray(x)
        x[x == 0] = np.nan
        return x

    def plot_beam_data(self, beam_id, beam_data):
        plt.cla()
        plt.xlabel('Channels')
        plt.ylabel('Power')
        x = beam_data
        index = beam_id * 4
        plt.plot(np.arange(x.sections[index + 0].nchannels),
                 self.zton(x.sections[index + 0].data))
        plt.plot(np.arange(x.sections[index + 1].nchannels),
                 self.zton(x.sections[index + 1].data))
        plt.plot(np.arange(x.sections[index + 2].nchannels),
                 self.zton(x.sections[index + 2].data))
        plt.plot(np.arange(x.sections[index + 3].nchannels),
                 self.zton(x.sections[index + 3].data))
        beam = StringIO()
        plt.savefig(beam, format='png', dpi=100)
        beam.seek(0)
        beam_png = beam.buf
        beam_png = base64.b64encode(beam_png).replace("\n", "")
        self._beam_pngs[beam_id].set_value(beam_png)

    def _stop_capture(self):
        if self._capture_thread:
            log.debug("Cleaning up capture thread")
            self._capture_thread.stop()
            self._capture_thread.join()
            self._capture_thread = None
            log.debug("Capture thread cleaned")

    @request(Int())
    @return_reply()
    def request_configure(self, req, fi_status):
        """
        @brief    Configure the FITS interface server

        @param    fi_status    FITS interface status: for active(1), inactive(0)

        @return   katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        if fi_status == 1:
            message = "FITS interface is active({})".format(fi_status)
        elif fi_status == 0:
            message = "FITS interface is passive({})".format(fi_status)
        else:
            message = "Invalid mode: {}".format(fi_status)
            return ("fail", message)
        log.info("Configuring FITS interface server with params: {}".format(message))
        self._mode = fi_status
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
        if self._mode == 1:
            try:
                fw_socket = self._fw_connection_manager.get_transmit_socket()
            except Exception as error:
                log.exception(str(error))
                return ("fail", str(error))
        else:
            fw_socket = None
        log.info("Starting FITS interface capture")
        self._stop_capture()
        self._paf_handler = PafHandler(self._mode, fw_socket)
        self._capture_thread = CaptureData(self._capture_interface,
                                           self._capture_port, self._paf_handler)
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

    def __init__(self, ip, port, handler):
        Thread.__init__(self, name=self.__class__.__name__)
        self._address = (ip, port)
        self._buffer_size = 1<<15
        self._stop_event = Event()
        self._handler = handler

    def _reset_socket(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setblocking(False)
        self._socket.bind(self._address)

    def stop(self):
        """
        @brief     Stop the capture thread
        """
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
                log.debug("Received {} byte message from {}".format(
                    len(data), addr))
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


class PafPacket(object):

    def __init__(self, raw_data, nchannels):
        self._raw_data = raw_data
        self._nchannels = nchannels
        self._data = self._get_packet_struct().from_buffer_copy(self._raw_data)
        for i in inspect.getmembers(self._data):
            if not i[0].startswith('_'):
                if not inspect.ismethod(i[1]):
                    setattr(self, i[0], i[1])

    def _get_packet_struct(self):
        class PafPacketFormat(C.LittleEndianStructure):
            _fields_ = [
                ("beam_id", C.c_uint),
                ("time_stamp", C.c_char * 28),
                ("integ_time", C.c_float),
                ("nchannels", C.c_uint32),
                ("center_freq", C.c_float),
                ("channel_bw", C.c_float),
                ("pol_type", C.c_uint32),
                ("pol_id", C.c_uint32),
                ("nfreq_chunks", C.c_uint32),
                ("freq_chunks_index", C.c_uint32),
                ("data", C.c_int32 * self._nchannels)
            ]
        return PafPacketFormat

#    def __repr__(self):
# return ["{} = {}>".format(key,getattr(self._data,key)) for key, _ in
# self._data._fields_]


def build_fw_type(nsections, nchannels):
    class FWData(C.LittleEndianStructure):
        _fields_ = [
            ('section_id', C.c_uint32),
            ('nchannels', C.c_uint32),
            ('data', C.c_float * nchannels)
        ]

    class FWPacket(C.LittleEndianStructure):
        _fields_ = [
            ("data_type", C.c_char * 4),
            ("channel_data_type", C.c_char * 4),
            ("packet_size", C.c_uint32),
            ("backend_name", C.c_char * 8),
            ("timestamp", C.c_char * 28),
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
    packet.backend_name = "PAF     "
    packet.timestamp = timestamp
    packet.integration_time = integration_time
    packet.blank_phases = blank_phases
    packet.nsections = nsections
    packet.blocking_factor = 1
    for ii in range(nsections):
        packet.sections[ii].section_id = ii + 1
        packet.sections[ii].nchannels = nchannels
        C.memset(packet.sections[ii].data, 0, C.sizeof(packet.sections[ii].data))
    return packet


class PafHandler(object):
    """
    Aggregates spectrometer  or stokes data from different beams for the given time
    before sending to the fits writer
    """

    def __init__(self, mode, transmit_socket):

        self._mode = mode
        self._npol = 4
        self._nsections = 36 * self._npol
        self._nphases = 1
        self._raw_data = ""
        self._transmit_socket = transmit_socket
        self._active_packets = {}

    def read_channels_per_packet(self):
        metadata = struct.unpack("<i28sfiffiiii", self._raw_data[0:64])
        channelsPerPacket = metadata[3] / metadata[8]
        return channelsPerPacket

    def __call__(self, raw_data):
        """
        @brief      Handle a raw packet from the network

        @param      raw_data  The raw data captured from the network
        """
        self._raw_data = raw_data
        self.nchannelsperpacket = self.read_channels_per_packet()
        packet = PafPacket(raw_data, self.nchannelsperpacket)
        log.debug("Aggregate received packet: {}".format(packet))
        log.debug(("From packet: beam_id: {}, pol_id: {}, ts: {}, "
                   "integ_time = {}, channels: {}, freq_chunks: {}, "
                   "chunk_index: {}").format(
            packet.beam_id, packet.pol_id, packet.time_stamp,
            packet.integ_time, packet.nchannels, packet.nfreq_chunks,
            packet.freq_chunks_index))
        key = packet.time_stamp
        if key not in self._active_packets:
            fw_packet = build_fw_object(
                self._nsections, packet.nchannels, packet.time_stamp,
                int(packet.integ_time * 10**6), self._nphases)

            section_id = (packet.beam_id * 4) + packet.pol_id
            freq_idx_start = (self.nchannelsperpacket * packet.freq_chunks_index)
            freq_idx_end = (self.nchannelsperpacket * (packet.freq_chunks_index + 1))
            fw_packet.sections[section_id].data[freq_idx_start:freq_idx_end] = packet.data
            max_age = packet.integ_time * 3
            if max_age < 1:
                max_age = 1
            self._active_packets[key] = [
                time.time(), max_age, packet, fw_packet]
        else:
            section_id = (packet.beam_id * 4) + packet.pol_id
            freq_idx_start = (self.nchannelsperpacket * packet.freq_chunks_index)
            freq_idx_end = (self.nchannelsperpacket * (packet.freq_chunks_index + 1))
            self._active_packets[key][3].sections[section_id].data[freq_idx_start:freq_idx_end] = packet.data
        self.flush()

    def flush(self):
        """
        @brief      Iterate through all currently managed packets and flush complete or
                    stale packet groups to the FITS writer
        """
        log.debug(
            "Number of active packets pre-flush: {}".format(len(self._active_packets)))
        now = time.time()
        for key in sorted(self._active_packets.iterkeys()):
            timestamp, time_out, metadata, fw_packet = self._active_packets[
                key]
            if ((now - timestamp) >= time_out):
                try:
                    sensor_queue.put((timestamp, metadata, fw_packet), False)
                except Queue.Full:
                    sensor_queue.get()
                    sensor_queue.put((timestamp, metadata, fw_packet), False)
                if self._mode == 1:
                    log.debug(
                        "Sending packets with timestamp: {}".format(timestamp))
                    self._transmit_socket.send(bytearray(fw_packet))
                del self._active_packets[key]
        log.debug(
            "Number of active packets post-flush: {}".format(len(self._active_packets)))


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
    parser.add_option('-p', '--port', dest='port', type=int,
                      help='Port number to bind to', default=5000)
    parser.add_option('', '--cap-ip', dest='cap_ip', type=str,
                      help='Host interface to bind to for data capture', default='127.0.0.1')
    parser.add_option('', '--cap-port', dest='cap_port', type=int,
                      help='Port number to bind to for data capture', default=5001)
    parser.add_option('', '--fw-ip', dest='fw_ip', type=str,
                      help='FITS writer interface to bind to for data trasmission', default='127.0.0.1')
    parser.add_option('', '--fw-port', dest='fw_port', type=int,
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
    server = FitsInterfaceServer(
        opts.host, opts.port, opts.cap_ip, opts.cap_port, opts.fw_ip, opts.fw_port)
    # Hook up to SIGINT so that ctrl-C results in a clean shutdown
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))
    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
