from __future__ import print_function, division
import tornado
import logging
import signal
import socket
import time
import errno
import coloredlogs
import ctypes as C
from multiprocessing import Process
from datetime import datetime
from threading import Thread, Event, RLock
from optparse import OptionParser
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags
from katcp.kattypes import (Int, Str, request, return_reply)
import spead2
import spead2.recv


log = logging.getLogger("mpikat.spead_fi_server")

data_to_fw = {}
lock = RLock()

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

        @param    ip    The IP address to serve on
        @param    port  The port to serve on
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
                        "Unexpected error on socket accept: {}".format(
                            str(error)))
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
            self._transmit_socket.shutdown(socket.SHUT_RDWR)
            self._transmit_socket.close()
            self._transmit_socket = None
            self._has_connection.clear()

    def get_transmit_socket(self, timeout=2):
        """
        @brief   Get the active FITS writer connection

        @param   timeout   The time to wait for a connection to
                           become available
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
    VERSION_INFO = ("spead-edd-fi-server-api", 1, 0)
    BUILD_INFO = ("spead-edd-fi-server-implementation", 0, 1, "")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    PROTOCOL_INFO = ProtocolFlags(5, 0, set([
        ProtocolFlags.MULTI_CLIENT,
        ProtocolFlags.MESSAGE_IDS,
    ]))

    def __init__(self, katcp_interface, katcp_port, capture_interface,
                 capture_port, fw_ip, fw_port):
        """
        @brief Initialization of the FitsInterfaceServer object

        @param  katcp_interface    Interface address to serve on
        @param  katcp_port         Port number to serve on
        @param  capture_interface  Interface to capture data on
        @param  fw_ip              IP address of the FITS writer
        @param  fw_port            Port number to connect to FITS writer
        """
        self._configured = False
        self._capture_interface = capture_interface
        self._fw_connection_manager = FitsWriterConnectionManager(
            fw_ip, fw_port)
        self._capture_thread = None
        self._shutdown = False
        super(FitsInterfaceServer, self).__init__(katcp_interface, katcp_port)

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
    def heap_group(self):
        return self.heap_group_sensor.value()

    @heap_group.setter
    def heap_group(self, value):
        self.heap_group_sensor.set_value(value)

    @property
    def nmcg(self):
        return self.nmcg_sensor.value()

    @nmcg.setter
    def nmcg(self, value):
        self.nmcg_sensor.set_value(value)

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
        self.heap_group_sensor = Sensor.float(
            "heap_group",
            description="Number of heaps for a timestamp",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self.heap_group_sensor)
        self.nmcg_sensor = Sensor.float(
            "nMCG",
            description="Number of multicast groups",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self.nmcg_sensor)

    def _stop_capture(self):
        if self._capture_thread:
            log.debug("Cleaning up capture thread")
            self._capture_thread.stop()
            self._capture_thread.join()
            self._capture_thread = None 
            log.debug("Capture thread cleaned")

    @request(Str(), Int(), Int(), Int())
    @return_reply()
    def request_configure(self, req, mc_interface, mc_port, nmcg, heap_group):
        """
        @brief      Configure the FITS interface server

        @param      mc_interface   Array of multicast group IPs
        @param      mc_port        Port number to multicast stream
        @param      nmcg           Number of multicast groups
        @param      heap_group     Number of heaps with same timestamp

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        ip = mc_interface.split(",")
        message = (["mc_ip: {}".format(ip[i]) for i in range(nmcg)], 
            "mc_port: {}, nmcg: {}, heap_group: {}".format(mc_port, nmcg, heap_group))
        log.info("Configuring FITS interface server with params: {}".format(message))
        self.heap_group = heap_group
        self.nmcg = nmcg
        self.mc_interface = []
        for i in range(self.nmcg):
            self.mc_interface.append(ip[i])
        self.mc_port = mc_port
        self._fw_connection_manager.drop_connection()
        self._stop_capture()
        self._configured = True
        return ("ok",)

    @request()
    @return_reply()
    def request_start(self, req):
        """
        @brief      Start the FITS interface server capturing data

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
        self._capture_thread = CaptureData(self.mc_interface,
                                           self.mc_port,
                                           self._capture_interface,
                                           self.nmcg,
                                           self.heap_group, 
                                           fw_socket)
        self._capture_thread.start()
        return ("ok",)

    @request()
    @return_reply()
    def request_stop(self, req):
        """
        @brief      Stop the FITS interface server capturing data

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
    @brief     Captures heaps from one or more streams that are transmitted in SPEAD format 
    """

    def __init__(self, mc_ip, mc_port, capture_ip, nmcg, heap_group, transmit_soc):
        """
        @brief      Initialization of the CaptureData thread

        @param  mc_ip              Array of multicast group IPs
        @param  mc_port            Port number of multicast streams
        @param  capture_interface  Interface to capture streams from MCG  on
        @param  nmcg               number of multicast groups to subscribe
        @param  heap_group         numbers of heaps for a given timestamp
        @param  transmit_soc       FITS writer interface
        """
        Thread.__init__(self, name=self.__class__.__name__)
        self._mc_ip = mc_ip
        self._mc_port = mc_port
        self._capture_ip = capture_ip
        self._nmcg = nmcg
        self._heap_group = heap_group
        self._stop_event = Event()
        self.stream = []
        self.stream_handlers= []
        self._transmit_soc = transmit_soc
        #self._buffer_size = buffer_size

    def stop(self):
        """
        @brief      Stop the capture thread
        """
        for i in range(self._nmcg):
            self.stream_handlers[i].stop()
        self._stop_event.set()

    def mcg_subscription(self, nmcg):
        """
        @brief      Subscribes to one or more multicast streams
        """
        for i in range(self._nmcg):
            thread_pool = spead2.ThreadPool()
            self.stream.append(spead2.recv.Stream(thread_pool, spead2.BUG_COMPAT_PYSPEAD_0_5_2))
            del thread_pool
            pool = spead2.MemoryPool(16384, 26214400, 12, 8)
            self.stream[i].set_memory_allocator(pool)
            self.stream[i].add_udp_reader(self._mc_ip[i], self._mc_port, max_size = 9200L,
                buffer_size= 212992L, interface_address=self._capture_ip)
        print("Stream Length: ", len(self.stream))
        return self.stream

    def run(self):
        self.mcg_subscription(self._nmcg)
        for i in range(self._nmcg):
            self.stream_handlers.append(StreamHandler(self.stream[i], 
                self._heap_group, self._transmit_soc))
            self.stream_handlers[i].start()


class HeapPacket(object):
    def __init__(self):
        self._first_heap = True 
    
    def heap_items(self):
        """
        @brief      Description of heap items 
        """
        self.ig = spead2.ItemGroup()
        self.ig.add_item(5632, "timestamp", "", (1,), format=[["u", 48]], order='C')
        self.ig.add_item(5633, "polID", "", (1,), dtype=">I")
        self.ig.add_item(5634, "ndStatus", "", (1,), dtype=">I")
        self.ig.add_item(5635, "nchannels", "", (1,), dtype=">I")
        self.ig.add_item(5636, "nsamples", "", (1,), dtype=">I")
        self.ig.add_item(5637, "integtime", "", (1,), dtype=">I")
        return self.ig

    def unpack_heap(self, heap):
        items = self.ig.update(heap)
        for item in items.values():
            if (item.id == 5635) and (self._first_heap):
                self.ig.add_item(5638, "data", "", (int(item.value),), dtype=">f")
                self._first_heap = False
            log.info("Iname: {}, Ivalue: {}".format(item.name, item.value))
            setattr(self, item.name, item.value)

    def __repr__(self):
        return "<HeapPacket, ts={}, polId={}, ndStatus={}, nchannels={}, nsamples={},\
                 integTime={}, data={}>".format(self.timestamp, self.polID, self.ndStatus,
                 self.nchannels, self.nsamples, self.integtime, self.data)


def isotime():
    return "{}UTC".format(datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S.%f')[:-2])


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


def build_fw_object(nsections, nchannels, timestamp, integration_time,
                    blank_phases):
    packet_format = build_fw_type(nsections, nchannels)
    packet = packet_format()
    packet.data_type = "EEEI"
    packet.channel_data_type = "F   "
    packet.packet_size = C.sizeof(packet_format)
    packet.backend_name = "EDDSPEAD"
    packet.timestamp = timestamp
    packet.integration_time = integration_time
    packet.blank_phases = blank_phases
    packet.nsections = nsections
    packet.blocking_factor = 1
    for ii in range(nsections):
        packet.sections[ii].section_id = ii + 1
        packet.sections[ii].nchannels = nchannels
        C.addressof(packet.sections[ii].data), 0, C.sizeof(
            packet.sections[ii].data)
    return packet


class StreamHandler(Thread):
    """
    Aggregates data from one or more streams for the given time
    before sending to the fits writer
    """
    def __init__(self, stream, heap_group, transmit_socket, max_age=2.0):
        """
        @brief      Initialization of the StreamHandler thread

        @param  stream             Streams received from mcg
        @param  heap_group         Number of heaps for a given timestamp
        @param  transmit_soc       FITS writer interface to which the data is sent
        @param  max_age            timeout
        """
        Thread.__init__(self)
        self._stream = stream
        self._nsections = heap_group 
        self._nphases = 1 
        self._transmit_socket = transmit_socket
        self._max_age = max_age
        self._first_heap = True

    def stop(self):
        self._stream.stop()

    def run(self):
        """
        @brief      Handle a raw packet from the network

        @param      raw_data  The raw data captured from the network
        """
        log.info("Reading stream..")
        self.packet = HeapPacket()
        self.packet.heap_items()
        for heap in self._stream:
            self.packet.unpack_heap(heap)
            if not self._first_heap:
                self.aggregate_data(self.packet)
            self._first_heap = False

    def aggregate_data(self, packet):
        key = tuple(packet.timestamp)
        if key not in data_to_fw:
            fw_pkt = build_fw_object(self._nsections, int(packet.nchannels), isotime(),
                                     packet.integtime, self._nphases)
            fw_pkt.sections[int(packet.ndStatus)].data[:]=packet.data
            lock.acquire()
            try:
                data_to_fw[key] = [time.time(), 1, fw_pkt]
            finally:
                lock.release()
        else:
            lock.acquire()
            try:
                data_to_fw[key][1] += 1
                data_to_fw[key][2].sections[int(packet.ndStatus)].data[:] = packet.data
            finally:
                lock.release()
        self.flush()

    def flush(self):
        """
        @brief      Iterate through all currently managed packets and
                    flush complete or stale packet groups to the FITS writer
        """
        log.debug(
            "Number of active packets pre-flush: {}".format(
                len(data_to_fw)))
        now = time.time()
        for key in sorted(data_to_fw.iterkeys()):
            timestamp, hits, fw_packet = data_to_fw[key]
            if ((now - timestamp) > self._max_age):
                log.warning(("Age exceeds maximum age. Incomplete packet"
                             " will be flushed to FITS writer."))
                self._transmit_socket.send(bytearray(fw_packet))
                del data_to_fw[key]
            elif (hits == self._nsections):
                log.debug(
                    "Sending complete packet with timestamp: {}".format(
                        timestamp))
                self._transmit_socket.send(bytearray(fw_packet))
                del data_to_fw[key]
        log.debug(
            "Number of active packets post-flush: {}".format(
                len(data_to_fw)))


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
                      help='Host interface to bind to for data capture',
                      default='127.0.0.1')
    parser.add_option('', '--cap-port', dest='cap_port', type=int,
                      help='Port number to bind to for data capture',
                      default=5001)
    parser.add_option('', '--fw-ip', dest='fw_ip', type=str,
                      help='IP to serve on for FW connections',
                      default='127.0.0.1')
    parser.add_option('', '--fw-port', dest='fw_port', type=int,
                      help='Port to serve on for FW connections',
                      default=5002)
    parser.add_option('', '--log-level', dest='log_level', type=str,
                      help='Defauly logging level', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
        level=opts.log_level.upper(),
        logger=log)
    log.setLevel(opts.log_level.upper())
    ioloop = tornado.ioloop.IOLoop.current()
    server = FitsInterfaceServer(
        opts.host, opts.port, opts.cap_ip,
        opts.cap_port, opts.fw_ip, opts.fw_port)
    # Hook up to SIGINT so that ctrl-C results in a clean shutdown
    signal.signal(signal.SIGINT,
                  lambda sig, frame: ioloop.add_callback_from_signal(
                    on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(
            server.bind_address))
    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
