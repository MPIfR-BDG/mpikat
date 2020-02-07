from __future__ import print_function, division
import tornado
from tornado.gen import coroutine
import logging
import signal
import socket
import time
import errno
import coloredlogs
import base64
import numpy as np
import ctypes
import json
from StringIO import StringIO
from Queue import Queue, Empty, Full
from datetime import datetime
from threading import Thread, Event

import spead2
import spead2.recv

from katcp import Sensor, ProtocolFlags
from katcp.kattypes import (Int, Str, request, return_reply)

from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig
import mpikat.utils.numa as numa

log = logging.getLogger("mpikat.spead_fi_server")



# Convert side channel item to
def convert48_64(A):
    assert (len(A) == 6)
    npd = np.array(A, dtype=np.uint64)
    return np.sum(256**np.arange(0,6, dtype=np.uint64)[::-1] * npd)


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
                        "Unexpected error on socket accept: {}".format(
                            str(error)))
                    raise error
            except Exception as error:
                log.exception(
                    "Unexpected error on socket accept: {}".format(str(error)))
                raise error


    def drop_connection(self):
        """
        @brief   Drop any current FITS writer connection and waits for new one.
        """
        log.debug("Drop connection")
        if not self._transmit_socket is None:
            self._transmit_socket.shutdown(socket.SHUT_RDWR)
            self._transmit_socket.close()
            self._transmit_socket = None
            self._has_connection.clear()
            #self._accept_connection()


    def get_transmit_socket(self, timeout=20):
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
        raise RuntimeError("Fits writer not connected!")


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



class FitsInterfaceServer(EDDPipeline):
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

    def __init__(self, ip, port):
        """
        @brief Initialization of the FitsInterfaceServer object

        @param  katcp_interface    Interface address to serve on
        @param  katcp_port         Port number to serve on
        @param  fw_ip              IP address of the FITS writer
        @param  fw_port            Port number to connect to FITS writer
        """
        EDDPipeline.__init__(self, ip, port, dict(input_data_streams=[],
            id="fits_interface", type="fits_interface",
            fits_writer_ip="0.0.0.0", fits_writer_port=5002))
        self._configured = False
        self._capture_interface = None
        self._fw_connection_manager = None
        self._capture_thread = None
        self._shutdown = False


    def _stop_capture(self):
        if self._capture_thread:
            log.debug("Cleaning up capture thread")
            self._capture_thread.stop()
            self._capture_thread.join()
            self._capture_thread = None
            log.debug("Capture thread cleaned")


    @coroutine
    def configure(self, config_json):
        log.info("Configuring Fits interface")
        log.debug("Configuration string: '{}'".format(config_json))

        self.state = "configuring"
        yield self.set(config_json)

        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)

        # find fastest nic on host
        nic_name, nic_description = numa.getFastestNic()
        self._capture_interface = nic_description['ip']
        log.info("Capturing on interface {}, ip: {}, speed: {} Mbit/s".format(nic_name, nic_description['ip'], nic_description['speed']))

        #ToDo: allow streams with multiple multicast groups and multiple ports

        self.mc_interface = []
        self.mc_port = None
        for stream_description in self._config['input_data_streams']:
            self.mc_interface.append(stream_description['ip'])
            if self.mc_port is None:
                self.mc_port = stream_description['port']
            else:
                if self.mc_port != stream_description['port']:
                    raise RuntimeError("All input streams have to use the same port!!!")

        if self._fw_connection_manager is not None:
            log.warning("Replacing fw_connection manager")
            self._fw_connection_manager.stop()
            self._fw_connection_manager.drop_connection()
            self._fw_connection_manager.join()
        self._fw_connection_manager = FitsWriterConnectionManager( self._config["fits_writer_ip"], self._config["fits_writer_port"])
        self._fw_connection_manager.start()

        #self._stop_capture()
        self._configured = True


    @coroutine
    def measurement_start(self):
        """
        """
        try:
            fw_socket = self._fw_connection_manager.get_transmit_socket()
        except Exception as error:
            raise RuntimeError("Exception in getting fits writer transmit socker: {}".format(error))
        log.info("Starting FITS interface capture")
        self._stop_capture()
        handler = StreamHandler(fw_socket)
        self._capture_thread = CaptureData(self.mc_interface,
                                           self.mc_port,
                                           self._capture_interface,
                                           handler)
        self._capture_thread.start()


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
            self.measurement_start()
        except Exception as error:
            return ("fail", str(error))
        return ("ok",)


    @coroutine
    def measurement_stop(self):
        log.info("Stopping FITS interface capture")
        self._stop_capture()
        self._fw_connection_manager.drop_connection()


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
        self.measurement_stop()
        return ("ok",)



class CaptureData(Thread):
    """
    @brief     Captures heaps from one or more streams that are transmitted in SPEAD format
    """

    def __init__(self, mc_ip, mc_port, capture_ip, handler):
        """
        @brief      Initialization of the CaptureData thread

        @param  mc_ip              Array of multicast group IPs
        @param  mc_port            Port number of multicast streams
        @param  capture_ip         Interface to capture streams from MCG  on
        @param  handler            Object that handles data in the stream
        """
        Thread.__init__(self, name=self.__class__.__name__)
        self._mc_ip = mc_ip
        self._mc_port = mc_port
        self._capture_ip = capture_ip
        self._stop_event = Event()
        self._handler = handler


    def stop(self):
        """
        @brief      Stop the capture thread
        """
        self._handler.stop = True
        self.stream.stop()
        self._stop_event.set()


    def resource_allocation(self):
        #ToDo: fix magic numbers for parameters in spead stream
        thread_pool = spead2.ThreadPool(threads=4)
        self.stream = spead2.recv.Stream(thread_pool, spead2.BUG_COMPAT_PYSPEAD_0_5_2, max_heaps=64, ring_heaps=64, contiguous_only = False)
        pool = spead2.MemoryPool(16384, ((32*4*1024**2)+1024), max_free=64, initial=64)
        self.stream.set_memory_allocator(pool)
        self.rb = self.stream.ringbuffer


    def mcg_subscription(self):
        log.debug(" Multicast subscribe ...")
        for i, mg in enumerate(self._mc_ip):
            log.debug(" - Subs {}: ip: {}, port: {}".format(i,self._mc_ip[i], self._mc_port ))
            self.stream.add_udp_reader(mg, int(self._mc_port), max_size = 9200L,
                buffer_size= 1073741820L, interface_address=self._capture_ip)

    def run(self):
        self.resource_allocation()
        self.mcg_subscription()
        self._handler(self.stream)



class HeapPacket(object):
    def __init__(self):
        #Description of heap items
        self.ig = spead2.ItemGroup()
        self.ig.add_item(5632, "timestamp_count", "", (6,), dtype=">u1")
        self.ig.add_item(5633, "polarization", "", (6,), dtype=">u1")
        self.ig.add_item(5634, "noise_diode_status", "", (6,), dtype=">u1")
        self.ig.add_item(5635, "fft_length", "", (6,), dtype=">u1")
        self.ig.add_item(5636, "number_of_input_samples", "", (6,), dtype=">u1")
        self.ig.add_item(5637, "sync_time", "", (6,), dtype=">u1")
        self.ig.add_item(5638, "sampling_rate", "", (6,), dtype=">u1")
        self.ig.add_item(5639, "naccumulate", "", (6,), dtype=">u1")

    def unpack_heap(self, heap):
        log.debug("Unpacking heap")
        items = self.ig.update(heap)
        if 'data' not in items:
            # Should be on first heap only
            fft_length = convert48_64(items['fft_length'].value)
            self.nchannels = int((fft_length/2)+1)
            self.ig.add_item(5640, "data", "", (self.nchannels,), dtype="<f")
            items = self.ig.update(heap)

        for item in items.values():
            if item.name != 'data':
                setattr(self, item.name, convert48_64(item.value))
            else:
                setattr(self, item.name, item.value)
            log.debug(" Storing iname: {}, Ivalue: {} w. type {}".format(item.name, getattr(self, item.name), type(getattr(self, item.name))))

        self.integration_time = (self.naccumulate * self.fft_length) / float(self.sampling_rate)
        self.data /= (self.number_of_input_samples + 1E-30)

        t = self.sync_time + self.timestamp_count / float(self.sampling_rate) + self.integration_time / 2
        if(self.noise_diode_status == 1):
            t += 0.0050

        def local_to_utc(t):
            secs = time.mktime(t)
            return time.gmtime(secs)

        dto = datetime.fromtimestamp(float(t))
        # Blank should be at the end according to specification
        self.timestamp = time.strftime('%Y-%m-%dT%H:%M:%S', local_to_utc(dto.timetuple() ))
        self.timestamp += ".{:04d}UTC ".format(int((float(t) - int(t)) * 10000))
        log.debug("Setting packet timestamp: {}".format(self.timestamp))





def build_fw_type(nsections, nchannels):
    class FWData(ctypes.LittleEndianStructure):
        _pack_ = 1
        _fields_ = [
            ('section_id', ctypes.c_uint32),
            ('nchannels', ctypes.c_uint32),
            ('data', ctypes.c_float * nchannels)
        ]

    class FWPacket(ctypes.LittleEndianStructure):
        _pack_ = 1
        _fields_ = [
            ("data_type", ctypes.c_char * 4),               #     0 -  3
            ("channel_data_type", ctypes.c_char * 4),       #     4 -  7
            ("packet_size", ctypes.c_uint32),               #     8 - 11
            ("backend_name", ctypes.c_char * 8),            #    12 - 19
            ("timestamp", ctypes.c_char * 28),              #    20 - 48
            ("integration_time", ctypes.c_uint32),          #    49 - 53
            ("blank_phases", ctypes.c_uint32),              #    54 - 57 alternate between 1 and 2 cal on or off
            ("nsections", ctypes.c_uint32),                 #    57 - 61 number of sections
            ("blocking_factor", ctypes.c_uint32),           #    61-63 spectra per section ?
            ("sections", FWData * nsections)                #    actual section data
        ]
    return FWPacket


def build_fw_object(nsections, nchannels, timestamp, integration_time,
                    blank_phases):
    packet_format = build_fw_type(nsections, nchannels)
    packet = packet_format()
    packet.data_type = "EEEI"
    packet.channel_data_type = "F   "
    packet.packet_size = ctypes.sizeof(packet_format)
    packet.backend_name = "EDDSPEAD"
    packet.timestamp = timestamp
    packet.integration_time = integration_time
    packet.blank_phases = blank_phases
    packet.nsections = nsections
    packet.blocking_factor = 1

    for ii in range(nsections):
        packet.sections[ii].section_id = ii + 1
        packet.sections[ii].nchannels = nchannels
        ctypes.addressof(packet.sections[ii].data), 0, ctypes.sizeof(
            packet.sections[ii].data)
    return packet


class StreamHandler(object):
    """
    Aggregates heaps that belong to a heap_group from one or more streams
    and sends to the fits writer
    """
    def __init__(self, transmit_socket, max_age=15.0):
        """
        @brief      Initialization of the StreamHandler thread

        @param  transmit_soc       FITS writer interface to which the data is sent
        @param  max_age            timeout
        """
        self._data_to_fw = {}
        self._transmit_socket = transmit_socket
        self._max_age = max_age
        self._nheaps = 0
        self._complete_heaps = 0
        self._incomplete_heaps = 0
        self._last_package_send_time = 0

        self._nsections = 2
        # ToDo: make configurable option
        self._dump_tcp_packages = True

    def __call__(self, stream):
        """
        @brief      Handle a raw packet from the network

        @param      stream  heaps from multiple multicast groups
        """
        self.rb = stream.ringbuffer
        log.info("Reading stream..")
        self.packet = HeapPacket()
        for heap in stream:
            self._nheaps += 1
            log.debug('Received heap {} - previously {} completed, {} incompleted'.format(self._nheaps, self._complete_heaps, self._incomplete_heaps))
            if isinstance(heap, spead2.recv.IncompleteHeap):
                log.warning('Received incomplete heap - received only {} / {} bytes.'.format(heap.received_length, heap.heap_length))
                self._incomplete_heaps += 1
                continue
            else:
                self._complete_heaps += 1
            self.packet.unpack_heap(heap)
            self.aggregate_data(self.packet)


    def aggregate_data(self, packet):
        sec_id = packet.polarization
        nphases = packet.noise_diode_status
        # Constantas two polarizations
        key = packet.timestamp
        if key not in self._data_to_fw:
            # DROP DC CHANNEL
            # ToDo: nphase naming and blank_phase
            fw_pkt = build_fw_object(self._nsections, int(packet.nchannels) - 1, packet.timestamp,
                                     int(packet.integration_time * 1000), 2 - int(nphases))
            # Direct numpy copy behaves weired as memory alignment is expected
            # but ctypes may not align?
            #fw_pkt.sections[int(sec_id)].data[:]=packet.data[1:]

            ctypes.memmove(ctypes.byref(fw_pkt.sections[int(sec_id)].data), packet.data.ctypes.data + 4,
                    packet.data.size * 4 - 4)
            self._data_to_fw[key] = [time.time(), 1, fw_pkt]
        else:
            self._data_to_fw[key][1] += 1
            ctypes.memmove(ctypes.byref(self._data_to_fw[key][2].sections[int(sec_id)].data), packet.data.ctypes.data + 4, packet.data.size * 4 - 4)
        self.flush()


    def flush(self):
        """
        @brief      Iterate through all currently managed packets and
                    flush complete or stale packet groups to the FITS writer
        """
        log.debug(
            "Number of active packets pre-flush: {}".format(
                len(self._data_to_fw)))
        now = time.time()

        for key in sorted(self._data_to_fw.iterkeys()):
            timestamp, hits, fw_packet = self._data_to_fw[key]
            if ((now - timestamp) > self._max_age):
                log.warning(("Age exceeds maximum age. Incomplete packet"
                             " will be dropped."))
                del self._data_to_fw[key]
                #ToDo: Implement proper queue that waits for a reasonable time,
                #e.g. depending on the integration time, or the number of
                #packages in it
            elif timestamp < self._last_package_send_time:
                log.warning(("Packet older than latest send, will be dropped."))
                del self._data_to_fw[key]
            elif (hits == self._nsections):
                log.debug(
                    "Sending complete packet with timestamp: {}".format(
                        timestamp))
                log.debug("Heaps in Ringbuffer: {}".format(self.rb.size()))
                log.debug("Heap statistics: total_heaps: {}, complete_heaps: {}, incomplete_heaps: {}".format(
                          self._nheaps, self._complete_heaps, self._incomplete_heaps))
                self._transmit_socket.send(bytearray(fw_packet))

                #fn = "/tmp/fw_packet_{}.dat".format(timestamp)
                #if self._dump_tcp_packages:
                #    with open(fn, 'wb') as ofile:
                #        log.debug("Wrote package to disk: {}".format(fn))
                #        ofile.write(bytearray(fw_packet))

                self._last_package_send_time = timestamp
                log.debug("Len of bytearray: {}".format(len(bytearray(fw_packet))))
                del self._data_to_fw[key]
        log.debug( "Number of active packets post-flush: {}".format( len(self._data_to_fw)))
        log.debug( " hits: {}".format(hits))



if __name__ == "__main__":
    launchPipelineServer(FitsInterfaceServer)
