from __future__ import print_function, division
from tornado.gen import coroutine
import logging
import socket
import time
import errno
import numpy as np
import ctypes
import json
from datetime import datetime
from threading import Thread, Event
import queue

import spead2
import spead2.recv

from katcp import Sensor, ProtocolFlags
from katcp.kattypes import (request, return_reply)

from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer
import mpikat.utils.numa as numa

log = logging.getLogger("mpikat.spead_fi_server")


class FitsWriterConnectionManager(Thread):
    """
    A class to handle TCP connections to the APEX FITS writer.

    This class implements a TCP/IP server that can accept connections from
    the FITS writer. Upon acceptance, the new communication socket is stored
    and packages from a local queue are streamed to the fits writer.
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
#        self._transmit_flag = Event()  # Potetntially redundant with has connection ?

        self.__output_queue = queue.PriorityQueue()
        self.__delay = 5         # Delay a packet for self.__delay seconds between its reference time before sending
        self.__latestPackageTime = 0
        self.__send_items = 0

        self._transmit_socket = None
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
                    continue
                else:
                    log.exception(
                        "Unexpected error on socket accept: {}".format( str(error)))
                    raise error


    def drop_connection(self):
        """
        @brief   Drop any current FITS writer connection and waits for new one.
        """
        if not self._transmit_socket is None:
            log.debug("Dropping connection")
            self._transmit_socket.shutdown(socket.SHUT_RDWR)
            self._transmit_socket.close()
            self._transmit_socket = None
            self._has_connection.clear()

    def put(self, ref_time, pkg):
        """
        Put new output pkg on local queue if there is a fits server connection. Drop packages otherwise.
        """
        if self._has_connection.is_set():
            self.__output_queue.put([ref_time, pkg])
        else:
            log.info("No connection, dropping package.")


    def send_data(self, delay):
        """
        Send all packages in queue older than delay seconds.
        """
        now = time.time()

        while not self.__output_queue.empty():
            timestamp, pkg = que.get()
            if (timestamp - now) < delay:
                # Pkg. too young, there might be others. Put back and return
                self.__output_queue.put([timestamp, pkg])
                self.__output_queue.task_done()
                return
            log.info('Send item {} with reference time {}. Fits timestamp: {}'.format(self.__send_items, timestamp, pkg.timestamp))
            pkg = self.__output_queue.pop(timestamp)
            self.__latestPackageTime = timestamp
            self._transmit_socket.send(bytearray(pkg))
            self.__send_items += 1
            self.__output_queue.task_done()

    def flush_queue(self):
        """
        Send all remaining packages.
        """
        self.send_data(0)
        #self.__output_queue.join()

    def stop(self):
        """
        @brief   Stop the server
        """
        self._shutdown.set()

    def run(self):
        while not self._shutdown.is_set():
            if not self._has_connection.is_set():
                self.drop_connection()  # Jsut in case
                try:
                    self._transmit_socket = self._accept_connection()
                except Exception as error:
                    log.exception(str(error))
                    continue

            self.send_data(self.__delay)
            time.sleep(0.1)

        self.flush_queue()
        self.drop_connection()
        self._server_socket.close()



class FitsInterfaceServer(EDDPipeline):
    """
    Class providing an interface between EDD processes and the
    Effelsberg FITS writer
    """
    VERSION_INFO = ("spead-edd-fi-server-api", 1, 0)
    BUILD_INFO = ("spead-edd-fi-server-implementation", 0, 1, "")

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

    @coroutine
    def capture_stop(self):
        if self._capture_thread:
            log.debug("Cleaning up capture thread")
            self._capture_thread.stop()
            self._capture_thread.join()
            self._capture_thread = None
            log.debug("Capture thread cleaned")
        if self._fw_connection_manager:
            self._fw_connection_manager.stop()
            self._fw_connection_manager.join()
            self._fw_connection_manager = None
            log.debug("Conenction manager thread cleaned")


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
            self._fw_connection_manager.join()

        self._fw_connection_manager = FitsWriterConnectionManager(self._config["fits_writer_ip"], self._config["fits_writer_port"])
        self._fw_connection_manager.start()

        #self._stop_capture()
        self._configured = True


    @coroutine
    def capture_start(self):
        """
        """
        #try:
        #    fw_socket = self._fw_connection_manager.get_transmit_socket()
        #except Exception as error:
        #    raise RuntimeError("Exception in getting fits writer transmit socker: {}".format(error))
        log.info("Starting FITS interface capture")
        #self._stop_capture()

        handler = GatedSpectrometerSpeadHandler(self._fw_connection_manager)
        self._capture_thread = SpeadCapture(self.mc_interface,
                                           self.mc_port,
                                           self._capture_interface,
                                           handler)
        self._capture_thread.start()

    @coroutine
    def measurement_start(self):
        log.info("Starting FITS interface data transfer as soon as connection is established ...")
        pass


    @coroutine
    def measurement_stop(self):
        log.info("Stopping FITS interface data transfer")
        #self._stop_capture()
        self._fw_connection_manager.flush_queue()
        self._fw_connection_manager.drop_connection()


    @coroutine
    def stop(self):
        """
        Stop pipeline server.
        """
        yield self.capture_stop()



class SpeadCapture(Thread):
    """
    @brief     Captures heaps from one or more streams that are transmitted in
               SPEAD format and call handler on completed heaps.
    """
    def __init__(self, mc_ip, mc_port, capture_ip, speadhandler):
        """
        @brief Constructor

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
        self._handler = speadhandler 

        #ToDo: fix magic numbers for parameters in spead stream
        thread_pool = spead2.ThreadPool(threads=4)
        self.stream = spead2.recv.Stream(thread_pool, spead2.BUG_COMPAT_PYSPEAD_0_5_2, max_heaps=64, ring_heaps=64, contiguous_only = False)
        pool = spead2.MemoryPool(16384, ((32*4*1024**2)+1024), max_free=64, initial=64)
        self.stream.set_memory_allocator(pool)


    def stop(self):
        """
        @brief      Stop the capture thread
        """
        self._handler.stop = True
        self.stream.stop()
        self._stop_event.set()


    def run(self):
        """
        Subscribe to MC groups and start processing.
        """
        log.debug("Subscribe to multicast groups:")
        for i, mg in enumerate(self._mc_ip):
            log.debug(" - Subs {}: ip: {}, port: {}".format(i,self._mc_ip[i], self._mc_port ))
            self.stream.add_udp_reader(mg, int(self._mc_port), max_size = 9200L,
                buffer_size= 1073741820L, interface_address=self._capture_ip)

        log.debug("Start processing heaps:")
        self._nheaps = 0
        self._incomplete_heaps = 0
        self._complete_heaps = 0

        for heap in self.stream:
            self._nheaps += 1
            log.debug('Received heap {} - previously {} completed, {} incompleted'.format(self._nheaps, self._complete_heaps, self._incomplete_heaps))
            if isinstance(heap, spead2.recv.IncompleteHeap):
                log.warning('Received incomplete heap - received only {} / {} bytes.'.format(heap.received_length, heap.heap_length))
                self._incomplete_heaps += 1
                continue
            else:
                self._complete_heaps += 1
            self._handler(heap)







def fw_factory(nsections, nchannels, data_type = "EEEI", channel_data_type = "F   "):
    """, fits_interface
    Crate a APEX compatible fits writer container with
      * nsections
      * nchannels
      * data_type
      * channel_data_type
    """
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

    packet = FWPacket()

    if channel_data_type != "F   ":
         raise NotADirectoryError("cannot handle backend data format {}".format(channel_data_type))
    packet.channel_data_type = channel_data_type

    if data_type != "EEEI":
         raise NotADirectoryError("cannot handle numerical encoding standard {}".format(data_type))
    packet.data_type = data_type

    packet.nsections = nsections

    # Hard coded for effelsberg ??
    packet.blocking_factor = 1
    for ii in range(nsections):
        packet.sections[ii].section_id = ii + 1
        packet.sections[ii].nchannels = nchannels
        ctypes.addressof(packet.sections[ii].data), 0, ctypes.sizeof(packet.sections[ii].data)

    return packet



def convert48_64(A):
    """
    Converts 48 bit to 64 bit integers
    """
    assert (len(A) == 6)
    npd = np.array(A, dtype=np.uint64)
    return np.sum(256**np.arange(0,6, dtype=np.uint64)[::-1] * npd)


class GatedSpectrometerSpeadHandler(object):
    """
    Parse heaps of gated spectrometer output and aggregate items with matching
    polarizations.
    Heaps above a max age will be dropped.
    Complete packages are passed to the fits interface queue
    """
    def __init__(self, fits_interface, max_age = 10):

        self.__max_age = max_age
        self.__fits_interface = fits_interface

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

        # Queue for aggregated objects
        self.__packages_in_preparation = {}

        # Now is the latest checked package
        self.__now = 0


    def __call__(self, heap):
        """
        handle heaps. Merge polarization heaps with matching timestamps and pass to output queue.
        """
        log.debug("Unpacking heap")
        items = self.ig.update(heap)
        if 'data' not in items:
            # On first heap only, get number of channels
            fft_length = convert48_64(items['fft_length'].value)
            self.nchannels = int((fft_length/2)+1)
            log.debug("First item - setting data length to {} channels".format(self.nchannels))
            self.ig.add_item(5640, "data", "", (self.nchannels,), dtype="<f")
            # Reprocess heap to get data
            items = self.ig.update(heap)

        class SpeadPacket:
            """
            Contains the items as members with conversion
            """
            def __init__(self, items):
                for item in items.values():
                    if item.name != 'data':
                        setattr(self, item.name, convert48_64(item.value))
                    else:
                        setattr(self, item.name, item.value)

        packet = SpeadPacket(items)

        log.debug("Aggregating data for timestamp {}, Noise diode: {}, Polarization: {}".format(packet.timestamp_count, packet.noise_diode_status, packet.polarization))

        # Integration period does not contain efficiency of sampling as heaps may
        # be lost respectively not in this gate
        packet.integration_period = (packet.naccumulate * packet.fft_length) / float(packet.sampling_rate)

        # The reference time is in the center of the integration # period
        packet.reference_time = packet.sync_time + packet.timestamp_count / float(packet.sampling_rate) + packet.integration_period/ 2
        if packet.reference_time > self.__now:
            self.__now = packet.reference_time

        # packets with noise diode on are required to arrive at different time
        # than off
        if(packet.noise_diode_status == 1):
            packet.reference_time += 0.0050

        if packet.reference_time not in self.__packages_in_preparation:
            fw_pkt = fw_factory(2, int(self.nchannels) - 1)
        else:
            fw_pkt = self.__packages_in_preparation[packet.reference_time]

        # Copy data and drop DC channel - Direct numpy copy behaves weired as memory alignment is expected
        # but ctypes may not be aligned
        sec_id = packet.polarization
        #ToDo: calculation of offsets without magic numbers

        log.debug("   Data (first 5 ch., including DC): {}".format(packet.data[:5]))
        packet.data /= (packet.number_of_input_samples + 1E-30)
        log.debug("                After normalization: {}".format(packet.data[:5]))
        ctypes.memmove(ctypes.byref(fw_pkt.sections[int(sec_id)].data), packet.data.ctypes.data + 4,
                packet.data.size * 4 - 4)

        if packet.reference_time in self.__packages_in_preparation:
            # Fill header fields + output data
            log.debug("Received two polarizations for refernce_time: {}".format(packet.reference_time))

            # Convert timestamp to datetimestring in UTC
            def local_to_utc(t):
                secs = time.mktime(t)
                return time.gmtime(secs)
            dto = datetime.fromtimestamp(packet.reference_time)
            timestamp = time.strftime('%Y-%m-%dT%H:%M:%S', local_to_utc(dto.timetuple() ))

            # Blank should be at the end according to specification
            timestamp += ".{:04d}UTC ".format(int((float(packet.reference_time) - int(packet.reference_time)) * 10000))
            fw_pkt.timestamp = timestamp
            log.debug("   Calculated timestamp for fits: {}".format(fw_pkt.timestamp))

            integration_time = packet.number_of_input_samples / float(sampling_rate)
            log.debug("   Integration period: {} s".format(integration_period))
            log.debug("   Received samples in period: {}".format(packet.number_of_input_samples))
            log.debug("   Integration time: {} s".format(integration_time))

            fw_pkt.backend_name = "EDDSPEAD"
            # As the data is normalized to the actual nyumber of samples, the
            # integration time for the fits writer corresponds to the nominal
            # time.
            fw_pkt.integration_time = integration_period * 1000 # in ms

            fw_pkt.blank_phases = 2 - noise_diode_status
            log.debug("   Noise diode status: {}, Blank phase: {}".format(packet.noise_diode_status, fw_pkt.blank_phases))

            self.__fits_interface.put(packet.reference_time, self.__packages_in_preparation.pop(packet.reference_time))

        else:
            self.__packages_in_preparation[packet.reference_time] = fw_pkt
            # Cleanup old packages
            tooold_packages = []
            log.debug('Checking {} packages for age restriction'.format(len(self.__packages_in_preparation)))
            for p in self.__packages_in_preparation:
                age = self.__now - p
                #log.debug(" Package with timestamp {}: now: {} age: {}".format(p, self.__now, age) )
                if age > self.__max_age:
                    log.warning("Age of package {} exceeded maximum age {} - Incomplete package will be dropped.".format(age, self.__max_age))
                    tooold_packages.append(p)
            for p in tooold_packages:
                self.__packages_in_preparation.pop(p)


if __name__ == "__main__":
    launchPipelineServer(FitsInterfaceServer)
