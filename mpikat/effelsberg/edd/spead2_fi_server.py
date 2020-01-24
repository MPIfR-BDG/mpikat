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
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from StringIO import StringIO
from Queue import Queue, Empty, Full
from datetime import datetime
from threading import Thread, Event

import spead2
import spead2.recv

from katcp import Sensor, ProtocolFlags
from katcp.kattypes import (Int, Str, request, return_reply)

import mpikat.effelsberg.edd.pipeline.EDDPipeline as EDDPipeline
import mpikat.utils.numa as numa

log = logging.getLogger("mpikat.spead_fi_server")

sensor_logging_queue = Queue()

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
        @brief   Drop any current FITS writer connection
        """
        if not self._transmit_socket is None:
            self._transmit_socket.shutdown(socket.SHUT_RDWR)
            self._transmit_socket.close()
            self._transmit_socket = None
            self._has_connection.clear()


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


class FitsInterfaceServer(EDDPipeline.EDDPipeline):
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
        EDDPipeline.EDDPipeline.__init__(self, ip, port, dict(input_data_streams=[], id="fits_interface", type="fits_interface", fits_writer_ip="localhost", fits_writer_port=5002))
        self._configured = False
        self._capture_interface = None
        self._fw_connection_manager = None
        self._capture_thread = None
        self._shutdown = False

    @property
    def heap_group(self):
        return self._heap_group_sensor.value()

    @heap_group.setter
    def heap_group(self, value):
        self._heap_group_sensor.set_value(value)

    @property
    def nmcg(self):
        return self._nmcg_sensor.value()

    @nmcg.setter
    def nmcg(self, value):
        self._nmcg_sensor.set_value(value)

    def setup_sensors(self):
        """
        @brief   Setup monitoring sensors
        """
        EDDPipeline.EDDPipeline.setup_sensors(self)
        self._heap_group_sensor = Sensor.integer(
            "heap_group",
            description="Number of heaps for a timestamp",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._heap_group_sensor)
        self._nmcg_sensor = Sensor.integer(
            "nMCG",
            description="Number of multicast groups",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nmcg_sensor)
        self._nchannels_sensor = Sensor.integer(
            "nchannels",
            description="Number of channels in each section",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nchannels_sensor)
        self._integration_time_sensor = Sensor.float(
            "integration_time",
            description="Integration time",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._integration_time_sensor)
        self._power_status_sensor = Sensor.string(
            "POWER_STATUS_PNG",
            description="Power in each stream",
            default= DEFAULT_BLOB,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._power_status_sensor)
        self._update_sensors_callback = tornado.ioloop.PeriodicCallback(
            self.update_sensors, 5000)
        self._update_sensors_callback.start()

    def update_sensors(self):
        try:
            log.debug("Updating sensor values")
            q_has_data = False
            while True:
                try:
                    data = sensor_logging_queue.get(False)
                    q_has_data = True
                except Empty:
                    break
            if not q_has_data:
                log.debug("No updates available for sensors")
                return
            self._integration_time_sensor.set_value(data.integration_time)
            self._heap_group_sensor.set_value(data.nsections)
            self._nchannels_sensor.set_value(data.sections[0].nchannels)
            self._power_status_sensor.set_value(self.plot_data(data))
        except Exception:
            log.exception("Exception while updating sensor values")

    def plot_data(self, packet):
        fig = plt.figure(1)
        plt.clf()
        fig.suptitle("Power Status")
        logX_ndON = (np.log10(packet.sections[0].data)*10)
        logX_ndOFF = (np.log10(packet.sections[1].data)*10)
        ax1 = plt.subplot(211)
        plt.plot(logX_ndON[:])
        ax2 = plt.subplot(212)
        plt.plot(logX_ndOFF[:])
        power = StringIO()
        #fig.savefig('plot.png', dpi=fig.dpi)
        plt.savefig(power, format='png', dpi=100)
        power.seek(0)
        power_png = base64.b64encode(power.buf).replace("\n", "")
        return power_png


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
        self.nmcg = len(self._config['input_data_streams'])
        self.heap_group = len(self._config['input_data_streams'])

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
            self._fw_connection_manager.drop_connection()
            self._fw_connection_manager.stop()
        self._fw_connection_manager = FitsWriterConnectionManager( self._config["fits_writer_ip"], self._config["fits_writer_port"])
        self._fw_connection_manager.start()

        self._stop_capture()
        self._configured = True
        self._nmcg_sensor.set_value(self.nmcg)


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
        handler = StreamHandler(self.heap_group, fw_socket)
        self._capture_thread = CaptureData(self.mc_interface,
                                           self.mc_port,
                                           self._capture_interface,
                                           self.nmcg,
                                           self.heap_group,
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
    def measurement_start(self):
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

    def __init__(self, mc_ip, mc_port, capture_ip, nmcg, heap_group, handler):
        """
        @brief      Initialization of the CaptureData thread

        @param  mc_ip              Array of multicast group IPs
        @param  mc_port            Port number of multicast streams
        @param  capture_ip         Interface to capture streams from MCG  on
        @param  nmcg               number of multicast groups to subscribe
        @param  heap_group         numbers of heaps for a given timestamp
        @param  handler            Object that handles data in the stream
        """
        Thread.__init__(self, name=self.__class__.__name__)
        self._mc_ip = mc_ip
        self._mc_port = mc_port
        self._capture_ip = capture_ip
        self._nmcg = nmcg
        self._stop_event = Event()
        self._handler = handler

    def stop(self):
        """
        @brief      Stop the capture thread
        """
        self.stream.stop()
        self._stop_event.set()

    def resource_allocation(self):
        thread_pool = spead2.ThreadPool(threads=4)
        self.stream = spead2.recv.Stream(thread_pool, spead2.BUG_COMPAT_PYSPEAD_0_5_2, max_heaps=64, ring_heaps=64, contiguous_only = False)
        pool = spead2.MemoryPool(16384, ((32*4*1024**2)+1024), max_free=64, initial=64)
        self.stream.set_memory_allocator(pool)
        self.rb = self.stream.ringbuffer

    def mcg_subscription(self):
        log.debug(" Multicast subscribe ...")
        for i in range(self._nmcg):
            log.debug(" - Subs {}: ip: {}, port: {}".format(i,self._mc_ip[i], self._mc_port ))
            self.stream.add_udp_reader(self._mc_ip[i], int(self._mc_port), max_size = 9200L,
                buffer_size= 1073741820L, interface_address=self._capture_ip)

    def run(self):
        self.resource_allocation()
        self.mcg_subscription()
        self._handler(self.stream)


class HeapPacket(object):
    def __init__(self):
        self._first_heap = True

    def heap_items(self):
        """
        @brief      Description of heap items
        """
        self.ig = spead2.ItemGroup()
        self.ig.add_item(5632, "timestamp_count", "", (6,), dtype=">B")
        self.ig.add_item(5633, "polID", "", (1,), dtype=">I")
        self.ig.add_item(5634, "ndStatus", "", (1,), dtype=">I")
        self.ig.add_item(5635, "fft_length", "", (1,), dtype=">I")
        self.ig.add_item(5636, "nheaps", "", (1,), dtype=">I")
        self.ig.add_item(5637, "synctime", "", (6,), dtype=">B")
        self.ig.add_item(5638, "sampling_rate", "", (1,), dtype=">I")
        self.ig.add_item(5639, "nspectrum", "", (1,), dtype=">I")
        return self.ig

    def unpack_heap(self, heap):
        items = self.ig.update(heap)
        for item in items.values():
            if (item.id == 5635) and (self._first_heap):
                self.nchannels = int((item.value/2)+1)
                self.ig.add_item(5640, "data", "", (self.nchannels,), dtype="<f")
                self._first_heap = False
            log.info("Iname: {}, Ivalue: {}".format(item.name, item.value))
            setattr(self, item.name, item.value)
        ts_count = ''
        sync = ''
        for i in range(6):
            ts_count = ts_count+'{0:08b}'.format(self.timestamp_count[i])
            sync = sync+'{0:08b}'.format(self.synctime[i])
        ts_count = int(ts_count,2)
        sync = int(sync,2)
        self.integtime = (self.nspectrum*self.fft_length)/self.sampling_rate
        t = (sync+((ts_count+(self.nspectrum*self.fft_length)/2)/self.sampling_rate))
        self.timestamp = datetime.fromtimestamp(t).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-2]


def build_fw_type(nsections, nchannels):
    class FWData(ctypes.LittleEndianStructure):
        _fields_ = [
            ('section_id', ctypes.c_uint32),
            ('nchannels', ctypes.c_uint32),
            ('data', ctypes.c_float * nchannels)
        ]

    class FWPacket(ctypes.LittleEndianStructure):
        _fields_ = [
            ("data_type", ctypes.c_char * 4),
            ("channel_data_type", ctypes.c_char * 4),
            ("packet_size", ctypes.c_uint32),
            ("backend_name", ctypes.c_char * 8),
            ("timestamp", ctypes.c_char * 28),
            ("integration_time", ctypes.c_uint32),
            ("blank_phases", ctypes.c_uint32),
            ("nsections", ctypes.c_uint32),
            ("blocking_factor", ctypes.c_uint32),
            ("sections", FWData * nsections)
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
    def __init__(self, heap_group, transmit_socket, max_age=5.0):
        """
        @brief      Initialization of the StreamHandler thread

        @param  heap_group         Number of heaps for a given timestamp
        @param  transmit_soc       FITS writer interface to which the data is sent
        @param  max_age            timeout
        """
        self._nsections = heap_group
        self._data_to_fw = {}
        self._nphases = 1
        self._transmit_socket = transmit_socket
        self._max_age = max_age
        self._first_heap = True
        self._nheaps = 0
        self._complete_heaps = 0
        self._incomplete_heaps = 0

    def __call__(self, stream):
        """
        @brief      Handle a raw packet from the network

        @param      stream  heaps from multiple multicast groups
        """
        self.rb = stream.ringbuffer
        log.info("Reading stream..")
        self.packet = HeapPacket()
        self.packet.heap_items()
        for heap in stream:
            self._nheaps += 1
            if isinstance(heap, spead2.recv.IncompleteHeap):
                self._incomplete_heaps += 1
                continue
            else:
                self._complete_heaps += 1
            self.packet.unpack_heap(heap)
            if not self._first_heap:
                self.aggregate_data(self.packet)
            self._first_heap = False

    def aggregate_data(self, packet):
        sec_id = (packet.polID*2)+packet.ndStatus
        key = tuple(packet.timestamp_count)
        if key not in self._data_to_fw:
            fw_pkt = build_fw_object(self._nsections, int(packet.nchannels), packet.timestamp,
                                     (packet.integtime*1000), self._nphases)
            fw_pkt.sections[int(sec_id)].data[:]=packet.data
            self._data_to_fw[key] = [time.time(), 1, fw_pkt]
        else:
            self._data_to_fw[key][1] += 1
            self._data_to_fw[key][2].sections[int(sec_id)].data[:] = packet.data
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
                             " will be flushed to FITS writer."))
                del self._data_to_fw[key]
            elif (hits == self._nsections):
                try:
                    sensor_logging_queue.put((fw_packet), False)
                except Queue.Full:
                    sensor_logging_queue.get()
                    sensor_logging_queue.put((fw_packet), False)
                log.debug(
                    "Sending complete packet with timestamp: {}".format(
                        timestamp))
                log.debug("Ringbuffer size: {}".format(self.rb.size()))
                log.debug("Heap statistics: total_heaps: {}, complete_heaps: {}, incomplete_heaps: {}".format(
                          self._nheaps, self._complete_heaps, self._incomplete_heaps))
                self._transmit_socket.send(bytearray(fw_packet))
                del self._data_to_fw[key]
        log.debug(
            "Number of active packets post-flush: {}".format(
                len(self._data_to_fw)))


@tornado.gen.coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down FITS writer interface server")
    yield server.stop()
    ioloop.stop()


if __name__ == "__main__":
    parser = EDDPipeline.getArgumentParser()
    # ToDo: pass fits writer properties as configuration
    parser.add_argument('--fw-ip', dest='fw_ip', type=str, default="localhost",
                      help='The ip for the fits writer')
    parser.add_argument('--fw-port', dest='fw_port', type=int, default=5002,
                      help='The port number for the redis server')
    args = parser.parse_args()

    logging.getLogger().addHandler(logging.NullHandler())
    log = logging.getLogger('mpikat')
    log.setLevel(args.log_level.upper())
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
        level=args.log_level.upper(),
        logger=log)

    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting Pipeline instance")
    server = FitsInterfaceServer(
        args.host, args.port, args.fw_ip, args.fw_port)
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
