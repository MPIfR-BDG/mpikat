
#start and stop commands implemented and it works.

import tornado
import logging
import signal
import socket
import Queue
import errno
import threading
from threading import Thread
from threading import Event 
from optparse import OptionParser
import numpy as np
import struct
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, Int, request, return_reply)


log = logging.getLogger("FitsInterfaceServer")

data_Queue = Queue.Queue()

class FitsInterfaceServer(AsyncDeviceServer):
    """
    @brief Interface object which accepts KATCP commands

    """
    VERSION_INFO = ("example-api", 1, 0)
    BUILD_INFO = ("example-implementation", 0, 1, "")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    # Optionally set the KATCP protocol version and features. Defaults to
    # the latest implemented version of KATCP, with all supported optional
    # featuresthat's all of the receivers
    PROTOCOL_INFO = ProtocolFlags(5, 0, set([
        ProtocolFlags.MULTI_CLIENT,
        ProtocolFlags.MESSAGE_IDS,
    ]))

    def __init__(self, ip, port):
        """
        @brief Initialization of the Roach2ResourceManager object

        @param ip       IP address of the board
        @param port     port of the board
        @param db       igui database object

        """
        self._no_active_beams = 0
        self._no_channels = 0
        self._integ_time = 0
        self._time_stamp = ""
        self._blank_phase = 4
        self._capture_thread = CaptureData("10.10.1.12", 60001, 2048)
        self._capture_thread.start()
        super(FitsInterfaceServer, self).__init__(ip, port)

    def start(self):
        super(FitsInterfaceServer, self).start()

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        self._device_status = Sensor.discrete(
            "device-status",
            description="Health status of FIServer",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status)
        self._active_beams = Sensor.float(
            "Number of active beams",
            description="Number of beams that are currently active",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._active_beams)
        self._channels = Sensor.float(
            "channels_per_beam",
            description="number of channels in each beam",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._channels)
        self._integration_period = Sensor.float(
            "integration_period",
            description="integration_period",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._integration_period)

    @request(Int(), Int(), Int(), Int())
    @return_reply()
    def request_configure(self, req, beams, channels, integTime, blankPhases):
        """
        @brief configures metadata for tcp packets sent to Fits Writer

        @param no. of beams - 1 to 36
        @param no. of blank phases - 1 to 4
        @param number of channels per beam
        @param integration period in seconds
        """
        self.configure_fits_metadata(beams, channels, integTime, blankPhases)
        return ("ok",)

    @request()
    @return_reply()
    def request_start(self, req):
        """
        @brief configures metadata for tcp packets sent to Fits Writer
        """
        self._capture_thread.start_capture()
        return ("ok",)

    @request()
    @return_reply()
    def request_stop(self, req):
        """
        @brief configures metadata for tcp packets sent to Fits Writer
        """
        self._capture_thread.stop_capture()
        return ("ok",)

    def configure_fits_metadata(self, beams, channels, integTime, blankPhases):
        self._no_active_beams = beams
        self._no_blank_phases = blankPhases
        self._no_channels = channels
        self._integ_time = integTime
        self._channels.set_value(self._no_channels)
        self._active_beams.set_value(self._no_active_beams)
        self._integration_period.set_value(self._integ_time)

class CaptureData(Thread):
    """
    @brief Captures data sent by the roach2 board
    """
    def __init__(self, ip, port, channels, name="CaptureData"):
        Thread.__init__(self, name=name)
        self._address = (ip, port)
        self._buffer_size = 4*(channels+2)
        self._data = "" 
        self._udpSoc = self._capture_socket() 
        self._stop_event = Event()
        #Data capture not activated when the thread is initialized
        self._stop_event.set()
        self._aggregate_thread = AggregateData(self._data, 2)


    def _capture_socket(self):
        self._udpSoc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udpSoc.bind((self._address))
        return self._udpSoc

    def start_capture(self):
        self._stop_event.clear()

    def stop_capture(self):
        self._stop_event.set()

    def run(self):
	while True:
            if not self._stop_event.is_set():
               try:
                    data = ""
                    while len(data) < self._buffer_size:
                        data += self._udpSoc.recv(self._buffer_size-len(data))
                    self._data = data
                    self._aggregate_thread.start_aggregating(self._data)
               except socket.error as error:
                   error_id = error.args[0]
                   if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                       raise 
                   return

class AggregateData():
    """
    @brief Aggregates spectrometer data from polarization channel 1 and 2 for the given time 
           before sending to the fits writer
    """
    def __init__(self, data, no_streams, name="AggregateDate"):
        self._no_streams = no_streams
        self._data_to_process = data
        self._data_stream = []
        self._final_data = self._no_streams*[[]]
        self._count = 0
        self._ref_seq_no = 0

    def phase_extract(self, num):
        mask = 0xf0000000
        phase = (num&mask) >> 28
        if (phase == 0): phase = 4
        return phase

    def pol_extract(self, num):
        mask = 0x0f000000
        pol = (num&mask) >> 24
        return pol

    def extract_sequence_num(self, num1,num2):
        counter_num = struct.pack(">II", num1,num2)
        unpack_num= np.zeros(1,dtype=np.int64)
        unpack_num = struct.unpack(">q", counter_num)
        mask = 0x00ffffffffffffff
        seq_no = unpack_num[0]&mask
        return seq_no

    def start_aggregating(self, data_to_process):
        self._count += 1
        data = np.zeros(2050,dtype=np.uint32)
        data = struct.unpack('>2050I', data_to_process)
        phase = self.phase_extract(data[0])
        polID = self.pol_extract(data[0])
        sequence_num = self.extract_sequence_num(data[0],data[1])
        print "Extracted phase: ", phase,
        print "Extracted polID: ", polID,
        print "Extracted seq. no.:: ", sequence_num
        #Capturing logic based on sequence no.
        if (self._count ==1):
            self._data_stream = [data[2:]]
            self._ref_seq_no = sequence_num
        elif ((sequence_num == (self._ref_seq_no+1)) or (sequence_num == (self._ref_seq_no-1))):
            self._data_stream.append(data[2:])
            data_Queue.put((self._no_streams, self._data_stream))
            self._count = 0
            self._data_stream = []
      #  strm, data_from_queue = data_Queue.get()
      #  print "from queue:                  ", len(data_from_queue)

class SendToFW(Thread):
    """
    @brief The data is formated with fits header and sent to the fits writer     
    """
    def __init__(self, server_ip, tcp_port, name="SendToFW"):
        Thread.__init__(self, name=name)
        self._serverAddr = (server_ip, tcp_port)
        self._serverSoc = self._server_socket()
        self._tcpSoc = self._tcp_data_socket()
        self._time_stamp = ""
        self._integ_time = 16
        self._blank_phase = 1
        self._no_streams = 0
        self._sending_stop_event = Event()
        self._sending_stop_event.set()

    def _server_socket(self):
        self._serverSoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._serverSoc.bind((server_ip, tcp_port))
        self._serverSoc.listen(1)
        self._serverSoc.settimeout(5) 
        return self._serverSoc

    def _tcp_data_socket(self):
        self._tcpSoc, addr = self._serverSoc.accept()
        return self._tcpSoc

    def start_sending_toFW(self):
        self._sending_stop_event.clear()
        self._tcpSoc, addr = self._FW_IP()

    def stop_sending_FW(self):
        self._sending_stop_event.set()
        self._tcpSoc.shutdown(socket.SHUT_RDWR)
        self._tcpSoc.close()
        del self._tcpSoc

    def pack_FI_metadata(self):
        #IEEE - big endian, EEEI - little endian
        headerData = ['EEEI']
        data_type = '<4s'

        #Data format of channel data: 'F' - Float
        channelDataType = 'F   '
        headerData.append(channelDataType)
        data_type += '4s'

        #Length of the data package
        lengthHeader = struct.calcsize('4s4si8s28siiii')
        lengthSecHeaders = struct.calcsize('ii')*self._no_active_beams
        lengthChannelData = struct.calcsize('f')*self._no_channels*self._no_active_beams
        dataPackageLength = lengthHeader+lengthSecHeaders+lengthChannelData
        headerData.append(dataPackageLength)
        data_type += 'l'

        #Backend name
        BEName = 'EDD     '
        headerData.append(BEName)
        data_type += '8s'

       #Time info
        headerData.append(self._time_stamp)
        data_type += '28s'

       #Integration time
        headerData.append(self._integ_time)
        data_type += 'l'

        #Phase number
        headerData.append(self._blank_phase)
        data_type += 'l'

        #Number of Back End Sections 
        headerData.append(self._no_streams)
        data_type += 'l'

    def pack_FI_data(self, data):
        data_type = ''
        eddData = []
        dataPointer = 0

        for BESecIndex in range(self._no_active_beams):
            BESecNum = BESecIndex+1
            data_type += 'l'
            eddData.append(BESecNum)
            data_type += 'l'
            eddData.append(self._no_channels)
            #data_type += '1024f'
            data_type += str('%sf' % self._no_channels)

            eddData.extend(channelData[dataPointer:dataPointer+channels])
            dataPointer += self._no_channels

        return(data_type, eddData)

    def pack_tcpData(self, dataType, data):
        packer = struct.Struct(dataType)
        packed_data = packer.pack(*data)
        return packed_data

    def pack_data(self):
        self._no_streams, data_from_queue = data_Queue.get()
        header_format, header_data = _pack_FI_metadata()
        data_format, pol_data = self.pack_FI_data(data_from_queue)
        tcp_data_format = header_format + data_format
        header_data.extend(pol_data)
        tcp_data = header_data
        data_to_send = self.pack_tcpData(tcp_data_format, tcp_data)
        return data_to_send

    def run(self):
        while True:
            if not self._sending_stop_event.is_set():
               try:
                #pack data
                data_to_fw = self.pack_data()
                #send data
                self._tcpSoc.send(data_to_fw)
               except socket.error as error:
                   error_id = error.args[0]
                   if error_id == errno.EAGAIN or error_id == errno.EWOULDBLOCK:
                       raise
                   return

@tornado.gen.coroutine
def on_shutdown(ioloop, server):
    print('Shutting down')
    yield server.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('', '--host', dest='host', type=str,
        help='Host interface to bind to', default='127.0.0.1')
    parser.add_option('-p', '--port', dest='port', type=long,
        help='Port number to bind to', default=5000)
    parser.add_option('', '--log_level', dest='log_level', type=str,
        help='Defauly logging level', default="INFO")
    (opts, args) = parser.parse_args()
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('FitsInterfaceServer')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(opts.log_level.upper())
    ioloop = tornado.ioloop.IOLoop.current()
    #CaptureData("10.10.1.12", 60001).start()
    server = FitsInterfaceServer(opts.host, opts.port)
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
