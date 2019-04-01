import logging
import time
import socket
import fcntl
import struct
import select
import atexit
import parser as spead_parser
import sys
import ctypes as C
from threading import Thread, Lock, Event
from Queue import Queue

lock = Lock()

PORT = 7148

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger('root')


class SpeadHeader(C.BigEndianStructure):
    _fields_ = [
        ("magic_number", C.c_byte),
        ("version", C.c_byte),
        ("item_pointer_width", C.c_byte),
        ("heap_addr_width", C.c_byte),
        ("reserved", C.c_uint16),
        ("num_items", C.c_uint16),
    ]

    def __repr__(self):
        values = ", ".join(["{}={}".format(key, getattr(self, key))
                            for key, _ in self._fields_])
        return "<{} {}>".format(self.__class__.__name__, values)


class Descriptor(C.BigEndianStructure):
    _fields_ = [
        ('is_value', C.c_uint8, 1),
        ('id', C.c_uint32, 15),
        ('value', C.c_uint64, 48)
    ]

    def __repr__(self):
        values = ", ".join(["{}={}".format(key, getattr(self, key))
                            for key, _, _ in self._fields_])
        return "<{} {}>".format(self.__class__.__name__, values)


class DescriptorUnion(C.Union):
    _fields_ = [('struct', Descriptor), ('uint64', C.c_uint64)]


class R2Feng64Packet(C.BigEndianStructure):
    _fields_ = [
        ("header", SpeadHeader),
        ("descriptors", Descriptor * 11),
        ("data", C.c_byte * (256 * 8 * 2 * 2))
    ]

class HeapStartPacket(C.BigEndianStructure):
    _fields_ = [
        ("header", SpeadHeader),
        ("descriptors", Descriptor * 7),
        ("data", C.c_byte * 8192)
    ]

class HeapPacket(C.BigEndianStructure):
    _fields_ = [
        ("header", SpeadHeader),
        ("descriptors", Descriptor * 4),
        ("data", C.c_byte * 8192)
    ]

def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

class StatusCatcherThread(Thread):

    def __init__(
            self,
            mcast_group,
            intf):
        self._mcast_group = mcast_group
        self._mcast_port = PORT
        self._intf = intf
        self._sock = None
        self._lock = Lock()
        self._stop_event = Event()
        self._data = None
        Thread.__init__(self)
        self.daemon = True

    @property
    def data(self):
        with self._lock:
            return self._data

    @data.setter
    def data(self, d):
        with self._lock:
            self._data = d

    def start(self):
        self._open_socket()
        Thread.start(self)

    def stop(self):
        self._stop_event.set()
        self._close_socket()

    def run(self):
        data = None
        print "Entering run"
        while not self._stop_event.is_set():
            try:
                # print "select"
                log.debug("Selecting on sock")
                r, o, e = select.select([self._sock], [], [], 0.0)
                if r:
                    log.debug("Data in socket... reading data")
                    data, _ = self._sock.recvfrom(1 << 17)
                    print "Packet size = {}, header size = {}".format(
                        len(data), len(data)-8192)
                    try:
                        packet = HeapStartPacket.from_buffer_copy(data)
                    except:
                        try:
                            packet = HeapPacket.from_buffer_copy(data)
                        except:
                            continue
                    print packet.header
                    for descriptor in packet.descriptors:
                        print "   {}".format(descriptor)
                        if(descriptor.id == 3):
                            if(descriptor.value == 0):
                                print "heap start"
                            if(descriptor.value == (262144 - 8192)):
                                print "heap end"
                    print "data: {}".format(packet.data[:10])
                else:
                    if data is not None:
                        log.debug("Updating data")
                        print data
                    log.debug("Sleeping")
                    self._stop_event.wait(0.5)
            except Exception:
                log.exception("Error on status retrieval")
                log.debug("Sleeping for 5 seconds")
                self._stop_event.wait(5.0)

    def _open_socket(self):
        log.debug("Opening socket")
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # if hasattr(socket, "SO_REUSEPORT"):
        #    self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        #self._sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, 20)
        #self._sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)
        #self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 15)
        # self._sock.setblocking(0)
        self._sock.bind(('', self._mcast_port))
        # self._sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF,
        #                      socket.inet_aton(self._intf))
        self._sock.setsockopt(
            socket.SOL_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton(
                self._mcast_group) +
            socket.inet_aton(self._intf))
        log.debug("Socket open")

    def _close_socket(self):
        self._sock.setsockopt(
            socket.SOL_IP,
            socket.IP_DROP_MEMBERSHIP,
            socket.inet_aton(
                self._mcast_group) +
            socket.inet_aton(self._intf))
        self._sock.close()

if __name__ == "__main__":
    import sys
    import argparse

    msg = """
    Benchmarking tool for dp4a-based filterbanking beamformer application.
    Arguments can be passed as comma separated lists.
    """
    parser = argparse.ArgumentParser(usage=msg)
    parser.add_argument("-g", "--groups", type=list, nargs='+',
                        required=True, help="Multicast groups to subscribe to")
    parser.add_argument("-i", "--intf", type=str,
                        required=True, help="Interface to listen on")
    #parser.add_argument("-p","--port", type=int, required=True, help="Port to listen on")
    args = parser.parse_args()

    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('root')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(logging.ERROR)

    print "Groups:"
    for group in args.groups:
        print "".join(group)

    print "Interface:", args.intf

    threads = [StatusCatcherThread(
        "".join(i), get_ip_address(args.intf)) for i in args.groups]

    def exit_handler():
        log.info("Cleaning-up threads")
        for thread in threads:
            log.debug("Killing thread {}".format(thread))
            try:
                thread.stop()
            except Exception as e:
                log.error(str(e))
    atexit.register(exit_handler)
    for thread in threads:
        thread.start()
    while True:
        for thread in threads:
            if not thread.is_alive():
                exit_handler()
                break
