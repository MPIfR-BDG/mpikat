import logging
import time
import socket
import select
import json
import coloredlogs
import signal
from lxml import etree
from threading import Thread, Event, Lock
from tornado.gen import coroutine
from tornado.ioloop import PeriodicCallback, IOLoop
from katcp import Sensor, AsyncDeviceServer, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str
from mpikat.effelsberg.status_config import EFF_JSON_CONFIG

TYPE_CONVERTER = {
    "float": float,
    "int": int,
    "string": str,
    "bool": int
}

JSON_STATUS_MCAST_GROUP = '224.168.2.132'
JSON_STATUS_PORT = 1602

log = logging.getLogger('mpikat.effelsberg.status_server')

STATUS_MAP = {
    "error": 3,  # Sensor.STATUSES 'error'
    "norm": 1,  # Sensor.STATUSES 'nominal'
    "ok": 1,  # Sensor.STATUSES 'nominal'
    "warn": 2  # Sensor.STATUSES 'warn'
}


class StatusCatcherThread(Thread):

    def __init__(
            self,
            mcast_group=JSON_STATUS_MCAST_GROUP,
            mcast_port=JSON_STATUS_PORT):
        self._mcast_group = mcast_group
        self._mcast_port = mcast_port
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
        while not self._stop_event.is_set():
            try:
                r, o, e = select.select([self._sock], [], [], 0.0)
                if r:
                    log.debug("Data in socket... reading data")
                    data, _ = self._sock.recvfrom(1 << 17)
                else:
                    if data is not None:
                        log.debug("Updating data")
                        self.data = json.loads(data)
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
        if hasattr(socket, "SO_REUSEPORT"):
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        self._sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, 20)
        self._sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1 << 15)
        self._sock.setblocking(0)
        self._sock.bind(('', self._mcast_port))
        intf = socket.gethostbyname(socket.gethostname())
        self._sock.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF,
                              socket.inet_aton(intf))
        self._sock.setsockopt(
            socket.SOL_IP,
            socket.IP_ADD_MEMBERSHIP,
            socket.inet_aton(
                self._mcast_group) +
            socket.inet_aton(intf))
        log.debug("Socket open")

    def _close_socket(self):
        intf = socket.gethostbyname(socket.gethostname())
        self._sock.setsockopt(
            socket.SOL_IP,
            socket.IP_DROP_MEMBERSHIP,
            socket.inet_aton(
                self._mcast_group) +
            socket.inet_aton(intf))
        self._sock.close()


class JsonStatusServer(AsyncDeviceServer):
    VERSION_INFO = ("reynard-eff-jsonstatusserver-api", 0, 1)
    BUILD_INFO = ("reynard-eff-jsonstatusserver-implementation", 0, 1, "rc1")

    def __init__(self, server_host, server_port,
                 mcast_group=JSON_STATUS_MCAST_GROUP,
                 mcast_port=JSON_STATUS_PORT,
                 parser=EFF_JSON_CONFIG,
                 dummy=False):
        self._mcast_group = mcast_group
        self._mcast_port = mcast_port
        self._parser = parser
        self._dummy = dummy
        if not dummy:
            self._catcher_thread = StatusCatcherThread()
        else:
            self._catcher_thread = None
        self._monitor = None
        self._updaters = {}
        self._controlled = set()
        super(JsonStatusServer, self).__init__(server_host, server_port)

    @coroutine
    def _update_sensors(self):
        log.debug("Updating sensor values")
        data = self._catcher_thread.data
        if data is None:
            log.warning("Catcher thread has not received any data yet")
            return
        for name, params in self._parser.items():
            if name in self._controlled:
                continue
            if "updater" in params:
                self._sensors[name].set_value(params["updater"](data))

    def start(self):
        """start the server"""
        super(JsonStatusServer, self).start()
        if not self._dummy:
            self._catcher_thread.start()
            self._monitor = PeriodicCallback(
                self._update_sensors, 1000, io_loop=self.ioloop)
            self._monitor.start()

    def stop(self):
        """stop the server"""
        if not self._dummy:
            if self._monitor:
                self._monitor.stop()
            self._catcher_thread.stop()
        return super(JsonStatusServer, self).stop()

    @request()
    @return_reply(Str())
    def request_xml(self, req):
        """request an XML version of the status message"""
        def make_elem(parent, name, text):
            child = etree.Element(name)
            child.text = text
            parent.append(child)

        @coroutine
        def convert():
            try:
                root = etree.Element("TelescopeStatus",
                                     attrib={
                                         "timestamp": str(time.time())
                                     })
                for name, sensor in self._sensors.items():
                    child = etree.Element("TelStat")
                    make_elem(child, "Name", name)
                    make_elem(child, "Value", str(sensor.value()))
                    make_elem(child, "Status", str(sensor.status()))
                    make_elem(child, "Type", self._parser[name]["type"])
                    if "units" in self._parser[name]:
                        make_elem(child, "Units", self._parser[name]["units"])
                    root.append(child)
            except Exception as error:
                req.reply("ok", str(error))
            else:
                req.reply("ok", etree.tostring(root))
        self.ioloop.add_callback(convert)
        raise AsyncReply

    @request()
    @return_reply(Str())
    def request_json(self, req):
        """request an JSON version of the status message"""
        return ("ok", self.as_json())

    def as_json(self):
        """Convert status sensors to JSON object"""
        out = {}
        for name, sensor in self._sensors.items():
            out[name] = str(sensor.value())
        return json.dumps(out)

    @request(Str())
    @return_reply(Str())
    def request_sensor_control(self, req, name):
        """take control of a given sensor value"""
        if name not in self._sensors:
            return ("fail", "No sensor named '{0}'".format(name))
        else:
            self._controlled.add(name)
            return ("ok", "{0} under user control".format(name))

    @request()
    @return_reply(Str())
    def request_sensor_control_all(self, req):
        """take control of all sensors value"""
        for name, sensor in self._sensors.items():
            self._controlled.add(name)
        return ("ok", "{0} sensors under user control".format(
            len(self._controlled)))

    @request()
    @return_reply(Int())
    def request_sensor_list_controlled(self, req):
        """List all controlled sensors"""
        count = len(self._controlled)
        for name in list(self._controlled):
            req.inform("{0} -- {1}".format(name, self._sensors[name].value()))
        return ("ok", count)

    @request(Str())
    @return_reply(Str())
    def request_sensor_release(self, req, name):
        """release a sensor from user control"""
        if name not in self._sensors:
            return ("fail", "No sensor named '{0}'".format(name))
        else:
            self._controlled.remove(name)
            return ("ok", "{0} released from user control".format(name))

    @request()
    @return_reply(Str())
    def request_sensor_release_all(self, req):
        """take control of all sensors value"""
        self._controlled = set()
        return ("ok", "All sensors released")

    @request(Str(), Str())
    @return_reply(Str())
    def request_sensor_set(self, req, name, value):
        """Set the value of a sensor"""
        if name not in self._sensors:
            return ("fail", "No sensor named '{0}'".format(name))
        if name not in self._controlled:
            return ("fail", "Sensor '{0}' not under user control".format(name))
        try:
            param = self._parser[name]
            value = TYPE_CONVERTER[param["type"]](value)
            self._sensors[name].set_value(value)
        except Exception as error:
            return ("fail", str(error))
        else:
            return (
                "ok", "{0} set to {1}".format(
                    name, self._sensors[name].value()))

    def setup_sensors(self):
        """Set up basic monitoring sensors.
        """
        for name, params in self._parser.items():
            if params["type"] == "float":
                sensor = Sensor.float(
                    name,
                    description=params["description"],
                    unit=params.get("units", None),
                    default=params.get("default", 0.0),
                    initial_status=Sensor.UNKNOWN)
            elif params["type"] == "string":
                sensor = Sensor.string(
                    name,
                    description=params["description"],
                    default=params.get("default", ""),
                    initial_status=Sensor.UNKNOWN)
            elif params["type"] == "int":
                sensor = Sensor.integer(
                    name,
                    description=params["description"],
                    default=params.get("default", 0),
                    unit=params.get("units", None),
                    initial_status=Sensor.UNKNOWN)
            elif params["type"] == "bool":
                sensor = Sensor.boolean(
                    name,
                    description=params["description"],
                    default=params.get("default", False),
                    initial_status=Sensor.UNKNOWN)
            else:
                raise Exception(
                    "Unknown sensor type '{0}' requested".format(
                        params["type"]))
            self.add_sensor(sensor)


@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='Host interface to bind to')
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='Port number to bind to')
    parser.add_option('', '--log-level', dest='log_level', type=str,
                      help='Port number of status server instance', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    logging.getLogger('katcp').setLevel(logging.DEBUG)
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    ioloop = IOLoop.current()
    log.info("Starting JsonStatusServer instance")
    server = JsonStatusServer(opts.host, opts.port)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    from optparse import OptionParser
    main()
