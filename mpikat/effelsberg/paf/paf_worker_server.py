import threading
import logging
import tornado
import coloredlogs
import signal
import json
import os
from astropy.time import Time
import astropy.units as units
from optparse import OptionParser
from tornado.gen import Return, coroutine
from tornado.iostream import IOStream
from katcp import AsyncDeviceServer, Message, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str, Discrete, Float
from mpikat.effelsberg.paf.pipeline import PIPELINES


log = logging.getLogger("mpikat.paf_worker_server")

#PIPELINES = {"mock":mock.Mock()}

# P8_IP="10.17.8.2"


class PafWorkerServer(AsyncDeviceServer):
    """
    @brief Interface object which accepts KATCP commands

    """
    VERSION_INFO = ("mpikat-paf-api", 1, 0)
    BUILD_INFO = ("mpikat-paf-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    PIPELINE_STATES = ["idle", "configuring", "ready",
                       "starting", "running", "stopping",
                       "deconfiguring", "error"]

    def __init__(self, ip, port):
        """
        @brief Initialization of the PafWorkerServer object

        @param ip       IP address of the server
        @param port     port of the PafWorkerServer

        """
        super(PafWorkerServer, self).__init__(ip, port)
        self.ip = ip
        self._managed_sensors = []

    def add_pipeline_sensors(self):
        """
        @brief Add pipeline sensors to the managed sensors list

        """
        for sensor in self._pipeline_instance.sensors:
            self.add_sensor(sensor)
            self._managed_sensors.append(sensor)
        self.mass_inform(Message.inform('interface-changed'))

    def remove_pipeline_sensors(self):
        """
        @brief Remove pipeline sensors from the managed sensors list

        """
        for sensor in self._managed_sensors:
            self.remove_sensor(sensor)
        self._managed_sensors = []
        self.mass_inform(Message.inform('interface-changed'))

    def state_change(self, state, callback):
        """
        @brief callback function for state changes

        @parma callback object return from the callback function from the pipeline
        """
        log.info('New state of the pipeline is {}'.format(str(state)))
        self._pipeline_sensor_status.set_value(str(state))

    @coroutine
    def start(self):
        """Start PafWorkerServer server"""
        super(PafWorkerServer, self).start()

    @coroutine
    def stop(self):
        """Stop PafWorkerServer server"""
        yield super(PafWorkerServer, self).stop()

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        self._device_status = Sensor.discrete(
            "device-status",
            "Health status of PafWorkerServer",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._device_status)

        self._pipeline_sensor_name = Sensor.string(
            "pipeline-name",
            "the name of the pipeline",
            "")
        self.add_sensor(self._pipeline_sensor_name)

        self._pipeline_sensor_status = Sensor.discrete(
            "pipeline-status",
            description="Status of the pipeline",
            params=self.PIPELINE_STATES,
            default="idle",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._pipeline_sensor_status)

        self._ip_address = Sensor.string(
            "ip",
            description="the ip address of the node controller",
            default=os.environ['PAF_NODE_IP'],
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._ip_address)

        self._mac_address = Sensor.string(
            "mac",
            description="the mac address of the node controller",
            default=os.environ['PAF_NODE_MAC'],
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._mac_address)

    @request(Str())
    @return_reply(Str())
    def request_configure(self, req, config_json):
        """
        @brief      Configure pipeline

        @param      pipeline    name of the pipeline
        """
        @coroutine
        def configure_pipeline():
            self._pipeline_sensor_name.set_value(pipeline_name)
            log.info("Configuring pipeline {}".format(
                self._pipeline_sensor_name.value()))
            try:
                _pipeline_type = PIPELINES[self._pipeline_sensor_name.value()]
            except KeyError as error:
                msg = "No pipeline called '{}', available pipeline are: {}".format(
                    self._pipeline_sensor_name.value(), " ".join(PIPELINES.keys()))
                self._pipeline_sensor_name.set_value("")
                log.error("{}".format(msg))
                req.reply("fail", msg)
            try:
                log.debug(
                "Trying to create pipeline instance {}".format(pipeline_name))
                self._pipeline_instance = _pipeline_type()
                self.add_pipeline_sensors()
                self._pipeline_instance.callbacks.add(self.state_change)
                log.debug(
                    "Trying to configure pipeline {}".format(pipeline_name))
                config = json.loads(config_json)
                log.debug("Unpacked config: {}".format(config))
                capture_start_time = Time.now() + 27.0 * units.s
                frequency = config["frequency"]
                self._pipeline_instance.configure(
                    capture_start_time,
                    frequency,
                    self._ip_address.value())
            except Exception as error:
                self._pipeline_sensor_name.set_value("")
                msg = "Couldn't start configure pipeline instance {}".format(
                    str(error))
                log.error("{}".format(msg))
                req.reply("fail", msg)
            msg = "pipeline instance {} configured".format(
                self._pipeline_sensor_name.value())
            log.info("{}".format(msg))
            req.reply("ok", msg)
        if self._pipeline_sensor_status.value() == "idle":
            config_dict = json.loads(config_json)
            pipeline_name = config_dict["mode"]
            self.ioloop.add_callback(configure_pipeline)
            raise AsyncReply
        else:
            msg = "Can't configure pipeline, status = {}".format(
                self._pipeline_sensor_status.value())
            log.info("{}".format(msg))
            return ("fail", msg)

    @request(Str())
    @return_reply(Str())
    def request_start(self, req, config_json):
        """
        @brief      Start pipeline

        """
        @coroutine
        def start_pipeline():
            try:
                config = json.loads(config_json)
                utc_start_process = Time.now()
                source_name = config["source-name"]
                ra = config["ra"]
                dec = config["dec"]
                self._pipeline_instance.start(
                    utc_start_process, source_name, ra, dec)
            except Exception as error:
                msg = "Couldn't start pipeline server {}".format(error)
                log.info("{}".format(msg))
                req.reply("fail", msg)
            msg = "Starting pipeline {}".format(
                self._pipeline_sensor_name.value())
            log.info("{}".format(msg))
            req.reply("ok", msg)
        if self._pipeline_sensor_status.value() == "ready":
            self.ioloop.add_callback(start_pipeline)
            raise AsyncReply
        else:
            msg = "Pipeline is not in the state of configured, status = {} ".format(
                self._pipeline_sensor_status.value())
            log.info("{}".format(msg))
            return ("fail", msg)

    @request()
    @return_reply(Str())
    def request_stop(self, req):
        """
        @brief      Stop pipeline

        """
        @coroutine
        def stop_pipeline():
            self._pipeline_sensor_status.set_value("stopping")
            try:
                self._pipeline_instance.stop()
            except Exception as error:
                msg = "Couldn't stop pipeline {}".format(error)
                log.info("{}".format(msg))
                req.reply("fail", msg)
            msg = "Stopping pipeline {}".format(
                self._pipeline_sensor_name.value())
            log.info("{}".format(msg))
            req.reply("ok", msg)

        if self._pipeline_sensor_status.value() == "running":
            self.ioloop.add_callback(stop_pipeline)
            raise AsyncReply
        else:
            msg = "Nothing to stop, status = {}".format(
                self._pipeline_sensor_status.value())
            log.info("{}".format(msg))
            return ("fail", msg)

    @request()
    @return_reply(Str())
    def request_deconfigure(self, req):
        """
        @brief      Deconfigure pipeline

        """
        @coroutine
        def deconfigure():
            log.info("Deconfiguring pipeline {}".format(
                self._pipeline_sensor_name.value()))
            try:
                self.remove_pipeline_sensors()
                self._pipeline_instance.deconfigure()
                del self._pipeline_instance
            except Exception as error:
                msg = "Couldn't deconfigure pipeline {}".format(error)
                log.error("{}".format(msg))
                req.reply("fail", msg)
            msg = "Deconfigured pipeline {}".format(
                self._pipeline_sensor_name.value())
            log.info("{}".format(msg))
            req.reply("ok", msg)
            self._pipeline_sensor_name.set_value("")
        if self._pipeline_sensor_status.value():
            self.ioloop.add_callback(deconfigure)
            raise AsyncReply
        else:
            msg = "Nothing to deconfigure, status = {}".format(
                self._pipeline_sensor_status.value())
            log.info("{}".format(msg))
            return ("fail", msg)

    @request()
    @return_reply(Str())
    def request_avail_pipeline(self, req):
        """
        @brief Get availiable pipelines
        """
        log.info("Requesting list of available pipeline")
        for key in PIPELINES.keys():
            req.inform("{}".format(key))
        return ("ok", len(PIPELINES))


@coroutine
def on_shutdown(ioloop, server):
    log.info('Shutting down server')
    yield server.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='Host interface to bind to')
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='Port number to bind to')
    parser.add_option('', '--log_level', dest='log_level', type=str,
                      help='logging level', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting PafWorkerServer instance")
    server = PafWorkerServer(opts.host, opts.port)
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
