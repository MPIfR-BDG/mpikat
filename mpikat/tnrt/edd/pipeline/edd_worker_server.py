import logging
import tornado
import coloredlogs
import signal
import json
import os
from optparse import OptionParser
from tornado.gen import Return, coroutine
from tornado.iostream import IOStream
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str, Discrete, Float
from mpikat.effelsberg.edd.pipeline.pipeline import PIPELINES


log = logging.getLogger("mpikat.edd.pipeline.edd_worker_server")

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

    def state_change(self, state, callback):
        """
        @brief callback function for state changes

        @parma callback object return from the callback function from the pipeline
        """
        log.info('New state of the pipeline is {}'.format(str(state)))
        self._pipeline_sensor_status.set_value(str(state))

    def start(self):
        super(PafWorkerServer, self).start()

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        self._device_status = Sensor.discrete(
            "device-status",
            "Health status of PafWorkerServer",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status)

        self._pipeline_sensor_name = Sensor.string("pipeline-name",
                                                   "the name of the pipeline", "")
        self.add_sensor(self._pipeline_sensor_name)

        self._pipeline_sensor_status = Sensor.discrete(
            "pipeline-status",
            description="Status of the pipeline",
            params=self.PIPELINE_STATES,
            default="idle",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._pipeline_sensor_status)

    @request(Str())
    @return_reply()
    def request_configure(self, req, config_json):
        """
        @brief      Configure pipeline
        @param      pipeline    name of the pipeline
        """
        @coroutine
        def configure_wrapper():
            try:
                self.configure(config_json)
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(configure_wrapper)
        raise AsyncReply

    @coroutine
    def configure(self, config_json):
        try:
            config_dict = json.loads(config_json)
            pipeline_name = config_dict["mode"]
        except KeyError as error:
            msg = "Error getting the pipeline name from config_json: {}".format(
                str(error))
            log.error(msg)
            raise PafPipelineKeyError(msg)
        self._pipeline_sensor_name.set_value(pipeline_name)
	self._pipeline_sensor_status.set_value("ready")
        log.info("Configuring pipeline {}".format(
            self._pipeline_sensor_name.value()))
        log.info("pipeline instance {} configured".format(
                self._pipeline_sensor_name.value()))

    @request()
    @return_reply(Str())
    def request_start(self, req):
        """
        @brief      Start pipeline

        """
        @coroutine
        def start_pipeline():
            msg = "Start pipeline"
	    self._pipeline_sensor_status.set_value("running")
            log.info("{}".format(msg))
            req.reply("ok", msg)

        if self._pipeline_sensor_status.value() == "ready":
            self.ioloop.add_callback(start_pipeline)
            raise AsyncReply
        else:
            msg = "pipeline is not in the state of configured, status = {} ".format(
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
        def stop_wrapper():
            try:
                yield self.stop_pipeline()
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(stop_wrapper)
        raise AsyncReply

    @coroutine
    def stop_pipeline(self):
        log.info("Stopping pipeline {}".format(
                self._pipeline_sensor_name.value()))
	self._pipeline_sensor_status.set_value("ready")

    @request()
    @return_reply(Str())
    def request_deconfigure(self, req):
        """
        @brief      Deconfigure pipeline
        """
        @coroutine
        def deconfigure_wrapper():
            try:
                yield self.deconfigure()
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(deconfigure_wrapper)
        raise AsyncReply

    @coroutine
    def deconfigure(self):
        log.info("Deconfiguring pipeline {}".format(
            self._pipeline_sensor_name.value()))
	self._pipeline_sensor_status.set_value("idle")

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
                      help='Host interface to bind to', default="127.0.0.1")
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='Port number to bind to', default=5000)
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
