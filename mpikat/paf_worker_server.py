import tornado
import logging
import signal
import json
from optparse import OptionParser
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, request, return_reply)
from paf_pipeline_server import PIPELINES #whatever the pipeline class will be

PIPELINE = {}

class PafWorkerServer(AsyncDeviceServer):
    """
    @brief Interface object which accepts KATCP commands

    """
    VERSION_INFO = ("example-api", 1, 0)
    BUILD_INFO = ("example-implementation", 0, 1, "")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    PIPELINE_STATUSES = ["idle", "configuring", "configured", "starting", "running", "stopping", "deconfiguring", "error"]


    # Optionally set the KATCP protocol version and features. Defaults to
    # the latest implemented version of KATCP, with all supported optional
    # featuresthat's all of the receivers
    PROTOCOL_INFO = ProtocolFlags(5, 0, set([
        ProtocolFlags.MULTI_CLIENT,
        ProtocolFlags.MESSAGE_IDS,
    ]))


    def __init__(self, ip, port):
        """
        @brief Initialization of the PafWorkerServer object

        @param ip       IP address of the board
        @param port     port of the board

        """
        super(PafWorkerServer, self).__init__(ip, port)
        self.pipeline = None
        self.pipeline_status = "idle"

    def start(self):
       super(PafWorkerServer, self).start()

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        self._device_status = Sensor.discrete(
            "device-status",
            description="Health status of R2RM",
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
            params=self.PIPELINE_STATUSES,
            default="idle",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._pipeline_sensor_status)      

    @request(Str(), Str(), Str())
    @return_reply()
    def configure(self, pipeline, bw , something):
        """
        @brief      Configure pipeline

        @param      pipeline    name of the pipeline
        @param      bw          bandwidth per se
        @param      something   more parameters
        """
        @tornado.gen.coroutine
        def configure_pipeline():
            self.pipeline = pipeline
            log.info("Configuring pipeline {}".format(self.pipeline))
            self.pipeline_status = "configuring"
            self._pipeline_sensor_status.set_value("configuring")
            try:
                _pipeline_type = PIPELINES[self.pipeline]
            except KeyError as error:
                log.error("No pipeline called '{}', available pipeline are: \n{}".format(self.pipeline, "\n".join(PIPELINES.keys())))
                req.reply("fail", "Error on pipeline load: {}".format(str(error)))
                self.pipeline_status = "error"
                self._pipeline_sensor_status.set_value("error")
                self.pipeline = None
                raise error
            self._pipeline_instance = _pipeline_type(bw, something)
            try:
                self._pipeline_instance.configure()
            except Exception as error:
                log.error("Couldn't start configure pipeline instance {}".format(error))
                req.reply("fail", "Error on pipeline load: {}".format(str(error)))
                self.pipeline_status = "error"
                self._pipeline_sensor_status.set_value("error")
                self.pipeline = None
                raise error

            self.pipeline_status = "configured"
            self._pipeline_sensor_status.set_value("configured")
            log.info("pipeline instance configured")
            req.reply("ok","pipeline instance configured")

        if self.pipeline_status == "idle":
            self.ioloop.add_callback(configure_pipeline)
            raise AsyncReply
        else:
            msg = "Can't Configure, status = ".format(self.pipeline_status)
            log.info("{}".format(msg))
            return ("fail", msg)
        
    @request()
    @return_reply()
    def start(self, req):
        """
        @brief      Start pipeline

        """
        @tornado.gen.coroutine        
        def start_pipeline():
            self.pipeline_status = "starting"
            self._pipeline_sensor_status.set_value("starting")
            try:
                self._pipeline_instance.start()
            except Exception as error:
                log.error("Couldn't start pipeline server {}".format(error))
                req.reply("fail", "Couldn't start pipeline server {}".format(error))
                self.pipeline_status = "error"
                self._pipeline_sensor_status.set_value("error")
                raise error
            log.info("Start pipeline {}".format(self.pipeline))    
            req.reply("ok", "Start pipeline {}".format(self.pipeline))
            self._pipeline_sensor_status.set_value("running")
            self.pipeline_status = "running"

        if self.pipeline_status == "configured":
            self.ioloop.add_callback(start_pipeline)
            raise AsyncReply
        else :
            req.info("pipeline is not in the state of configured, status = ".format(self.pipeline_status))
            return ("fail", "pipeline is not in the state of configured, status = ".format(self.pipeline_status))
        
        
    @request()
    @return_reply()
    def stop(self, req): 
        """
        @brief      Stop pipeline

        """
        @tornado.gen.coroutine
        def stop_pipeline():
            self.pipeline_status = "stopping"
            self._pipeline_sensor_status.set_value("stopping")
            try:
                self._pipeline_instance.stop()
            except Exception as error:
                log.error("Couldn't stop pipeline {}".format(error))
                req.reply("fail", "Couldn't stop pipeline {}".format(error))
                self.pipeline_status = "error"
                self._pipeline_sensor_status.set_value("error")
                raise error
            log.info("Stop pipeline {}".format(self.pipeline))
            req.reply("ok", "Stop pipeline {}".format(self.pipeline))
            self.pipeline_status = "stopped"
            self._pipeline_sensor_status.set_value("stopped")

        if self.pipeline_status == "running":
            self.ioloop.add_callback(stop_pipeline)
            raise AsyncReply
        else :
            req.info("nothing to stop, status = {}".format(self.pipeline_status))
            return ("fail", "nothing to stop, status = {}".format(self.pipeline_status))


    @request()
    @return_reply()
    def deconfigure(self, req):
        """
        @brief      Deconfigure pipeline

        """
        @tornado.gen.coroutine
        def deconfigure():
            log.info("deconfiguring pipeline {}".format(self.pipeline))
            self.pipeline_status = "deconfiguring"
            self._pipeline_sensor_status.set_value("deconfiguring")
            try:
                self._pipeline_instance.deconfigure()
            except Exception as error:
                log.error("Couldn't deconfigure pipeline {}".format(error))
                req.reply("fail", "Couldn't deconfigure pipeline {}".format(error))
                self.pipeline_status = "error"
                self._pipeline_sensor_status.set_value("error")
                raise error
            log.info("Deconfigured pipeline {}".format(self.pipeline))
            req.reply("ok", "Deconfigured pipeline {}".format(self.pipeline))
            self.pipeline_status = "deconfigured"
            self._pipeline_sensor_status.set_value("deconfigured")
            self.pipeline = None

        if self.pipeline_status == "configured":   
            self.ioloop.add_callback(deconfigure)
            raise AsyncReply
        else:
            req.info("nothing to deconfigure, status = {}".format(self.pipeline_status))
            return ("fail", "nothing to deconfigure, status = {}".format(self.pipeline_status))

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
    logger = logging.getLogger('paf_worker_server')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(opts.log_level.upper())
    ioloop = tornado.ioloop.IOLoop.current()
    server = PafWorkerServer(opts.host, opts.port)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))
    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))
    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()