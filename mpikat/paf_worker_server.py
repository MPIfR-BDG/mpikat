import tornado
import logging
import signal
import json
import igui_sidecar as sidecar
from optparse import OptionParser
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, request, return_reply)
#from firmware_server import FIRMWARES
from paf_pipeline_server import PIPELINES #whatever the pipeline class will be

PIPELINE = {}

class PafWorkerServer(AsyncDeviceServer):
    """
    @brief Interface object which accepts KATCP commands

    """
    VERSION_INFO = ("example-api", 1, 0)
    BUILD_INFO = ("example-implementation", 0, 1, "")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    PIPELINE_STATUSES = ["running", "done", "fail", "not running"]

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
        self.configured = 'N'
        self.pipeline_status = None

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

        self._pipeline_name = Sensor.string("pipeline-name",
            "the name of the pipeline", "")
        self.add_sensor(self._pipeline_name)

        self._pipeline_status = Sensor.discrete(
            "pipeline-status",
            description="Status of the pipeline",
            params=self.PIPELINE_STATUSES,
            default="not running",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status)      

    @request(Str(), Str(), Str())
    @return_reply()
    def configure(self, pipeline, bw , something):
        """
        @brief      Configure firmware for a user

        @param      pipeline    name of the pipeline
        @param      bw          bandwidth per se
        @param      something   more parameters
        """
        @tornado.gen.coroutine
        def configure_pipeline():
            self.pipeline = pipeline
            log.info("Configuring pipeline {}".format(self.pipeline))
            try:
                _pipeline_type = FIRMWARES[self.pipeline]
            except KeyError as error:
                log.error("No pipeline called '{}', available pipeline are: \n{}".format(self.pipeline, "\n".join(PIPELINES.keys())))
                req.reply("fail", "Error on pipeline load: {}".format(str(error)))
                raise error
            self._pipeline_instance = _firmware_type('parameters to be pass')
            try:
                self._pipeline_instance.configure()
            except Exception as error:
                log.error("Couldn't start configure pipeline instance {}".format(error))
                req.reply("fail", "Error on pipeline load: {}".format(str(error)))
                raise error
            log.info("pipeline instance configured")
            req.reply("ok","pipeline instance configured")   
        if self.pipeline == None:
            self.ioloop.add_callback(configure_pipeline)
            raise AsyncReply
        else:
            msg = "No, there is already one pipline configured/started/stop"
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
            try:
                self._pipeline_instance.start()
            except Exception as error:
                log.error("Couldn't start pipeline server {}".format(error))
                req.reply("Couldn't start pipeline server {}".format(error))
                raise error
            log.info("Start pipeline server {}".format(self.pipeline))    
            req.reply("Start pipeline server {}".format(self.pipeline))
        self.ioloop.add_callback(start_pipeline)
        raise AsyncReply
        
    @request()
    @return_reply()
    def stop(self, req): 
        """
        @brief      Stop pipeline

        """
        @tornado.gen.coroutine
        def stop_pipeline():
            try:
                self._pipeline_instance.stop()
            except Exception as error:
                log.error("Couldn't stop pipeline {}".format(error))
                req.reply("Couldn't stop pipeline {}".format(error))
                raise error
            log.info("Stop pipeline {}".format(self.pipeline))
            req.reply("Stop pipeline {}".format(self.pipeline))
            self.configured = 'N'
        self.ioloop.add_callback(stop_pipeline)
        raise AsyncReply

    @request()
    @return_reply()
    def deconfigure(self, req):
        """
        @brief      Deconfigure pipeline

        """
        @tornado.gen.coroutine
        def deconfigure():
            log.info("Deconfiguring")
            try:
                self._pipeline_instance.deconfigure()
            except Exception as error:
                log.error("Couldn't deconfigure pipeline {}".format(error))
                req.reply("Couldn't deconfigure pipeline {}".format(error))
                raise error
            log.info("Stop pipeline {}".format(self.pipeline))
            req.reply("Stop pipeline {}".format(self.pipeline))
            self.pipeline = None   
        self.ioloop.add_callback(deconfigure)
        raise AsyncReply

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