import logging
import tornado
import coloredlogs
import signal
import json
import os
import time
from optparse import OptionParser
from tornado.gen import Return, coroutine
#import mock
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str, Discrete, Float
from pipeline import PIPELINES
from tornado.iostream import IOStream
#from mpikat.master_controller import MasterController
#from mpikat.paf_product_controller import PafProductController
#from mpikat.paf_worker_wrapper import PafWorkerPool
#from mpikat.exceptions import ProductLookupError


log = logging.getLogger("mpikat.paf_worker_server")

#PIPELINES = {"mock":mock.Mock()}

P8_IP="10.17.8.2"

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
    PROTOCOL_INFO = ProtocolFlags(5, 0, set([
        ProtocolFlags.MULTI_CLIENT,
        ProtocolFlags.MESSAGE_IDS,
    ]))


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

        #do I do things here?
        """
        if callback.state=="error":
            @coroutine
            def stop_pipeline():
                try:
                    self._pipeline_instance.stop()
                except Exception as error:
                    msg = "Couldn't stop pipeline {}".format(error)
                    log.info("{}".format(msg))
                    req.reply("fail", msg)
                    self._pipeline_sensor_status.set_value("error")
                    raise error
                msg = "Stop pipeline {}".format(self._pipeline_sensor_name.value())
                log.info("{}".format(msg))
                req.reply("ok", msg)
            self.ioloop.add_callback(configure_pipeline)
            raise AsyncReply
        """
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

        self._ip_address = Sensor.string("ip",
            description="the ip address of the node controller", 
            default=os.environ['PAF_DATA_INTERFACE'])
        self.add_sensor(self._ip_address)

    @request(Str(),Str(),Str(),Str())
    @return_reply(Str())
    def request_configure(self, req, pipeline_name, utc_start, freq, ip):
        """
        @brief      Configure pipeline

        @param      pipeline    name of the pipeline
        """
        @coroutine
        def configure_pipeline():
            self._pipeline_sensor_name.set_value(pipeline_name)  
            log.info("Configuring pipeline {}".format(self._pipeline_sensor_name.value()))
            try:
                _pipeline_type = PIPELINES[self._pipeline_sensor_name.value()]
            except KeyError as error:
                msg = "No pipeline called '{}', available pipeline are: \n{}".format(self._pipeline_sensor_name.value(), "\n".join(PIPELINES.keys()))
                log.info("{}".format(msg))
                req.reply("fail", msg)
                self._pipeline_sensor_status.set_value("error")
                self._pipeline_sensor_name.set_value("")
                raise error
            self._pipeline_instance = _pipeline_type()
            self._pipeline_instance.callbacks.add(self.state_change)
            try:
                self._pipeline_instance.configure(utc_start, freq, P8_IP)
            except Exception as error:
                msg = "Couldn't start configure pipeline instance {}".format(str(error))
                log.info("{}".format(msg))
                req.reply("fail", msg)
                self._pipeline_sensor_status.set_value("error")
                self._pipeline_sensor_name.set_value("")
                raise error
            msg = "pipeline instance configured"
            log.info("{}".format(msg))
            req.reply("ok",msg)
        if self._pipeline_sensor_status.value() == "idle":
            self.ioloop.add_callback(configure_pipeline)
            raise AsyncReply
        else:
            msg = "Can't Configure, status = {}".format(self._pipeline_sensor_status.value())
            log.info("{}".format(msg))
            return ("fail", msg)
        
    @request(Str(),Str(),Str(),Str())
    @return_reply(Str())
    def request_start(self, req, source_name, ra, dec, start_buf):
        """
        @brief      Start pipeline

        """
        @coroutine       
        def start_pipeline():
            try:
                self._pipeline_instance.start(source_name, ra, dec, start_buf)
            except Exception as error:
                msg = "Couldn't start pipeline server {}".format(error)
                log.info("{}".format(msg))
                req.reply("fail", msg)
                self._pipeline_sensor_status.set_value("error")
                raise error
            msg = "Start pipeline {}".format(self._pipeline_sensor_name.value())
            log.info("{}".format(msg))    
            req.reply("ok", msg)

        if self._pipeline_sensor_status.value() == "ready":
            self.ioloop.add_callback(start_pipeline)
            raise AsyncReply
        else:
            msg = "pipeline is not in the state of configured, status = {} ".format(self._pipeline_sensor_status.value())
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
                self._pipeline_sensor_status.set_value("error")
                raise error
            msg = "Stop pipeline {}".format(self._pipeline_sensor_name.value())
            log.info("{}".format(msg))
            req.reply("ok", msg)

        if self._pipeline_sensor_status.value() == "running":
            self.ioloop.add_callback(stop_pipeline)
            raise AsyncReply
        else :
            msg = "nothing to stop, status = {}".format(self._pipeline_sensor_status.value())
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
            log.info("deconfiguring pipeline {}".format(self._pipeline_sensor_name.value()))
            try:
                self._pipeline_instance.deconfigure()
            except Exception as error:
                msg = "Couldn't deconfigure pipeline {}".format(error)
                log.info("{}".format(msg))
                req.reply("fail", msg)
                self._pipeline_sensor_status.set_value("error")
                raise error
            msg = "deconfigured pipeline {}".format(self._pipeline_sensor_name.value())
            log.info("{}".format(msg))
            req.reply("ok", msg)
            self._pipeline_sensor_name.set_value("")
        if self._pipeline_sensor_status.value() == "ready":   
            self.ioloop.add_callback(deconfigure)
            raise AsyncReply
        else:
            msg = "nothing to deconfigure, status = {}".format(self._pipeline_sensor_status.value())
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
    parser.add_option('', '--log_level',dest='log_level',type=str,
        help='logging level',default="INFO")
    (opts, args) = parser.parse_args()
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
        log.info("Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()