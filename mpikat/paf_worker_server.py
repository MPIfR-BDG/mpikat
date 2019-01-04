import tornado
import coloredlogs
import logging
import signal
import json
import mock
import os
from optparse import OptionParser
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import (Str, request, return_reply)
from pipeline import PIPELINES
#from paf_pipeline_server import PIPELINES #whatever the pipeline class will be

log = logging.getLogger("mpikat.paf_worker_server")

#PIPELINES = {"mock":mock.Mock()}


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
        #self._pipeline_sensor_status.set_value(str(callback.state))

        #do I do things here?
        """
        if callback.state=="error":
            @tornado.gen.coroutine
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
            params=self.PIPELINE_STATUSES,
            default="idle",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._pipeline_sensor_status) 

        self._ip_address = Sensor.string("ip",
            description="the ip address of the node controller", 
            default=os.environ['PAF_DATA_INTERFACE'])
        self.add_sensor(self._ip_address)

    @request(Str(),Str(),Str())
    @return_reply(Str())
    def request_configure(self, req, pipeline_name, utc_start, freq):
        """
        @brief      Configure pipeline

        @param      pipeline    name of the pipeline
        """
        @tornado.gen.coroutine
        def configure_pipeline():
            self._pipeline_sensor_name.set_value(pipeline_name)  
            log.info("Configuring pipeline {}".format(self._pipeline_sensor_name.value()))
            self._pipeline_sensor_status.set_value("configuring")
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
                self._pipeline_instance.configure(utc_start, freq, self._ip_address)
            except Exception as error:
                msg = "Couldn't start configure pipeline instance {}".format(str(error))
                log.info("{}".format(msg))
                req.reply("fail", msg)
                self._pipeline_sensor_status.set_value("error")
                self._pipeline_sensor_name.set_value("")
                raise error
            self._pipeline_sensor_status.set_value("configured")
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
        
    @request()
    @return_reply(Str())
    def request_start_capture(self, req):
        """
        @brief      Start pipeline

        """
        @tornado.gen.coroutine        
        def start_pipeline():
            self._pipeline_sensor_status.set_value("starting")
            try:
                self._pipeline_instance.start()
            except Exception as error:
                msg = "Couldn't start pipeline server {}".format(error)
                log.info("{}".format(msg))
                req.reply("fail", msg)
                self._pipeline_sensor_status.set_value("error")
                raise error
            msg = "Start pipeline {}".format(self._pipeline_sensor_name.value())
            log.info("{}".format(msg))    
            req.reply("ok", msg)
            self._pipeline_sensor_status.set_value("running")

        if self._pipeline_sensor_status.value() == "configured":
            self.ioloop.add_callback(start_pipeline)
            raise AsyncReply
        else:
            msg = "pipeline is not in the state of configured, status = {} ".format(self._pipeline_sensor_status.value())
            log.info("{}".format(msg))
            return ("fail", msg)
        
        
    @request()
    @return_reply(Str())
    def request_stop_capture(self, req): 
        """
        @brief      Stop pipeline

        """
        @tornado.gen.coroutine
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

            self._pipeline_sensor_status.set_value("configured")

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
        @tornado.gen.coroutine
        def deconfigure():
            log.info("deconfiguring pipeline {}".format(self._pipeline_sensor_name.value()))
            self._pipeline_sensor_status.set_value("deconfiguring")
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
            self._pipeline_sensor_status.set_value("idle")
            self._pipeline_sensor_name.set_value("")
        if self._pipeline_sensor_status.value() == "configured":   
            self.ioloop.add_callback(deconfigure)
            raise AsyncReply
        else:
            msg = "nothing to deconfigure, status = {}".format(self._pipeline_sensor_status.value())
            log.info("{}".format(msg))
            return ("fail", msg)

@tornado.gen.coroutine
def on_shutdown(ioloop, server):
    log.info('Shutting down server')
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