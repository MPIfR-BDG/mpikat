"""
Copyright (c) 2019 Jason Wu <jwu@mpifr-bonn.mpg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import logging
import tornado
import coloredlogs
import signal
import json
import time
import astropy.units as units
from astropy.time import Time
from optparse import OptionParser
from tornado.gen import coroutine
from katcp import AsyncDeviceServer, Message, Sensor, AsyncReply
from katcp.kattypes import request, return_reply, Str
from mpikat.effelsberg.edd.pipeline.pipeline import PIPELINES


log = logging.getLogger("mpikat.effelsberg.edd.pipeline.edd_worker_server")

#PIPELINES = {"mock":mock.Mock()}

# P8_IP="10.17.8.2"


class EddPipelineKeyError(Exception):
    pass


class EddPipelineError(Exception):
    pass


class EddWorkerServer(AsyncDeviceServer):
    """
    @brief Interface object which accepts KATCP commands

    """
    VERSION_INFO = ("mpikat-edd-api", 1, 0)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")
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
        super(EddWorkerServer, self).__init__(ip, port)
        self.ip = ip
        self._managed_sensors = []

    def add_pipeline_sensors(self):
        """
        @brief Add pipeline sensors to the managed sensors list

        """
        for sensor in self._pipeline_instance.sensors:
            log.debug("sensor name is {}".format(sensor))
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
        super(EddWorkerServer, self).start()

    @coroutine
    def stop(self):
        """Stop PafWorkerServer server"""
        #if self._pipeline_sensor_status.value() == "ready":
        #    log.info("Pipeline still running, stopping pipeline")
       #yield self.deconfigure()
        yield super(EddWorkerServer, self).stop()

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
        @param      config_json     A dictionary containing configuration information.
                                    The dictionary should have a form similar to:
                                    @code
                                    {
                                    "mode":"DspsrPipelineSrxdev",
                                    "mc_source":"239.2.1.154",
                                    "central_freq":"1400.4"
                                    }
                                    @endcode
        """
        @coroutine
        def configure_wrapper():
            try:
                yield self.configure_pipeline(config_json)
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(configure_wrapper)
        raise AsyncReply

    @coroutine
    def configure_pipeline(self, config_json):
        try:
            config_dict = json.loads(config_json)
            pipeline_name = config_dict["mode"]
        except KeyError as error:
            msg = "Error getting the pipeline name from config_json: {}".format(str(error))
            log.error(msg)
            raise EddPipelineKeyError(msg)
        self._pipeline_sensor_name.set_value(pipeline_name)
        log.info("Configuring pipeline {}".format(
            self._pipeline_sensor_name.value()))
        try:
            _pipeline_type = PIPELINES[self._pipeline_sensor_name.value()]
        except KeyError as error:
            msg = "No pipeline called '{}', available pipeline are: \n{}".format(
                self._pipeline_sensor_name.value(), "\n".join(PIPELINES.keys()))
            self._pipeline_sensor_name.set_value("")
            log.err(msg)
            raise EddPipelineKeyError(msg)
        try:
            log.debug(
                "Trying to create pipeline instance: {}".format(pipeline_name))
            self._pipeline_instance = _pipeline_type()
            self.add_pipeline_sensors()
            self._pipeline_instance.callbacks.add(self.state_change)
            self.capture_start_time = Time.now()
            self.capture_start_time.format = 'isot'
            self.capture_start_time = self.capture_start_time + 27.0 * units.s
            config_dict['capture_start_time'] = self.capture_start_time.value
            config_dict['interger'] = 12
            config_json = json.dumps(config_dict)
            log.debug("Unpacked config: {}".format(json.loads(config_json)))
            self._pipeline_instance.configure(config_json)
        except Exception as error:
            self._pipeline_sensor_name.set_value("")
            msg = "Couldn't start configure pipeline instance {}".format(
                str(error))
            log.error(msg)
            raise EddPipelineError(msg)
        else:
            log.info("Pipeline instance {} configured".format(
                self._pipeline_sensor_name.value()))

    @request(Str())
    @return_reply(Str())
    def request_start(self, req, config_json):
        """
        @brief      Start pipeline
        @param      config_json     A dictionary containing configuration information.
                                    The dictionary should have a form similar to:
                                    @code
                                    {
                                    "source-name":"J1022+1001",
                                    "ra":"123.4",
                                    "dec":"-20.1"
                                    }
                                    @endcode
        """
        @coroutine
        def start_wrapper():
            try:
                yield self.start_pipeline(config_json)
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(start_wrapper)
        raise AsyncReply

    @coroutine
    def start_pipeline(self, config_json):
        try:
            config = json.loads(config_json)
            source_name = config["source-name"]
            ra = config["ra"]
            dec = config["dec"]
            log.debug("Unpacked config: {}".format(config))
            log.info("staring pipeline for {} at {}, {}".format(source_name, ra, dec))
            self._pipeline_instance.start(config_json)
        except Exception as error:
            msg = "Couldn't start pipeline server {}".format(str(error))
            log.error(msg)
            raise EddPipelineError(msg)
        else:
            log.info("Starting pipeline {}".format(
                self._pipeline_sensor_name.value()))

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
        try:
            self.remove_pipeline_sensors()
            self._pipeline_instance.deconfigure()
            #del self._pipeline_instance
        except Exception as error:
            msg = "Couldn't deconfigure pipeline {}".format(str(error))
            log.error(msg)
            raise EddPipelineError(msg)
        else:
            log.info("Deconfigured pipeline {}".format(
                self._pipeline_sensor_name.value()))
            self._pipeline_sensor_name.set_value("")

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
        try:
            self._pipeline_instance.stop()
        except Exception as error:
            msg = "Couldn't stop pipeline {}".format(str(error))
            log.error(msg)
            raise EddPipelineError(msg)
        else:
            log.info("Stopping pipeline {}".format(
                self._pipeline_sensor_name.value()))

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
    if server._pipeline_sensor_status.value() == "running":
        log.info("Pipeline still running, stopping pipeline")
        yield server.stop_pipeline()
        time.sleep(10)
    if server._pipeline_sensor_status.value() != "idle":
        log.info("Pipeline still configured, deconfiguring pipeline")
        yield server.deconfigure()
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
    server = EddWorkerServer(opts.host, opts.port)
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
