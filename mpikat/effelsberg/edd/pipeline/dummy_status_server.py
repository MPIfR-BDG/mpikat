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


log = logging.getLogger(
    "mpikat.effelsberg.edd.pipeline.pipeline.dummy_status_server")

#PIPELINES = {"mock":mock.Mock()}

# P8_IP="10.17.8.2"


class EddPipelineKeyError(Exception):
    pass


class EddPipelineError(Exception):
    pass


class EddDummyStatusServer(AsyncDeviceServer):
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
        super(EddDummyStatusServer, self).__init__(ip, port)
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
        super(EddDummyStatusServer, self).start()

    @coroutine
    def stop(self):
        """Stop PafWorkerServer server"""
        # if self._pipeline_sensor_status.value() == "ready":
        #    log.info("Pipeline still running, stopping pipeline")
       # yield self.deconfigure()
        yield super(EddDummyStatusServer, self).stop()

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

        self._observing = Sensor.string(
            "observing",
            description="_observing",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._observing)
        self._source = Sensor.string(
            "source",
            description="source",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._source)
        self._ra = Sensor.string(
            "ra",
            description="ra",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ra)
        self._dec = Sensor.string(
            "dec",
            description="dec",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._dec)

    @coroutine
    @request(Str(), Str(), Str(), Str())
    @return_reply()
    def request_set_source(self, req, observing, source_name, ra, dec):
        """Add two numbers"""
        self._source.set_value(str(ra))
        self._source.set_value(str(dec))
        self._source.set_value(str(source_name))
        self._observing.set_value(str(observing))
        req.reply("ok")
        raise AsyncReply


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
                      help='Port number to bind to', default=5001)
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
    server = EddDummyStatusServer(opts.host, opts.port)
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
