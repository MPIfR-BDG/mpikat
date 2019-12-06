"""
Copyright (c) 2019 Tobias Winchen <twinchen@mpifr-bonn.mpg.de>

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
from mpikat.utils.process_tools import ManagedProcess, command_watcher
from mpikat.utils.process_monitor import SubprocessMonitor
from mpikat.utils.sensor_watchdog import SensorWatchdog
from mpikat.utils.db_monitor import DbMonitor
from mpikat.utils.mkrecv_stdout_parser import MkrecvSensors
from mpikat.effelsberg.edd.pipeline.pipeline_register import register_pipeline
import mpikat.utils.numa as numa

from katcp import Sensor, AsyncDeviceServer, AsyncReply, FailReply
from katcp.kattypes import request, return_reply, Int, Str

import tornado
from tornado.gen import coroutine

import os
import datetime
import logging
import signal
from argparse import ArgumentParser
import coloredlogs
import json
import tempfile
import threading

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.EDDPipeline")
log.setLevel('DEBUG')




class EDDPipeline(AsyncDeviceServer):
    """@brief Abstract interface for EDD Pipelines

    Pipelines are required to implement:
            * configure
            * capture_start
            * capture_stop
            * deconfigure
    After configure, the current configuration needs to be stored in self._config for reconfigure to work.
    """
    DEVICE_STATUSES = ["ok", "degraded", "fail"]


    PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "error"]

    def __init__(self, ip, port):
        """@brief initialize the pipeline."""
        self.callbacks = set()
        self._state = "idle"
        self._sensors = []
        self._config = None
        self._subprocesses = []
        self._subprocessMonitor = None
        AsyncDeviceServer.__init__(self, ip, port) 


    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        self._pipeline_sensor_status = Sensor.discrete(
            "pipeline-status",
            description="Status of the pipeline",
            params=self.PIPELINE_STATES,
            default="idle",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._pipeline_sensor_status)

        self._device_status = Sensor.discrete(
            "device-status",
            description="Health status of device",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status)

        self._status_change_time = Sensor.string(
            "status-change-time",
            description="Time of last status change",
            default=datetime.datetime.now().replace(microsecond=0).isoformat(),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._status_change_time)


    @property
    def sensors(self):
        return self._sensors


    def notify(self):
        """@brief callback function."""
        for callback in self.callbacks:
            callback(self._state, self)


    @property
    def state(self):
        """@brief property of the pipeline state."""
        return self._state


    @state.setter
    def state(self, value):
        self._state = value
        self._pipeline_sensor_status.set_value(self._state)
        self._status_change_time.set_value(datetime.datetime.now().replace(microsecond=0).isoformat())
        self.notify()


    def start(self):
        """
        @brief    Start the server
        """
        AsyncDeviceServer.start(self)


    def stop(self):
        """
        @brief    Stop the server
        """
        AsyncDeviceServer.stop(self)


    def _decode_capture_stdout(self, stdout, callback):
        log.debug('{}'.format(str(stdout)))


    def _handle_execution_stderr(self, stderr, callback):
        log.info(stderr)


    def _subprocess_error(self, proc):
        """
        Sets the error state because proc has ended.
        """
        log.error("Errror handle called because subprocess {} ended with return code {}".format(proc.pid, proc.returncode))
        self._subprocessMonitor.stop()
        self.state =  "error"


    @request(Str())
    @return_reply()
    def request_configure(self, req, config_json):
        """
        @brief      Configure EDD to receive and process data

       @note        ToDo:  Device a method to add the sublcass doc string here! 

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """

        @coroutine
        def configure_wrapper():
            try:
                yield self.configure(config_json)
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(configure_wrapper)
        raise AsyncReply


    @coroutine
    def configure(self, config_json=""):
        """@brief Configure the pipeline"""
        raise NotImplementedError


    @request()
    @return_reply()
    def request_reconfigure(self, req):
        """
        @brief      Configure the EDD using the last configuration.

        @return     katcp reply object [[[ !reconfigure ok | (fail [error description]) ]]]
        """

        @coroutine
        def reconfigure_wrapper():
            try:
                yield self.configure(self._config)
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(reconfigure_wrapper)
        raise AsyncReply


    @request()
    @return_reply()
    def request_reconfigure(self, req):
        """
        @brief      Configure the pipeline using the last configuration.

        @return     katcp reply object [[[ !reconfigure ok | (fail [error description]) ]]]
        """

        @coroutine
        def reconfigure_wrapper():
            try:
                yield self.configure(self._config)
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(reconfigure_wrapper)
        raise AsyncReply


    @request()
    @return_reply()
    def request_capture_start(self, req):
        """
        @brief      Start the EDD backend processing

        @note       This method may be updated in future to pass a 'scan configuration' containing
                    source and position information necessary for the population of output file
                    headers.

        @note       This is the KATCP wrapper for the capture_start command

        @return     katcp reply object [[[ !capture_start ok | (fail [error description]) ]]]
        """

        @coroutine
        def start_wrapper():
            try:
                yield self.capture_start()
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(start_wrapper)
        raise AsyncReply


    @coroutine
    def request_halt(self, req, msg):
        """
        Halts the process. Reimplemnetation of base class halt without timeout as this crash
        """
        if self.state == "running":
            yield self.capture_stop()
        yield self.deconfigure()
        self.ioloop.stop()
        req.reply("Server has stopepd - ByeBye!")
        raise AsyncReply


    def watchdog_error(self):
        """
        @brief Set error mode requested by watchdog.
        """
        log.error("Error state requested by watchdog!")
        self.state = "error"


    @coroutine
    def capture_start(self):
        """@brief start the pipeline."""
        raise NotImplementedError


    @request()
    @return_reply()
    def request_capture_stop(self, req):
        """
        @brief      Stop the EDD backend processing

        @note       This is the KATCP wrapper for the capture_stop command

        @return     katcp reply object [[[ !capture_stop ok | (fail [error description]) ]]]
        """

        @coroutine
        def stop_wrapper():
            try:
                yield self.capture_stop()
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(stop_wrapper)
        raise AsyncReply


    @coroutine
    def capture_stop(self):
        """@brief stop the pipeline."""
        raise NotImplementedError


    @request()
    @return_reply()
    def request_deconfigure(self, req):
        """
        @brief      Deconfigure the pipeline.

        @note       This is the KATCP wrapper for the deconfigure command

        @return     katcp reply object [[[ !deconfigure ok | (fail [error description]) ]]]
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
        """@brief deconfigure the pipeline."""
        raise NotImplementedError




@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


def getArgumentParser():
    parser = ArgumentParser()
    parser.add_argument('-H', '--host', dest='host', type=str, default='localhost',
                      help='Host interface to bind to')
    parser.add_argument('-p', '--port', dest='port', type=int, default=1235,
                      help='Port number to bind to')
    parser.add_argument('--log-level', dest='log_level', type=str,
                      help='Port number of status server instance', default="INFO")

    return parser


def launchPipelineServer(Pipeline):
    parser = getArgumentParser()
    args = parser.parse_args()

    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    logger.setLevel(args.log_level.upper())

    log.setLevel(args.log_level.upper())
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
        level=args.log_level.upper(),
        logger=logger)
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting Pipeline instance")
    server = Pipeline(
        args.host, args.port
        )
    log.info("Created Pipeline instance")
    signal.signal(
        signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, server))

    def start_and_display():
        log.info("Starting Pipeline server")
        server.start()
        log.debug("Started Pipeline server")
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(
                server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()
