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
import types

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.EDDPipeline")
log.setLevel('DEBUG')


def updateConfig(oldo, new):
    """
    @breif Merge retrieved config [new] into [old] via recursive dict merge
    """
    old = oldo.copy()
    for k in new:
        if isinstance(old[k], dict):
            log.debug("update sub dict for key: {}".format(k))
            old[k] = updateConfig(old[k], new[k])
        else:
            if type(old[k]) != type(new[k]):
                log.warning("Update option {} with different type! Old value(type) {}({}), new {}({}) ".format(k, old[k], type(old[k]), new[k], type(new[k])))
            old[k] = new[k]
    return old


class EDDPipeline(AsyncDeviceServer):
    """
    @brief Abstract interface for EDD Pipelines

    @detail Pipelines can implement functions to act within the following sequence of commands:

            * ?set "partial config"
            * ?set "partial config"
            * ?set "partial config"
            * ?configure "partial config"
            * ?capture_start
            * ?measurement_prepare "data"
            * ?measurement_start
            * ?measurement_stop
            * ?measurement_prepare "data"
            * ?measurement_start
            * ?measurement_stop
            * ?measurement_prepare "data"
            * ?measurement_start
            * ?measurement_stop
            * ?capture_stop
            * ?deconfigure

    * set - updates the curent configuration with the provided partial config. This
            is handeld enterily within the parent class which updates the member
            attribute _config.
    * configure - optionally does a final update of the curernt config and
                  prepares the pipeline. Configuring the pipeline may take time, so all
                  lengthy preparations should be done here.
    * capture start - The pipeline should send data (into the EDD) after this command.
    * measurement prepare - receive optional configuration before each measurement. The pipeline must not stop streaming on update.
    * measurement start - Start of an individual measuerment. Should be quasi
                          isntantaneous. E.g. a recorder should be already connected to the dat
                          stream and just start writing to disk.
    * measurement stop -  Stop the measurement

    Pipelines can also implement:
        * populate_data_store to send data to the store. The address and port for a data store is received along the request.

    """
    DEVICE_STATUSES = ["ok", "degraded", "fail"]


    PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "error"]

    def __init__(self, ip, port, default_config={}):
        """
        @brief Initialize the pipeline. Subclasses are required to provide their default config dict.
        """
        self.callbacks = set()
        self._state = "idle"
        self._sensors = []
        self.__config = default_config.copy()
        self._default_config = default_config
        self._subprocesses = []
        self._subprocessMonitor = None
        AsyncDeviceServer.__init__(self, ip, port)

    @property
    def _config(self):
        """
        @brief The current configuration of the pipeline, i.e. the default
        after all updates received via set and configure commands. This value
        should then be used in the _configure method.
        """
        return self.__config

    @_config.setter
    def _config(self, value):
        if not isinstance(value, dict):
            raise RuntimeError("_config has to be a dict!")
        self.__config = value
        self._edd_config_sensor.set_value(json.dumps(self._config, indent=4))


    def setup_sensors(self):
        """
        @brief Setup monitoring sensors.

        @detail The EDDPipeline base provides default sensors. Should be called by a subclass.

        """
        self._pipeline_sensor_status = Sensor.discrete(
            "pipeline-status",
            description="Status of the pipeline",
            params=self.PIPELINE_STATES,
            default="idle",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._pipeline_sensor_status)

        self._edd_config_sensor = Sensor.string(
            "current-config",
            description="The current configuration for the EDD backend",
            default=json.dumps(self._default_config, indent=4),
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._edd_config_sensor)

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
        """
        @ToDo: Potentially obsolete ??
        """
        log.debug('{}'.format(str(stdout)))


    def _handle_execution_stderr(self, stderr, callback):
        """
        @ToDo: Potentially obsolete ??
        """
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

        @note       ToDo:  Device a method to add the sublcass doc string here!

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
        pass


    @request()
    @return_reply()
    def request_set_default_config(self, req):
        """
        @brief      Set the current config to the default config

        @return     katcp reply object [[[ !reconfigure ok | (fail [error description]) ]]]
        """

        logging.info("Setting default configuration")
        self._config = self._default_config.copy()
        req.reply("ok")
        raise AsyncReply


    @request(Str())
    @return_reply()
    def request_set(self, req, config_json):
        """
        @brief      Add the config_json to the current config

        @note       ToDo:  Device a method to add the sublcass doc string here!

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """

        try:
            self.set(config_json)
        except FailReply as fr:
            log.error(str(fr))
            req.reply("fail", str(fr))
        except Exception as error:
            log.exception(str(error))
            req.reply("fail", str(error))
        else:
            req.reply("ok")
        raise AsyncReply




    @coroutine
    def set(self, config_json):
        """
        @brief      Add the config_json to the current config

        @note       ToDo:  Device a method to add the sublcass doc string here!

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """

        log.debug("Updating configuration: '{}'".format(config_json))
        if isinstance(config_json, str):
            log.debug("Received config as string")
            try:
                cfg = json.loads(config_json)
            except:
                log.error("Error parsing json")
                raise FailReply("Cannot handle config string {} - Not valid json!".format(config_json))
        elif isinstance(config_json, dict):
            log.debug("Received config as dict")
            cfg = config_json
        else:
            raise FailReply("Cannot handle config type {}. Config has to bei either json formatted string or dict!".format(type(config_json)))
        try:
            self._config = updateConfig(self.__config, cfg)
            log.debug("Updated config: '{}'".format(self._config))
        except KeyError as error:
            raise FailReply("Unknown configuration option: {}".format(str(error)))
        except Exception as error:
            raise FailReply("Unknown ERROR: {}".format(str(error)))



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
        pass


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
        pass


    @request(Str())
    @return_reply()
    def request_measurement_prepare(self, req, config_json):
        """
        @brief      Prepare measurement request

        @note        ToDo:  Device a method to add the sublcass doc string here!

        @return     katcp reply object [[[ !measurement_prepare ok | (fail [error description]) ]]]
        """

        @coroutine
        def wrapper():
            try:
                yield self.measurement_prepare(config_json)
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)
        raise AsyncReply


    @coroutine
    def measurement_prepare(self, config_json=""):
        """@brief prepare the measurement"""
        pass



    @request()
    @return_reply()
    def request_measurement_start(self, req):
        """
        @brief      Start

        @note       This is the KATCP wrapper for the measurement_start command

        @return     katcp reply object [[[ !measurement_start ok | (fail [error description]) ]]]
        """

        @coroutine
        def wrapper():
            try:
                yield self.measurement_start()
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)
        raise AsyncReply


    @coroutine
    def measurement_start(self):
        """@brief start the emasurement."""
        pass


    @request()
    @return_reply()
    def request_measurement_stop(self, req):
        """
        @brief      Start

        @note       This is the KATCP wrapper for the measurement_stop command

        @return     katcp reply object [[[ !measurement_start ok | (fail [error description]) ]]]
        """

        @coroutine
        def wrapper():
            try:
                yield self.measurement_stop()
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(wrapper)
        raise AsyncReply


    @coroutine
    def measurement_stop(self):
        """@brief stop the emasurement."""
        pass

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
        pass

    @request(Str(), Int())
    @return_reply()
    def request_populate_data_store(self, req, host, port):
        """
        @brief Populate the data store with opipeline specific informations, as e.g. data stream format

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """

        @coroutine
        def populate_data_store_wrapper():
            try:
                yield self.populate_data_store(host, port)
            except FailReply as fr:
                log.error(str(fr))
                req.reply("fail", str(fr))
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(populate_data_store_wrapper)
        raise AsyncReply


    @coroutine
    def populate_data_store(self, host, port):
        """@brief Populate the data store"""
        log.debug("Populate data store @ {}:{}".format(host, port))
        pass




@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


def getArgumentParser():
    """
    @brief Provide a arguemnt parser with standard arguments for all pipelines.
    """
    parser = ArgumentParser()
    parser.add_argument('-H', '--host', dest='host', type=str, default='localhost',
                      help='Host interface to bind to')
    parser.add_argument('-p', '--port', dest='port', type=int, default=1235,
                      help='Port number to bind to')
    parser.add_argument('--log-level', dest='log_level', type=str,
                      help='Port number of status server instance', default="INFO")

    return parser


def launchPipelineServer(Pipeline, args=None):
    """
    @brief Launch a Pipeline server.

    @param Pipeline Instance or ServerClass to launch.
    @param ArgumentParser args to use for launch.
    """
    if not args:
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

    if (type(Pipeline) == types.ClassType) or isinstance(Pipeline, type):
        log.info("Created Pipeline instance")
        server = Pipeline(
            args.host, args.port
            )
    else:
        server = Pipeline

    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting Pipeline instance")
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
