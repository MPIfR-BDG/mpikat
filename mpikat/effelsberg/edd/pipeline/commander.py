import logging
import tempfile
import json
import os
import time
from astropy.time import Time
from subprocess import PIPE, Popen
from mpikat.effelsberg.edd.edd_digpack_client import DigitiserPacketiserClient
from mpikat.effelsberg.edd.pipeline.dada import render_dada_header, make_dada_key_string
import shlex
import threading
import base64
import tornado
import coloredlogs
import signal
import astropy.units as units
from optparse import OptionParser
from tornado.gen import coroutine
#from katcp import AsyncDeviceServer, Message, AsyncReply
from katcp import AsyncDeviceServer, Message, Sensor, AsyncReply, KATCPClientResource
from katcp.kattypes import request, return_reply, Str
import codecs
import re
import sys
import katcp
import logging
from katcp import BlockingClient, Message


log = logging.getLogger("mpikat.effelsberg.edd.pipeline")
log.setLevel('DEBUG')

ESCAPE_SEQUENCE_RE = re.compile(r'''
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )''', re.UNICODE | re.VERBOSE)


class KATCPToIGUIConverter(object):

    def __init__(self, host, port):
        """
        @brief      Class for katcp to igui converter.

        @param   host             KATCP host address
        @param   port             KATCP port number
        """
        self.rc = KATCPClientResource(dict(
            name="test-client",
            address=(host, port),
            controlled=True))
        self.host = host
        self.port = port
        self.ioloop = None
        self.ic = None
        self.api_version = None
        self.implementation_version = None
        self.previous_sensors = set()
        self.sensor_callbacks = set()
        self.new_sensor_callbacks = set()
        self._sensor = []

    def sensor_notify(self):
        for callback in self.sensor_callbacks:
            callback(self._sensor, self)

    @property
    def sensor(self):
        return self._sensor

    @sensor.setter
    def sensor(self, value):
        self._sensor = value
        self.sensor_notify()

    def new_sensor_notify(self):
        for callback in self.new_sensor_callbacks:
            callback(self._new_sensor, self)

    @property
    def new_sensor(self):
        return self._new_sensor

    @new_sensor.setter
    def new_sensor(self, value):
        self._new_sensor = value
        self.new_sensor_notify()

    def start(self):
        """
        @brief      Start the instance running

        @detail     This call will trigger connection of the KATCPResource client and
                    will login to the iGUI server. Once both connections are established
                    the instance will retrieve a mapping of the iGUI receivers, devices
                    and tasks and will try to identify the parent of the device_id
                    provided in the constructor.

        @param      self  The object

        @return     { description_of_the_return_value }
        """
        @tornado.gen.coroutine
        def _start():
            log.debug("Waiting on synchronisation with server")
            yield self.rc.until_synced()
            log.debug("Client synced")
            log.debug("Requesting version info")
            response = yield self.rc.req.version_list()
            log.info("response {}".format(response))
            self.ioloop.add_callback(self.update)
        log.debug("Starting {} instance".format(self.__class__.__name__))
        self.rc.start()
        self.ic = self.rc._inspecting_client
        self.ioloop = self.rc.ioloop
        self.ic.katcp_client.hook_inform("interface-changed",
                                         lambda message: self.ioloop.add_callback(self.update))
        self.ioloop.add_callback(_start)

    @tornado.gen.coroutine
    def update(self):
        """
        @brief    Synchronise with the KATCP servers sensors and register new listners
        """
        log.debug("Waiting on synchronisation with server")
        yield self.rc.until_synced()
        log.debug("Client synced")
        current_sensors = set(self.rc.sensor.keys())
        log.debug("Current sensor set: {}".format(current_sensors))
        removed = self.previous_sensors.difference(current_sensors)
        log.debug("Sensors removed since last update: {}".format(removed))
        added = current_sensors.difference(self.previous_sensors)
        log.debug("Sensors added since last update: {}".format(added))
        for name in list(added):
            # for name in ["source_name", "observing", "timestamp"]:
            # if name == 'observing':
            #log.debug("Setting sampling strategy and callbacks on sensor '{}'".format(name))
            # strat3 = ('event-rate', 2.0, 3.0)              #event-rate doesn't work
            # self.rc.set_sampling_strategy(name, strat3)    #KATCPSensorError:
            # Error setting strategy
            # not sure that auto means here
            self.rc.set_sampling_strategy(name, "auto")
            #self.rc.set_sampling_strategy(name, ["period", (1)])
        #self.rc.set_sampling_strategy(name, "event")
            self.rc.set_sensor_listener(name, self._sensor_updated)
            self.new_sensor = name
            #log.debug("Setting new sensor with name = {}".format(name))
        self.previous_sensors = current_sensors

    def _sensor_updated(self, sensor, reading):
        """
        @brief      Callback to be executed on a sensor being updated

        @param      sensor   The sensor
        @param      reading  The sensor reading
        """

        # log.debug("Recieved sensor update for sensor '{}': {}".format(
        #    sensor.name, repr(reading)))
        self.sensor = sensor.name, sensor.value
        #log.debug("Value of {} sensor {}".format(sensor.name, sensor.value))

    def stop(self):
        """
        @brief      Stop the client
        """
        self.rc.stop()


class BlockingRequest(BlockingClient):

    def __init__(self, host, port):
        device_host = host
        device_port = port
        super(BlockingRequest, self).__init__(device_host, device_port)

    def __del__(self):
        super(BlockingRequest, self).stop()
        super(BlockingRequest, self).join()

    def unescape_string(self, s):
        def decode_match(match):
            return codecs.decode(match.group(0), 'unicode-escape')
        return ESCAPE_SEQUENCE_RE.sub(decode_match, s)

    def decode_katcp_message(self, s):
        """
        @brief      Render a katcp message human readable

    @params s   A string katcp message
        """
        return self.unescape_string(s).replace("\_", " ")

    def to_stream(self, reply, informs):
        log.info(self.decode_katcp_message(reply.__str__()))
        for msg in informs:
            log.info(self.decode_katcp_message(msg.__str__()))

    def start(self):
        self.setDaemon(True)
        super(BlockingRequest, self).start()
        self.wait_protocol()

    def stop(self):
        super(BlockingRequest, self).stop()
        super(BlockingRequest, self).join()

    def help(self):
        reply, informs = self.blocking_request(
            katcp.Message.request("help"), timeout=20)
        self.to_stream(reply, informs)

    def configure(self, paras, sensors):
        reply, informs = self.blocking_request(
            katcp.Message.request("configure", paras, sensors))
        self.to_stream(reply, informs)

    def deconfigure(self):
        reply, informs = self.blocking_request(
            katcp.Message.request("deconfigure"))
        self.to_stream(reply, informs)

    def capture_start(self, source_name):
        reply, informs = self.blocking_request(
            katcp.Message.request("start", source_name, 1024, 1024))
        self.to_stream(reply, informs)

    def capture_stop(self):
        reply, informs = self.blocking_request(katcp.Message.request("stop"))
        self.to_stream(reply, informs)


class EddCommander(AsyncDeviceServer):
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
        @brief Initialization of the EddCommander object

        @param ip       IP address of the server
        @param port     port of the EddCommander

        """
        super(EddCommander, self).__init__(ip, port)
        # self.setup_sensors()
        self.ip = ip
        self._managed_sensors = []
        self.callbacks = set()
        self._state = "idle"
        self._volumes = ["/tmp/:/scratch/"]
        self._dada_key = None
        self._config = None
        self._source_config = None
        self._dspsr = None
        self._mkrecv_ingest_proc = None
        self._status_server = KATCPToIGUIConverter("134.104.70.66", 5001)
        self._status_server.start()
        self._status_server.sensor_callbacks.add(
            self.sensor_update)
        self._status_server.new_sensor_callbacks.add(
            self.new_sensor)
        self._bc = BlockingRequest("134.104.70.66", 5000)
        self.first_flag = True
        # self.start_working()

    def sensor_update(self, sensor_value, callback):
        #log.debug('Settting sensor value for EDD_pipeline sensor : {} with value {}'.format(sensor_value[0],sensor_value[1]))
        self.test_object = self.get_sensor(sensor_value[0].replace("-", "_"))
        #log.debug("{} {}".format(
        #    sensor_value[0].replace("-", "_"), sensor_value[1]))
        self.test_object.set_value(str(sensor_value[1]))
        self._observing = self.get_sensor("_observing")
        self._source = self.get_sensor("_source")
        log.debug("Value for _observing {}".format(self._observing.value()))
        log.debug(bool(self._observing.value() == 'True' & self.first_flag == True))
        if self._observing.value() == 'True' & self.first_flag == True:
            log.debug("observing sensor value is {}".format(
                self._observing.value()))
            log.debug("Should send a start command to the pipeline")
            self.first_flag = False

            # log.debug(")
        elif self._observing.value() == 'False':
            log.debug("observing sensor value is {}".format(
                self._observing.value()))
            log.debug("Should send a stop to the pipeline")
            self.first_flag = True

    def new_sensor(self, sensor_name, callback):
        #log.debug('New sensor reporting = {}'.format(str(sensor_name)))
        self.add_pipeline_sensors(sensor_name)

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
        self.notify()

    def add_pipeline_sensors(self, sensor):
        """
        @brief Add pipeline sensors to the managed sensors list

        """
        # for sensor in self._pipeline_instance.sensors:
        #log.debug("sensor name is {}".format(sensor))
        self.add_sensor(Sensor.string("{}".format(sensor), description="{}".format(sensor),
                                      default="N/A", initial_status=Sensor.UNKNOWN))
        # self.add_sensor(sensor)
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
        super(EddCommander, self).start()

    @coroutine
    def stop(self):
        """Stop PafWorkerServer server"""
        # if self._pipeline_sensor_status.value() == "ready":
        #    log.info("Pipeline still running, stopping pipeline")
        # yield self.deconfigure()
        yield super(EddCommander, self).stop()

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

    @property
    def sensors(self):
        return self._sensors

    def _decode_capture_stdout(self, stdout, callback):
        log.debug('{}'.format(str(stdout)))

    def _error_treatment(self, callback):
        pass
        # log.debug('reconfigureing')
        # self.reconfigure()

    def _save_capture_stdout(self, stdout, callback):
        with open("{}.par".format(self._source_config["source-name"]), "a") as file:
            file.write('{}\n'.format(str(stdout)))

    def _handle_execution_returncode(self, returncode, callback):
        log.debug(returncode)

    def _handle_execution_stderr(self, stderr, callback):
        if bool(stderr[:8] == "Finished") & bool("." not in stderr):
            self._time_processed.set_value(stderr)
            log.info(stderr)
        if bool(stderr[:8] != "Finished"):
            log.info(stderr)

    def _handle_eddpolnmerge_stderr(self, stderr, callback):
        log.debug(stderr)

    def _add_tscrunch_to_sensor(self, png_blob, callback):
        self._tscrunch.set_value(png_blob)

    def _add_fscrunch_to_sensor(self, png_blob, callback):
        self._fscrunch.set_value(png_blob)

    def _add_profile_to_sensor(self, png_blob, callback):
        self._profile.set_value(png_blob)

"""
    @coroutine
    @request()
    @return_reply()
    def request_start_working(self, req):


        #ereq.reply("ok")
        #raise AsyncReply
        @coroutine
        def start_wrapper():
            try:
                while True:
    #        # if observing == TRUE and observing_started == FALSE:
                # grab source name and send start command
            #    self._source.get_value
                    print("checking sensor value")
                    time.sleep(10)
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
                #self._pipeline_sensor_status.set_value("ready")
        self.ioloop.add_callback(start_wrapper)
        raise AsyncReply

"""


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
    # parser.add_option('-H', '--target-host', dest='target-host', type=str,
    #                  help='Host interface to bind to', default="127.0.0.1")
    # parser.add_option('-p', '--target-port', dest='target-port', type=long,
    #                  help='Port number to bind to', default=5000)
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting EddCommander instance")
    server = EddCommander(opts.host, opts.port)
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
