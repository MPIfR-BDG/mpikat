"""
Copyright (c) 2018 Ewan Barr <ebarr@mpifr-bonn.mpg.de>

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
import json
import signal
import coloredlogs
from optparse import OptionParser
from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop
from katcp import Sensor, AsyncDeviceServer, Message
from katcp.kattypes import request, return_reply, Str
from mpikat.core.utils import LoggingSensor
from mpikat.meerkat.apsuse.apsuse_capture import ApsCapture


log = logging.getLogger("mpikat.apsuse_worker_server")


class ApsWorkerServer(AsyncDeviceServer):
    VERSION_INFO = ("aps-worker-server-api", 0, 1)
    BUILD_INFO = ("aps-worker-server-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    STATES = ["idle", "starting", "capturing", "recording", "stopping", "error"]
    IDLE, STARTING, CAPTURING, RECORDING, STOPPING, ERROR = STATES

    def __init__(self, ip, port, capture_interface):
        """
        @brief       Construct new ApsWorkerServer instance

        @params  ip       The interface address on which the server should listen
        @params  port     The port that the server should bind to
        """
        self._dada_input_key = "dada"
        self._capture_interface = capture_interface
        self._capture_instances = []
        super(ApsWorkerServer, self).__init__(ip, port)

    @coroutine
    def start(self):
        """Start ApsWorkerServer server"""
        super(ApsWorkerServer, self).start()

    @coroutine
    def stop(self):
        yield super(ApsWorkerServer, self).stop()

    def setup_sensors(self):
        """
        @brief    Set up monitoring sensors.

        @note     The following sensors are made available on top of default
                  sensors implemented in AsynDeviceServer and its base classes.
        """
        self._device_status_sensor = Sensor.discrete(
            "device-status",
            description="Health status of ApsWorkerServer instance",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._device_status_sensor)

        self._capture_interface_sensor = Sensor.string(
            "capture-interface",
            description="The IP address of the NIC to be used for data capture",
            default=self._capture_interface,
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._capture_interface_sensor)

        self._state_sensor = LoggingSensor.discrete(
            "state",
            params=self.STATES,
            description="The current state of this worker instance",
            default=self.IDLE,
            initial_status=Sensor.NOMINAL)
        self._state_sensor.set_logger(log)
        self.add_sensor(self._state_sensor)

    @property
    def capturing(self):
        return self.state == self.CAPTURING

    @property
    def idle(self):
        return self.state == self.IDLE

    @property
    def starting(self):
        return self.state == self.STARTING

    @property
    def stopping(self):
        return self.state == self.STOPPING

    @property
    def recording(self):
        return self.state == self.RECORDING

    @property
    def error(self):
        return self.state == self.ERROR

    @property
    def state(self):
        return self._state_sensor.value()

    @request(Str())
    @return_reply()
    @coroutine
    def request_configure(self, req, config_json):
        """
        @brief      Prepare APSUSE to receive and process data from FBFUSE

        @detail     REQUEST ?configure config_json
                    Configure APSUSE for the particular data products

        @param      req                 A katcp request object

        @param      config_json         A JSON object containing configuration information
                                        for coherent and/or incoherent beams to be captured by
                                        the instance.

        @note       The JSON config takes the form:

                    @code
                    {"coherent-beams":
                        {"bandwidth": 856000000.0,
                         "beam-ids": ["cfbf00012",
                                      "cfbf00013",
                                      "cfbf00014",
                                      "cfbf00015",
                                      "cfbf00016",
                                      "cfbf00017"],
                         "centre-frequency": 1284000000.0,
                         "filesize": 10000000000.0,
                         "base_output_dir": "/output/blah",
                         "heap-size": 8192,
                         "idx1-step": 268435456,
                         "mcast-groups": ["239.11.1.0"],
                         "mcast-port": 7147,
                         "nchans": 4096,
                         "nchans-per-heap": 16,
                         "sample-clock": 1712000000.0,
                         "sampling-interval": 0.000306,
                         "stream-indices": [12, 13, 14, 15, 16, 17],
                         "sync-epoch": 1232352352.0},
                     "incoherent-beams":
                         {"bandwidth": 856000000.0,
                          "beam-ids": ["ifbf00000"],
                          "centre-frequency": 1284000000.0,
                          "filesize": 10000000000.0,
                          "base_output_dir": "/output/blah"
                          "heap-size": 8192,
                          "idx1-step": 268435456,
                          "mcast-groups": ["239.11.1.1"],
                          "mcast-port": 7147,
                          "nchans": 4096,
                          "nchans-per-heap": 16,
                          "sample-clock": 1712000000.0,
                          "sampling-interval": 0.000306,
                          "stream-indices": [0],
                          "sync-epoch": 1232352352.0}}
                    @endcode

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        if not self.idle:
            raise Return(("fail", "APS worker not in IDLE state"))
        log.info("Preparing worker server instance")
        try:
            config = json.loads(config_json)
        except Exception as error:
            msg = ("Unable to parse config with "
                   "error: {}").format(str(error))
            log.error("Prepare failed: {}".format(msg))
            raise Return(("fail", msg))
        log.info("Config: {}".format(config))

        self._state_sensor.set_value(self.STARTING)
        if "coherent-beams" in config:
            coherent_cap = ApsCapture(
                self._capture_interface,
                "/tmp/apsuse_capture_coherent.sock",
                "/tmp/coherent_mkrecv.cfg",
                "0-2", "2-3",
                "coherent", "dada")
            for sensor in coherent_cap._sensors:
                self.add_sensor(sensor)
            yield coherent_cap.capture_start(config["coherent-beams"])
            self._capture_instances.append(coherent_cap)
        if "incoherent-beams" in config:
            incoherent_cap = ApsCapture(
                self._capture_interface,
                "/tmp/apsuse_capture_incoherent.sock",
                "/tmp/incoherent_mkrecv.cfg",
                "3", "4",
                "incoherent", "caca")
            for sensor in incoherent_cap._sensors:
                self.add_sensor(sensor)
            yield incoherent_cap.capture_start(config["incoherent-beams"])
            self._capture_instances.append(incoherent_cap)
        self.mass_inform(Message.inform('interface-changed'))
        self._state_sensor.set_value(self.CAPTURING)
        raise Return(("ok",))

    @request()
    @return_reply()
    @coroutine
    def request_deconfigure(self, req):
        """
        @brief      Deconfigure the apsuse worker

        @param      req                 A katcp request object

        @return     katcp reply object [[[ !deconfigure ok | (fail [error description]) ]]]
        """
        self._state_sensor.set_value(self.STOPPING)
        futures = []
        for capture_instance in self._capture_instances:
            capture_instance.target_stop()
            futures.append(capture_instance.capture_stop())
            for sensor in capture_instance._sensors:
                self.remove_sensor(sensor)
        self.mass_inform(Message.inform('interface-changed'))
        for future in futures:
            try:
                yield future
            except Exception as error:
                log.exception("Error during capture_stop")
                raise Return(("fail", str(error)))
        self._state_sensor.set_value(self.IDLE)
        raise Return(("ok",))

    @request()
    @return_reply()
    def request_target_start(self, req, beam_info):
        """
        @brief      Request that the worker server starts recording

        @param      req                 A katcp request object

        @param      beam_info           A JSON object containing beam information

        @note       The 'beam_info' JSON takes the form:

                    @code
                    [ {"id": "cfbf00000",
                      "target": "source0,radec,00:00:00.00,00:00:00"},
                      {"id": "cfbf00001",
                      "target": "source0,radec,00:00:00.00,00:00:00"},
                      {"id": "cfbf00002",
                      "target": "source0,radec,00:00:00.00,00:00:00"},
                      {"id": "cfbf00003",
                      "target": "source0,radec,00:00:00.00,00:00:00"},
                      {"id": "cfbf000004",
                      "target": "source0,radec,00:00:00.00,00:00:00"},
                      {"id": "cfbf00005",
                      "target": "source0,radec,00:00:00.00,00:00:00"},
                      {"id": "cfbf00006",
                      "target": "source0,radec,00:00:00.00,00:00:00"},
                      {"id": "cfbf00007",
                      "target": "source0,radec,00:00:00.00,00:00:00"},]
                    @endcode

        @return     katcp reply object [[[ !target-start ok | (fail [error description]) ]]]
        """
        if self.state != self.CAPTURING:
            return ("fail", "Worker not in 'capturing' state")
        for capture_instance in self._capture_instances:
            capture_instance.target_start(beam_info)
        self._state_sensor.set_value(self.RECORDING)
        return ("ok",)

    @request()
    @return_reply()
    def request_target_stop(self, req):
        """
        @brief      Request that the worker server stops recording

        @param      req                 A katcp request object

        @return     katcp reply object [[[ !target-stop ok | (fail [error description]) ]]]
        """
        if self.state not in self.RECORDING:
            return ("fail", "Worker not in 'capturing' or 'recording' state")
        for capture_instance in self._capture_instances:
            capture_instance.target_stop()
        self._state_sensor.set_value(self.CAPTURING)
        return ("ok",)


@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='Host interface to bind to')
    parser.add_option('-p', '--port', dest='port', type=int,
                      help='Port number to bind to')
    parser.add_option('-c', '--capture-ip', dest='cap_ip', type=str,
                      help='The interface to use for data capture')
    parser.add_option('-d', '--base-output-dir', dest='outdir', type=str,
                      help='The base output directory for storing recorded data')
    parser.add_option('', '--log-level', dest='log_level', type=str,
                      help='Port number of status server instance',
                      default="INFO")
    (opts, args) = parser.parse_args()
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s -"
             "%(filename)s:%(lineno)s] %(message)s"),
        level=opts.log_level.upper(),
        logger=logger)
    logger.setLevel(opts.log_level.upper())
    logging.getLogger('katcp').setLevel(logging.ERROR)
    ioloop = IOLoop.current()
    log.info("Starting ApsWorkerServer instance")
    server = ApsWorkerServer(opts.host, opts.port, opts.cap_ip, opts.outdir)
    signal.signal(
        signal.SIGINT,
        lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(
                server.bind_address))
    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
