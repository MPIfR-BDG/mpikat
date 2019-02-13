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
import coloredlogs
import json
import tornado
import signal
from optparse import OptionParser
from tornado.gen import coroutine
from katcp import Sensor, AsyncReply
from katcp.kattypes import request, return_reply, Str
from mpikat.core.master_controller import MasterController
from mpikat.effelsberg.status_server import JsonStatusServer
from mpikat.effelsberg.paf.paf_product_controller import PafProductController
from mpikat.effelsberg.paf.paf_worker_wrapper import PafWorkerPool
from mpikat.effelsberg.paf.paf_scpi_interface import PafScpiInterface

# ?halt message means shutdown everything and power off all machines
# beamfile observer8:/home/obseff/paf_test/Scripts


log = logging.getLogger("mpikat.paf_master_controller")

PAF_PRODUCT_ID = "paf0"
SCPI_BASE_ID = "PAFBE"
PAF_REQUIRED_KEYS = ["nbeams", "nbands", "band_offset",
                     "mode", "frequency", "write_filterbank"]


class PafConfigurationError(Exception):
    pass


class PafMasterController(MasterController):
    """This is the main KATCP interface for the PAF
    pulsar searching system on MeerKAT. This controller only
    holds responsibility for capture of data from the CBF
    network and writing of that data to disk.

    This interface satisfies the following ICDs:
    CAM-PAF: <link>
    """
    VERSION_INFO = ("mpikat-paf-api", 0, 1)
    BUILD_INFO = ("mpikat-paf-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]

    CONTROL_MODES = ["KATCP", "SCPI"]
    KATCP, SCPI = CONTROL_MODES

    def __init__(self, ip, port, scpi_ip, scpi_port):
        """
        @brief       Construct new PafMasterController instance

        @params  ip       The IP address on which the server should listen
        @params  port     The port that the server should bind to
        """
        super(PafMasterController, self).__init__(ip, port, PafWorkerPool())
        self._control_mode = self.KATCP
        self._scpi_ip = scpi_ip
        self._scpi_port = scpi_port
        self._status_server = JsonStatusServer(ip, 0)

    def start(self):
        super(PafMasterController, self).start()
        self._status_server.start()
        address = self._status_server.bind_address
        log.info("Status server started at {}".format(address))
        self._status_server_sensor.set_value(address)
        self._scpi_interface = PafScpiInterface(
            self, self._scpi_ip, self._scpi_port, self.ioloop)

    def stop(self):
        self._scpi_interface.stop()
        self._scpi_interface = None
        self._status_server.stop()
        super(PafMasterController, self).stop()

    def setup_sensors(self):
        super(PafMasterController, self).setup_sensors()
        self._paf_config_sensor = Sensor.string(
            "current-config",
            description="The currently set configuration for the PAF backend",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._paf_config_sensor)

        self._status_server_sensor = Sensor.address(
            "status-server-address",
            description="The address of the status server",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._status_server_sensor)

    @property
    def katcp_control_mode(self):
        return self._control_mode == self.KATCP

    @property
    def scpi_control_mode(self):
        return self._control_mode == self.SCPI

    @request(Str())
    @return_reply()
    def request_set_control_mode(self, req, mode):
        """ Weeeee """
        mode = mode.upper()
        if not mode in self.CONTROL_MODES:
            return ("fail", "Unknown mode '{}', valid modes are '{}' ".format(
                mode, ", ".join(self.CONTROL_MODES)))
        else:
            self._control_mode = mode
        if self._control_mode == self.SCPI:
            self._scpi_interface.start()
        else:
            self._scpi_interface.stop()
        return ("ok",)

    @request(Str())
    @return_reply()
    def request_configure(self, req, config_json):
        """
        @brief      Configure PAF to receive and process data

        @param      req           A katcp request object
        @param      config_json   A JSON object containing configuration
                                  information.

        @note  The JSON configuration object should be of the form:
               @code
               {
                   "mode": "Search1Beam",
                   "nbands": 48,
                   "frequency": 1340.5,
                   "nbeams": 18,
                   "band_offset": 0,
                   "write_filterbank": 0
               }
               @endcode

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(
                self._control_mode))

        @coroutine
        def configure_wrapper():
            try:
                yield self.configure(config_json)
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(configure_wrapper)
        raise AsyncReply

    @coroutine
    def configure(self, config_json):
        log.info("Configuring PAF processing")
        log.debug("Configuration string: '{}'".format(config_json))
        if self._products:
            log.error("PAF already has a configured data product")
            raise PafConfigurationError(
                "PAF already has a configured data product")
        config_dict = json.loads(config_json)
        for key in PAF_REQUIRED_KEYS:
            if key not in config_dict:
                message = "No value set for required configuration parameter '{}'".format(
                    key)
                log.error(message)
                raise PafConfigurationError(message)
        self._products[PAF_PRODUCT_ID] = PafProductController(
            self, PAF_PRODUCT_ID)
        self._paf_config_sensor.set_value(config_json)
        self._update_products_sensor()
        try:
            yield self._products[PAF_PRODUCT_ID].configure(config_json)
        except Exception as error:
            log.error(
                "Failed to configure product with error: {}".format(str(error)))
            raise PafConfigurationError(str(error))
        else:
            log.debug(
                "Configured PAF instance with ID: {}".format(PAF_PRODUCT_ID))

    @request()
    @return_reply()
    def request_deconfigure(self, req):
        """
        @brief      Deconfigure the PAF instance.

        @param      req               A katcp request object

        @return     katcp reply object [[[ !deconfigure ok | (fail [error description]) ]]]
        """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(
                self._control_mode))

        @coroutine
        def deconfigure_wrapper():
            try:
                yield self.deconfigure()
            except Exception as error:
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(deconfigure_wrapper)
        raise AsyncReply

    @coroutine
    def deconfigure(self):
        log.info("Deconfiguring PAF processing")
        product = self._get_product(PAF_PRODUCT_ID)
        yield product.deconfigure()
        del self._products[PAF_PRODUCT_ID]
        self._update_products_sensor()

    @request()
    @return_reply()
    def request_capture_start(self, req):
        """ arse """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(
                self._control_mode))

        @coroutine
        def start_wrapper():
            try:
                yield self.capture_start()
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(start_wrapper)
        raise AsyncReply

    @coroutine
    def capture_start(self):
        status_json = self._status_server.as_json()
        log.info("Telescope status at capture start:\n{}".format(
                json.loads(status_json)))
        product = self._get_product(PAF_PRODUCT_ID)
        yield product.capture_start(status_json)

    @request()
    @return_reply()
    def request_capture_stop(self, req):
        """
        @brief      Stop PAF streaming

        @param      PAF_PRODUCT_ID  This is a name for the data product, used to track which subarray is being deconfigured.
                                    For example "array_1_bc856M4k".
        """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(
                self._control_mode))

        @coroutine
        def stop_wrapper():
            try:
                yield self.capture_stop()
            except Exception as error:
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(stop_wrapper)
        raise AsyncReply

    @coroutine
    def capture_stop(self):
        product = self._get_product(PAF_PRODUCT_ID)
        yield product.capture_stop()


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
    parser.add_option('', '--scpi-interface', dest='scpi_interface', type=str,
                      help='The interface to listen on for SCPI requests',
                      default="")
    parser.add_option('', '--scpi-port', dest='scpi_port', type=int,
                      help='The port number to listen on for SCPI requests')
    parser.add_option('', '--log-level', dest='log_level', type=str,
                      help='Port number of status server instance', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting PafMasterController instance")
    server = PafMasterController(
        opts.host, opts.port, opts.scpi_interface, opts.scpi_port)
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
