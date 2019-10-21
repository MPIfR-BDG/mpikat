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
from tornado.gen import Return, coroutine
from katcp import Sensor, AsyncReply
from katcp.kattypes import request, return_reply, Str
from mpikat.core.master_controller import MasterController
#from mpikat.effelsberg.edd.edd_roach2_product_controller import (
#    EddRoach2ProductController)
from mpikat.effelsberg.edd.edd_product_controller import EddProductController
from mpikat.effelsberg.edd.edd_worker_wrapper import EddWorkerPool
from mpikat.effelsberg.edd.edd_scpi_interface import EddScpiInterface
from mpikat.effelsberg.edd.edd_digpack_client import DigitiserPacketiserClient
from mpikat.effelsberg.edd.edd_fi_client import EddFitsInterfaceClient
from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController 

log = logging.getLogger("mpikat.edd_master_controller")
EDD_REQUIRED_KEYS = []


class EddConfigurationError(Exception):
    pass


class UnknownControlMode(Exception):
    pass


class EddMasterController(MasterController):
    """
    The main KATCP interface for the EDD backend
    """
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]

    CONTROL_MODES = ["KATCP", "SCPI"]
    KATCP, SCPI = CONTROL_MODES

    def __init__(self, ip, port, scpi_ip, scpi_port, r2rm_host, r2rm_port):
        """
        @brief       Construct new EddMasterController instance

        @params  ip       The IP address on which the server should listen
        @params  port     The port that the server should bind to
        """
        self._control_mode = self.KATCP
        self._scpi_ip = scpi_ip
        self._scpi_port = scpi_port
        self._r2rm_host = r2rm_host
        self._r2rm_port = r2rm_port
        self._scpi_interface = None
        self._packetisers = []
        self._fits_interfaces = []
        super(EddMasterController, self).__init__(ip, port, EddWorkerPool())

    def start(self):
        """
        @brief    Start the server
        """
        super(EddMasterController, self).start()
        self._scpi_interface = EddScpiInterface(
            self, self._scpi_ip, self._scpi_port, self.ioloop)

    def stop(self):
        """
        @brief    Stop the server
        """
        self._scpi_interface.stop()
        self._scpi_interface = None
        super(EddMasterController, self).stop()

    def setup_sensors(self):
        """
        @brief    Set up all sensors on the server

        @note     This is an internal method and is invoked by
                  the constructor of the base class.
        """
        super(EddMasterController, self).setup_sensors()
        self._control_mode_sensor = Sensor.string(
            "control-mode",
            description="The control mode for the EDD",
            default=self._control_mode,
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._control_mode_sensor)
        self._edd_config_sensor = Sensor.string(
            "current-config",
            description="The current configuration for the EDD backend",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._edd_config_sensor)
        self._edd_scpi_interface_addr_sensor = Sensor.string(
            "scpi-interface-addr",
            description="The SCPI interface address for this instance",
            default="{}:{}".format(self._scpi_ip, self._scpi_port),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._edd_scpi_interface_addr_sensor)

    @property
    def katcp_control_mode(self):
        return self._control_mode == self.KATCP

    @property
    def scpi_control_mode(self):
        return self._control_mode == self.SCPI

    @request(Str())
    @return_reply()
    def request_set_control_mode(self, req, mode):
        """
        @brief     Set the external control mode for the master controller

        @param     mode   The external control mode to be used by the server
                          (options: KATCP, SCPI)

        @detail    The EddMasterController supports two methods of external control:
                   KATCP and SCPI. The server will always respond to a subset of KATCP
                   commands, however when set to SCPI mode the following commands are
                   disabled to the KATCP interface:
                       - configure
                       - capture_start
                       - capture_stop
                       - deconfigure
                   In SCPI control mode the EddScpiInterface is activated and the server
                   will respond to SCPI requests.

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        try:
            self.set_control_mode(mode)
        except Exception as error:
            return ("fail", str(error))
        else:
            return ("ok",)

    def set_control_mode(self, mode):
        """
        @brief     Set the external control mode for the master controller

        @param     mode   The external control mode to be used by the server
                          (options: KATCP, SCPI)
        """
        mode = mode.upper()
        if mode not in self.CONTROL_MODES:
            raise UnknownControlMode("Unknown mode '{}', valid modes are '{}' ".format(
                mode, ", ".join(self.CONTROL_MODES)))
        else:
            self._control_mode = mode
        if self._control_mode == self.SCPI:
            self._scpi_interface.start()
        else:
            self._scpi_interface.stop()
        self._control_mode_sensor.set_value(self._control_mode)

    @request(Str())
    @return_reply()
    def request_configure(self, req, config_json):
        """
        @brief      Configure EDD to receive and process data

        @note       This is the KATCP wrapper for the configure command

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(self._control_mode))

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
    def _packetiser_config_helper(self, config):
        try:
            client = DigitiserPacketiserClient(*config["address"])
        except Exception as error:
            log.error(
                "Error while connecting to packetiser: {}".format(str(error)))
            raise error
        try:
            yield client.set_sampling_rate(config["sampling_rate"])
            yield client.set_predecimation(config["predecimation_factor"])
            yield client.flip_spectrum(config["flip_spectrum"])
            yield client.set_bit_width(config["bit_width"])
            yield client.set_destinations(config["v_destinations"], config["h_destinations"])
            for interface, ip_address in config["interface_addresses"].items():
                yield client.set_interface_address(interface, ip_address)
            yield client.synchronize()
            yield client.capture_start()
        except Exception as error:
            log.error(
                "Error during packetiser configuration: {}".format(str(error)))
            raise error
        else:
            raise Return(client)
        finally:
            client.stop()

    @coroutine
    def configure(self, config_json):
        """
        @brief   Configure the EDD backend

        @param   config_json    A JSON dictionary object containing configuration information

        @detail  The configuration dictionary is highly flexible. An example is below:
                 @code
                     {
                         "packetisers":
                         [
                             {
                                 "id": "digitiser_packetiser_01",
                                 "address": ["134.104.73.132", 7147],
                                 "sampling_rate": 2600000000.0,
                                 "bit_width": 12,
                                 "v_destinations": "225.0.0.152+3:7148",
                                 "h_destinations": "225.0.0.156+3:7148"
                             }
                         ],
                         "products":
                         [
                             {
                                 "id": "roach2_spectrometer",
                                 "type": "roach2",
                                 "icom_id": "R2-E01",
                                 "firmware": "EDDFirmware",
                                 "commands":
                                 [
                                     ["program", []],
                                     ["start", []],
                                     ["set_integration_period", [1000.0]],
                                     ["set_destination_address", ["10.10.1.12", 60001]]
                                 ]
                             }
                         ],
                         "fits_interfaces":
                         [
                             {
                                 "id": "fits_interface_01",
                                 "name": "FitsInterface",
                                 "address": ["134.104.73.132", 6000],
                                 "nbeams": 1,
                                 "nchans": 2048,
                                 "integration_time": 1.0,
                                 "blank_phases": 1
                             }
                         ]
                     }
                 @endcode
        """
        log.info("Configuring EDD backend for processing")
        log.debug("Configuration string: '{}'".format(config_json))
        if self._products:
            log.warning(
                "EDD already has configured data products, attempting deconfigure")
            try:
                yield self.deconfigure()
            except Exception as error:
                log.warning(
                    "Unable to deconfigure EDD before configuration: {}".format(str(error)))
        self._packetisers = []
        self._fits_interfaces = []
        log.debug("Parsing JSON configuration")
        try:
            config_dict = json.loads(config_json)
        except Exception as error:
            log.error("Unable to parse configuration dictionary")
            raise error

        log.info("Configuring digitisers/packetisers")
        dp_configure_futures = []
        for dp_config in config_dict["packetisers"]:
            dp_configure_futures.append(
                self._packetiser_config_helper(dp_config))
        for future in dp_configure_futures:
            dp_client = yield future
            self._packetisers.append(dp_client)

        log.info("Configuring products")
        product_configure_futures = []
        for product_config in config_dict["products"]:
            product_id = product_config["id"]
            if product_config["type"] == "roach2":
                self._products[product_id] = EddRoach2ProductController(self, product_id,
                                                                        (self._r2rm_host, self._r2rm_port))
            elif product_config["type"] == "server":
                self._products[product_id] = EddServerProductController(self, product_id, product_config["address"])
            else:
                raise NotImplementedError(
                    "Only roach2 products are currently supported")
            future = self._products[product_id].configure(product_config)
            product_configure_futures.append(future)
        for future in product_configure_futures:
            yield future

        log.info("Configuring FITS interfaces")
        for fi_config in config_dict["fits_interfaces"]:
            fi = EddFitsInterfaceClient(fi_config["id"], fi_config["address"])
            yield fi.configure(fi_config)
            self._fits_interfaces.append(fi)
        self._edd_config_sensor.set_value(config_json)
        self._update_products_sensor()
        log.info("Successfully configured EDD")

    @request()
    @return_reply()
    def request_deconfigure(self, req):
        """
        @brief      Deconfigure the EDD backend.

        @note       This is the KATCP wrapper for the deconfigure command

        @return     katcp reply object [[[ !deconfigure ok | (fail [error description]) ]]]
        """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(self._control_mode))

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
        """
        @brief      Deconfigure the EDD backend.
        """
        log.info("Deconfiguring all products")
        for product in self._products.values():
            yield product.deconfigure()
        log.info("Stopping packetiser data transmission")
        for packetiser in self._packetisers:
            yield packetiser.capture_stop()
        log.info("Stopping FITS interfaces")
        for fi in self._fits_interfaces:
            fi.capture_stop()
        self._products = {}
        self._update_products_sensor()

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
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(self._control_mode))

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
        """
        @brief      Start the EDD backend processing

        @detail     Not all processing components will respond to a capture_start request.
                    For example the packetisers and roach2 boards will in general constantly
                    stream once configured. Processing components (such as the FITS interfaces)
                    which must be cognisant of scan boundaries should respond to this request.

        @note       This method may be updated in future to pass a 'scan configuration' containing
                    source and position information necessary for the population of output file
                    headers.
        """
        for fi in self._fits_interfaces:
            yield fi.capture_start()
        for product in self._products.values():
            yield product.capture_start()

    @request()
    @return_reply()
    def request_capture_stop(self, req):
        """
        @brief      Stop the EDD backend processing

        @note       This is the KATCP wrapper for the capture_stop command

        @return     katcp reply object [[[ !capture_stop ok | (fail [error description]) ]]]
        """
        if not self.katcp_control_mode:
            return ("fail", "Master controller is in control mode: {}".format(self._control_mode))

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
        """
        @brief      Stop the EDD backend processing

        @detail     Not all processing components will respond to a capture_stop request.
                    For example the packetisers and roach2 boards will in general constantly
                    stream once configured. Processing components (such as the FITS interfaces)
                    which must be cognisant of scan boundaries should respond to this request.
        """
        for fi in self._fits_interfaces:
            yield fi.capture_stop()
        for product in self._products.values():
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
    parser.add_option('', '--r2rm-host', dest='r2rm_host', type=str,
                      help='The IP or host name of the R2RM server to use')
    parser.add_option('', '--r2rm-port', dest='r2rm_port', type=int,
                      help='The port number on which R2RM is serving')
    parser.add_option('', '--scpi-mode', dest='scpi_mode', action="store_true",
                      help='Activate the SCPI interface on startup')
    parser.add_option('', '--log-level', dest='log_level', type=str,
                      help='Port number of status server instance', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    logging.getLogger('katcp').setLevel(logging.DEBUG)
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
        level=opts.log_level.upper(),
        logger=logger)
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting EddMasterController instance")
    server = EddMasterController(
        opts.host, opts.port,
        opts.scpi_interface, opts.scpi_port,
        opts.r2rm_host, opts.r2rm_port)
    signal.signal(
        signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        if opts.scpi_mode:
            server.set_control_mode(server.SCPI)
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(
                server.bind_address))
    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
