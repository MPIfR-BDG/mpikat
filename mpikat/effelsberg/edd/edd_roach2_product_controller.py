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
import time
from tornado.gen import coroutine, Return
from katcp import Sensor, Message, KATCPClientResource
from mpikat.core.utils import LoggingSensor
from mpikat.core.product_controller import ProductController, state_change

log = logging.getLogger("mpikat.edd_roach2_product_controller")

EDD_R2RM_USER = "EDD_control_system"

class EddRoach2ProductStateError(Exception):
    def __init__(self, expected_states, current_state):
        message = "Possible states for this operation are '{}', but current state is '{}'".format(
            expected_states, current_state)
        super(EddRoach2ProductStateError, self).__init__(message)

class EddRoach2ProductError(Exception):
    pass

class EddRoach2ProductController(ProductController):
    """
    Wrapper class for an EDD ROACH2 product.
    """
    def __init__(self, parent, product_id, r2rm_addr):
        """
        @brief      Construct new instance

        @param      parent            The parent EddRoach2MasterController instance
        @param      product_id        A unique identifier for this product
        @param      r2rm_addr         The address of the R2RM (ROACH2 resource manager) to be
                                      used by this product. Passed in tuple format,
                                      e.g. ("127.0.0.1", 5000)
        """
        super(EddRoach2ProductController, self).__init__(parent, product_id)
        self._r2rm_client = KATCPClientResource(dict(
            name="r2rm-client",
            address=r2rm_addr,
            controlled=True))
        self._r2rm_client.start()
        self._firmware = None
        self._icom_id = None

    def setup_sensors(self):
        """
        @brief    Setup the default KATCP sensors.

        @note     As this call is made only upon an EDD product configure call a mass inform
                  is required to let connected clients know that the proxy interface has
                  changed.
        """
        super(EddRoach2ProductController, self).setup_sensors()
        self._firmware_server_sensor = Sensor.string(
            "firmware-server",
            description = "The address of the firmware server started by this product",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._firmware_server_sensor)
        self._parent.mass_inform(Message.inform('interface-changed'))

    @state_change(["capturing", "error"], "idle")
    @coroutine
    def deconfigure(self):
        """
        @brief      Deconfigure the product

        @detail     This method will remove any product sensors that were added to the
                    parent master controller.
        """
        yield self._r2rm_client.until_synced(2)
        response = yield self._r2rm_client.req.force_deconfigure_board(self._icom_id)
        if not response.reply.reply_ok():
            self.log.error("Error on deconfigure request: {}".format(response.reply.arguments[1]))
            raise EddRoach2ProductError(response.reply.arguments[1])
        self.teardown_sensors()
        self._firmware = None
        self._icom_id = None

    @state_change(["idle", "error"], "capturing", "preparing")
    @coroutine
    def configure(self, config):
        """
        @brief      Configure the roach2 product

        @param      config  A dictionary containing configuration information.
                            The dictionary should have a form similar to:
                            @code
                                 {
                                     "id": "roach2_spectrometer",
                                     "type": "roach2",
                                     "icom_id": "R2-EDD",
                                     "firmware": "EDDFirmware",
                                     "commands":
                                     [
                                         ["program", []],
                                         ["start", []],
                                         ["set_integration_period", [1000.0]],
                                         ["set_destination_address", ["10.10.1.12", 60001]]
                                     ]
                                 }
                            @endcode

        @detail  This method will request the specified roach2 board from the R2RM server
                 and request a firmware deployment. The values of the 'icom_id' and 'firmware'
                 must correspond to valid managed roach2 boards and firmwares as understood by
                 the R2RM server.
        """
        log.debug("Syncing with R2RM server")
        yield self._r2rm_client.until_synced(2)
        self._icom_id = config["icom_id"]
        self._firmware = config["firmware"]
        log.debug("Sending configure request to R2RM server")
        response = yield self._r2rm_client.req.configure_board(self._icom_id, EDD_R2RM_USER, self._firmware, timeout=20)
        if not response.reply.reply_ok():
            self.log.error("Error on configure request: {}".format(response.reply.arguments[1]))
            raise EddRoach2ProductError(response.reply.arguments[1])
        _, firmware_ip, firmware_port = response.reply.arguments
        log.debug("Connecting client to activated firmware server @ {}:{}".format(firmware_ip, firmware_port))
        firmware_client = KATCPClientResource(dict(
            name="firmware-client",
            address=(firmware_ip, firmware_port),
            controlled=True))
        firmware_client.start()
        log.debug("Syncing with firmware client")
        yield firmware_client.until_synced(2)
        for command, args in config["commands"]:
            log.debug("Sending firmware server request '{}' with args '{}'".format(command, args))
            response = yield firmware_client.req[command](*args, timeout=20)
            if not response.reply.reply_ok():
                self.log.error("Error on {}->{} request: {}".format(command, args, response.reply.arguments[1]))
                raise EddRoach2ProductError(response.reply.arguments[1])
        log.debug("Stopping client connection to firmware server")
        firmware_client.stop()

    @coroutine
    def capture_start(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        pass

    @coroutine
    def capture_stop(self):
        """
        @brief      A no-op method for supporting the product controller interface.
        """
        pass


