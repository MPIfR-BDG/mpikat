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
    Wrapper class for an EDD product product.
    """
    def __init__(self, parent, product_id, r2rm_addr):
        """
        @brief      Construct new instance

        @param      parent            The parent EddRoach2MasterController instance
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
        yield self._r2rm_client.until_synced(2)
        response = yield self._r2rm_client.req.deconfigure_board(self._icom_id, EDD_R2RM_USER, self._firmware)
        if not response.reply.reply_ok():
            self.log.error("Error on deconfigure request: {}".format(response.reply.arguments[1]))
            raise EddRoach2ProductError(response.reply.arguments[1])
        self.teardown_sensors()
        self._firmware = None
        self._icom_id = None

    @state_change(["idle", "error"], "capturing", "preparing")
    @coroutine
    def configure(self, config):
        yield self._r2rm_client.until_synced(2)
        self._icom_id = config["icom_id"]
        self._firmware = config["firmware"]
        response = yield self._r2rm_client.req.configure_board(self._icom_id, EDD_R2RM_USER, self._firmware)
        if not response.reply.reply_ok():
            self.log.error("Error on configure request: {}".format(response.reply.arguments[1]))
            raise EddRoach2ProductError(response.reply.arguments[1])
        _, firmware_ip, firmware_port = response.reply.arguments
        firmware_client = KATCPClientResource(dict(
            name="firmware-client",
            address=(firmware_ip, firmware_port),
            controlled=True))
        firmware_client.start()
        yield firmware_client.until_synced(2)
        for command, args in config["commands"]:
            response = yield firmware_client.req[command](*args)
            if not response.reply.reply_ok():
                self.log.error("Error on {}->{} request: {}".format(command, args, response.reply.arguments[1]))
                raise EddRoach2ProductError(response.reply.arguments[1])
        firmware_client.stop()

    @coroutine
    def capture_start(self):
        pass

    @coroutine
    def capture_stop(self):
        pass


