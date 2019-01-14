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
from mpikat.core.worker_pool import WorkerAllocationError
from mpikat.core.utils import LoggingSensor

log = logging.getLogger("mpikat.paf_product_controller")

class PafProductStateError(Exception):
    def __init__(self, expected_states, current_state):
        message = "Possible states for this operation are '{}', but current state is '{}'".format(
            expected_states, current_state)
        super(PafProductStateError, self).__init__(message)

class PafProductController(object):
    """
    Wrapper class for an PAF product.
    """
    STATES = ["idle", "preparing", "ready", "starting", "capturing", "stopping", "error"]
    IDLE, PREPARING, READY, STARTING, CAPTURING, STOPPING, ERROR = STATES

    def __init__(self, parent, product_id):
        """
        @brief      Construct new instance

        @param      parent            The parent PafMasterController instance
        """
        self.log = logging.getLogger("mpikat.apsuse_product_controller.{}".format(product_id))
        self._parent = parent
        self._product_id = product_id
        self._managed_sensors = []
        self._servers = []
        self.setup_sensors()

    def __del__(self):
        self.teardown_sensors()

    def info(self):
        """
        @brief    Return a metadata dictionary describing this product controller
        """
        out = {
            "state":self.state,
        }
        return out

    def add_sensor(self, sensor):
        """
        @brief    Add a sensor to the parent object

        @note     This method is used to wrap calls to the add_sensor method
                  on the parent PafMasterController instance. In order to
                  disambiguate between sensors from describing different products
                  the associated proxy name is used as sensor prefix. For example
                  the "servers" sensor will be seen by clients connected to the
                  PafMasterController server as "<proxy_name>-servers" (e.g.
                  "apsuse_1-servers").
        """
        prefix = "{}.".format(self._product_id)
        if sensor.name.startswith(prefix):
            self._parent.add_sensor(sensor)
        else:
            sensor.name = "{}{}".format(prefix, sensor.name)
            self._parent.add_sensor(sensor)
        self._managed_sensors.append(sensor)

    def setup_sensors(self):
        """
        @brief    Setup the default KATCP sensors.

        @note     As this call is made only upon an PAF configure call a mass inform
                  is required to let connected clients know that the proxy interface has
                  changed.
        """
        self._state_sensor = LoggingSensor.discrete(
            "state",
            description = "Denotes the state of this PAF instance",
            params = self.STATES,
            default = self.IDLE,
            initial_status = Sensor.NOMINAL)
        self._state_sensor.set_logger(self.log)
        self.add_sensor(self._state_sensor)

        self._servers_sensor = Sensor.string(
            "servers",
            description = "The worker server instances currently allocated to this product",
            default = ",".join(["{s.hostname}:{s.port}".format(s=server) for server in self._servers]),
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._servers_sensor)
        self._parent.mass_inform(Message.inform('interface-changed'))
        self._state_sensor.set_value(self.IDLE)

    def teardown_sensors(self):
        """
        @brief    Remove all sensors created by this product from the parent server.

        @note     This method is required for cleanup to stop the APS sensor pool
                  becoming swamped with unused sensors.
        """
        for sensor in self._managed_sensors:
            self._parent.remove_sensor(sensor)
        self._managed_sensors = []
        self._parent.mass_inform(Message.inform('interface-changed'))

    @property
    def servers(self):
        return self._servers

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
    def ready(self):
        return self.state == self.READY

    @property
    def preparing(self):
        return self.state == self.PREPARING

    @property
    def error(self):
        return self.state == self.ERROR

    @property
    def state(self):
        return self._state_sensor.value()

    def set_error_state(self, message):
        self._state_sensor.set_value(self.ERROR)

    @coroutine
    def deconfigure(self):
        if self.capturing:
            yield self.stop()
        deconfigure_futures = []
        for server in self._servers:
            deconfigure_futures.append(server._client.req.deconfigure())
        for future,server in zip(deconfigure_futures, self._servers):
            result = yield future
        self.teardown_sensors()
        self._parent._server_pool.deallocate(self._servers)
        self._servers = []

    @coroutine
    def configure(self, dummy_string):
        if not self.idle:
            raise PafProductStateError([self.IDLE], self.state)
        self._state_sensor.set_value(self.PREPARING)
        self.log.debug("Product moved to 'preparing' state")
        # Here we always allocate all servers to the backend
        nservers = self._parent._server_pool.navailable()
        servers = self._parent._server_pool.allocate(nservers)
        configure_futures = []
        for server in servers:
            self._servers.append(server)
            configure_futures.append(server._client.req.configure(dummy_string))
        for future in configure_futures:
            result = yield future
        server_str = ",".join(["{s.hostname}:{s.port}".format(s=server) for server in self._servers])
        self._servers_sensor.set_value(server_str)
        self._state_sensor.set_value(self.READY)
        self.log.debug("Product moved to 'ready' state")

    @coroutine
    def start_capture(self):
        if not self.ready:
            raise PafProductStateError([self.READY], self.state)
        self._state_sensor.set_value(self.STARTING)
        self.log.debug("Product moved to 'starting' state")
        start_futures = []
        for server in self._servers:
            start_futures.append(server._client.req.start_capture())
        for future in start_futures:
            result = yield future
        self._state_sensor.set_value(self.CAPTURING)
        self.log.debug("Product moved to 'capturing' state")

    @coroutine
    def stop_capture(self):
        """
        @brief      Stops the beamformer servers streaming.

        @detail     This should only be called on a schedule block reconfiguration
                    if the same configuration persists between schedule blocks then
                    it is preferable to continue streaming rather than stopping and
                    starting again.
        """
        if not self.capturing and not self.error:
            return
        self._state_sensor.set_value(self.STOPPING)
        self.log.debug("Product moved to 'stopping' state")
        stop_futures = []
        for server in self._servers:
            stop_futures.append(server._client.req.stop_capture())
        for future in stop_futures:
            result = yield future
        self._state_sensor.set_value(self.READY)
        self.log.debug("Product moved to 'ready' state")