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
from copy import deepcopy
from tornado.gen import coroutine, Return
from tornado.locks import Event
from katcp import Sensor, Message, KATCPClientResource
from katpoint import Target, Antenna
from mpikat.core.worker_pool import WorkerAllocationError
from mpikat.core.ip_manager import ip_range_from_stream
from mpikat.core.utils import parse_csv_antennas, LoggingSensor
from mpikat.meerkat.apsuse.apsuse_config import get_required_workers
from mpikat.meerkat.apsuse.apsuse_beam_monitor import FBFUSEBeamMonitor


log = logging.getLogger("mpikat.apsuse_product_controller")


class ApsProductStateError(Exception):
    def __init__(self, expected_states, current_state):
        message = "Possible states for this operation are '{}', but current state is '{}'".format(
            expected_states, current_state)
        super(ApsProductStateError, self).__init__(message)


class ApsProductController(object):
    """
    Wrapper class for an APSUSE product.
    """
    STATES = ["idle", "preparing", "ready", "starting", "capturing", "stopping", "error"]
    IDLE, PREPARING, READY, STARTING, CAPTURING, STOPPING, ERROR = STATES

    def __init__(self, parent, product_id, fbf_monitor, proxy_name):
        """
        @brief      Construct new instance

        @param      parent            The parent ApsMasterController instance

        @param      product_id        The name of the product

        @param      fbf_monitor       An FbfKatportalMonitor instance

        @param      proxy_name        The name of the proxy associated with this subarray (used as a sensor prefix)

        #NEED FENG CONFIG

        @param      servers           A list of ApsWorkerServer instances allocated to this product controller
        """
        self.log = logging.getLogger("mpikat.apsuse_product_controller.{}".format(product_id))
        self.log.debug("Creating new ApsProductController with args: {}".format(
            ", ".join([str(i) for i in (parent, product_id, fbf_monitor, proxy_name)])))
        self._parent = parent
        self._product_id = product_id
        self._fbf_monitor = fbf_monitor
        self._fbf_beam_monitor = FBFUSEBeamMonitor() #???
        self._on_target_tracker = OnTargetTracker() #???
        self._proxy_name = proxy_name
        self._managed_sensors = []
        self._servers = []
        self._state_interrupt = Event()
        self.setup_sensors()

    def __del__(self):
        self.teardown_sensors()

    def info(self):
        """
        @brief    Return a metadata dictionary describing this product controller
        """
        out = {
            "state":self.state,
            "proxy_name":self._proxy_name
        }
        return out

    def add_sensor(self, sensor):
        """
        @brief    Add a sensor to the parent object

        @note     This method is used to wrap calls to the add_sensor method
                  on the parent ApsMasterController instance. In order to
                  disambiguate between sensors from describing different products
                  the associated proxy name is used as sensor prefix. For example
                  the "servers" sensor will be seen by clients connected to the
                  ApsMasterController server as "<proxy_name>-servers" (e.g.
                  "apsuse_1-servers").
        """
        prefix = "{}.".format(self._product_id)
        if sensor.name.startswith(prefix):
            self._parent.add_sensor(sensor)
        else:
            sensor.name = "{}{}".format(prefix,sensor.name)
            self._parent.add_sensor(sensor)
        self._managed_sensors.append(sensor)

    def setup_sensors(self):
        """
        @brief    Setup the default KATCP sensors.

        @note     As this call is made only upon an APSUSE configure call a mass inform
                  is required to let connected clients know that the proxy interface has
                  changed.
        """
        self._state_sensor = LoggingSensor.discrete(
            "state",
            description="Denotes the state of this APS instance",
            params=self.STATES,
            default=self.IDLE,
            initial_status=Sensor.NOMINAL)
        self._state_sensor.set_logger(self.log)
        self.add_sensor(self._state_sensor)

        self._fbf_sb_config_sensor=Sensor.string(
            "fbfuse-sb-config",
            description="The full FBFUSE schedule block configuration",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._fbf_sb_config_sensor)

        self._servers_sensor=Sensor.string(
            "servers",
            description="The worker server instances currently allocated to this product",
            default=",".join(["{s.hostname}:{s.port}".format(s=server) for server in self._servers]),
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._servers_sensor)

        # Server to multicast group map

        # ???

        self._parent.mass_inform(Message.inform('interface-changed'))
        self._state_sensor.set_value(self.READY)

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

    def deconfigure(self):
        """
        @brief  Deconfigure the product. To be called on a subarray deconfigure.

        @detail This is the final cleanup operation for the product, it should delete all sensors
                and ensure the release of all resource allocations.
        """
        self.teardown_sensors()

    @coroutine
    def target_start(self, target):
        """
        @brief  Disable recording of current observation and prepare for next

        @detail   Disables all writers before waiting on RETILING state
                  on FBFUSE, after retiling, writing is re-enabled with
                  the correct beam coordinates deployed.
        """

        # Target start implies previous target has ended
        yield self.disable_all_writers()

        @coroutine
        def trigger_writers():
            # Now we need to wait for all beams to be set by FBFUSE
            # Only need to wait for beam zero to change then put a timer
            # on to wait for all beam updates.
            yield self._fbf_monitor.

            # Wait until we are on source before dispatching the enable call
            # if we are already on source then dispatch immediately

            # This setup cannot handle on the fly retilings
            # (but APSUSE shouldn't need to).
            yield self.enable_writers()

        self.ioloop.add_callback(trigger_writers)

    @coroutine
    def disable_all_writers(self):
        for server in self._servers:
            yield server.disable_writers()

    @coroutine
    def enable_writers(self):
        for server in self._servers:
            yield server.enable_writers(self._beam_map)

    @coroutine
    def capture_start(self):
        if not self.ready:
            raise ApsProductStateError([self.READY], self.state)
        self._state_sensor.set_value(self.STARTING)
        self.log.debug("Product moved to 'starting' state")
        fbf_sb_config = yield self._fbf_monitor.get_sb_config()
        yield self._fbf_monitor.subscribe_to_beams()

        worker_configs = get_required_workers(fbf_sb_config)
        capture_start_futures = []
        for worker_config in worker_configs:
            try:
                server = self._parent._server_pool.allocate(1)
            except WorkerAllocationError:
                self.log.warning("Could not allocate enough workers to catch all FBFUSE data")
                break
            else:
                self._servers.append(server)
                yield server.prepare(fbf_sb_config)
                #capture_start_future = server.capture_start(worker_config)
                #capture_start_futures.append(capture_start_future)
                #
        for future in capture_start_futures:
            result = yield future
            #do something with future
        server_str = ",".join(["{s.hostname}:{s.port}".format(s=server) for server in self._servers])
        self._servers_sensor.set_value(server_str)
        self._state_sensor.set_value(self.CAPTURING)
        self.log.debug("Product moved to 'capturing' state")

    @coroutine
    def capture_stop(self):
        """
        @brief      Stops the beamformer servers streaming.

        @detail     This should only be called on a schedule block reconfiguration
                    if the same configuration persists between schedule blocks then
                    it is preferable to continue streaming rather than stopping and
                    starting again.
        """
        if not self.capturing and not self.error:
            return

        yield self._fbf_monitor.unsubscribe_from_beams()
        self._state_sensor.set_value(self.STOPPING)
        self.target_stop()

        # talk to servers and do something clever

        # deallocate servers

        self._state_sensor.set_value(self.READY)