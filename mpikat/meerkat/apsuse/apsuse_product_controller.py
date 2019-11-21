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
from tornado.gen import coroutine
from tornado.locks import Event
from katcp import Sensor, Message, KATCPClientResource
from mpikat.core.worker_pool import WorkerAllocationError
from mpikat.core.utils import LoggingSensor
from mpikat.meerkat.katportalclient_wrapper import Interrupt
from mpikat.meerkat.apsuse.apsuse_config import get_required_workers

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

    def __init__(self, parent, product_id, katportal_client, proxy_name):
        """
        @brief      Construct new instance

        @param      parent            The parent ApsMasterController instance

        @param      product_id        The name of the product

        @param      katportal_client       An katportal client wrapper instance

        @param      proxy_name        The name of the proxy associated with this subarray (used as a sensor prefix)

        #NEED FENG CONFIG

        @param      servers           A list of ApsWorkerServer instances allocated to this product controller
        """
        self.log = logging.getLogger(
            "mpikat.apsuse_product_controller.{}".format(product_id))
        self.log.debug(
            "Creating new ApsProductController with args: {}".format(
                ", ".join([str(i) for i in (
                parent, product_id, katportal_client, proxy_name)])))
        self._parent = parent
        self._product_id = product_id
        self._katportal_client = katportal_client
        self._proxy_name = proxy_name
        self._managed_sensors = []
        self._worker_config_map = {}
        self._servers = []
        self._fbf_sb_config = None
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

        self._fbf_sb_config_sensor = Sensor.string(
            "fbfuse-sb-config",
            description="The full FBFUSE schedule block configuration",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._fbf_sb_config_sensor)

        self._worker_configs_sensor = Sensor.string(
            "worker-configs",
            description="The configurations for each worker server",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._worker_configs_sensor)

        self._servers_sensor = Sensor.string(
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

    @coroutine
    def configure(self):
        pass

    @coroutine
    def disable_all_writers(self):
        for server in self._servers:
            yield server.disable_writers()

    @coroutine
    def enable_writers(self):
        beam_map = yield self._katportal_client.get_fbfuse_coherent_beam_positions(self._product_id)
        beam_map.update({"ifbf00000": self._fbf_sb_config["phase-reference"]})
        enable_futures = []
        for server in self._servers:
            worker_config = self._worker_config_map[server]
            sub_beam_list = {}
            for beam in worker_config.incoherent_beams():
                if beam in beam_map:
                    sub_beam_list[beam] = beam_map[beam]
            for beam in worker_config.coherent_beams():
                if beam in beam_map:
                    sub_beam_list[beam] = beam_map[beam]
            enable_futures.append(server.enable_writers(sub_beam_list))
        for future in enable_futures:
            yield future

    @coroutine
    def capture_start(self):
        if not self.ready:
            raise ApsProductStateError([self.READY], self.state)
        self._state_sensor.set_value(self.STARTING)
        self.log.debug("Product moved to 'starting' state")
        # At this point assume we do not know about the SB config and get everything fresh
        proposal_id = yield self._katportal_client.get_proposal_id()
        sb_id = yield self._katportal_client.get_sb_id()
        # determine base output path
        # /output/{proposal_id}/{sb_id}/
        # scan number will be added to the path later
        # The /output/ path is usually a mount of /beegfs/DATA/TRAPUM
        base_output_dir = "/output/{}/{}/".format(proposal_id, sb_id)
        fbf_sb_config = yield self._katportal_client.get_fbfuse_sb_config(self._product_id)
        self._fbf_sb_config = fbf_sb_config
        self._fbf_sb_config_sensor.set_value(fbf_sb_config)
        worker_configs = get_required_workers(fbf_sb_config)

        # allocate workers
        self._worker_config_map = {}
        self._servers = []
        for config in worker_configs:
            try:
                server = self._parent._server_pool.allocate(1)[0]
            except WorkerAllocationError:
                message = (
                    "Could not allocate resources for capture of the following groups\n",
                    "incoherent groups: {}\n".format(",".join(map(str, config.incoherent_groups()))),
                    "coherent groups: {}\n".format(",".join(map(str, config.coherent_groups()))))
                self.log.warning(message)
            else:
                self._worker_config_map[server] = config
                self._servers.append(server)

        # Get all common configuration parameters
        common_config = {
            "bandwidth": fbf_sb_config["bandwidth"],
            "centre-frequency": fbf_sb_config["centre-frequency"],
            "sample-clock": fbf_sb_config["bandwidth"] * 2,
        }
        common_config["sync-epoch"] = yield self._katportal_client.get_sync_epoch()

        common_coherent_config = {
            "heap-size": fbf_sb_config["coherent-beam-heap-size"],
            "idx1-step": fbf_sb_config["coherent-beam-idx1-step"],
            "nchans": fbf_sb_config["nchannels"] / fbf_sb_config["coherent-beam-fscrunch"],
            "nchans-per-heap": fbf_sb_config["coherent-beam-subband-nchans"],
            "sampling-interval": fbf_sb_config["coherent-beam-time-resolution"],
            "base_output_dir": "{}/coherent".format(base_output_dir)
        }

        common_incoherent_config = {
            "heap-size": fbf_sb_config["incoherent-beam-heap-size"],
            "idx1-step": fbf_sb_config["incoherent-beam-idx1-step"],
            "nchans": fbf_sb_config["nchannels"] / fbf_sb_config["incoherent-beam-fscrunch"],
            "nchans-per-heap": fbf_sb_config["incoherent-beam-subband-nchans"],
            "sampling-interval": fbf_sb_config["incoherent-beam-time-resolution"],
            "base_output_dir": "{}/incoherent".format(base_output_dir)
        }

        prepare_futures = []
        all_server_configs = {}
        for server, config in self._worker_config_map.items():
            server_config = {}
            if config.incoherent_groups():
                incoherent_config = deepcopy(common_config)
                incoherent_config.update(common_incoherent_config)
                incoherent_config["beam-ids"] = []
                incoherent_config["stream-indices"] = []
                incoherent_config["mcast-groups"] = []
                incoherent_config["mcast-port"] = 7147  # Where should this info come from?
                for beam in config.incoherent_beams():
                    incoherent_config["beam-ids"].append(beam)
                    incoherent_config["stream-indices"].append(int(beam.lstrip("ifbf")))
                incoherent_config["mcast-groups"].extend(map(str, config.incoherent_groups()))
                server_config["incoherent-beams"] = incoherent_config
            if config.coherent_groups():
                coherent_config = deepcopy(common_config)
                coherent_config.update(common_coherent_config)
                coherent_config["beam-ids"] = []
                coherent_config["stream-indices"] = []
                coherent_config["mcast-groups"] = []
                coherent_config["mcast-port"] = 7147  # Where should this info come from?
                for beam in config.coherent_beams():
                    coherent_config["beam-ids"].append(beam)
                    coherent_config["stream-indices"].append(int(beam.lstrip("cfbf")))
                coherent_config["mcast-groups"].extend(map(str, config.coherent_groups()))
                server_config["coherent-beams"] = coherent_config
            prepare_futures.append(server.prepare(server_config))
            all_server_configs[server] = server_config
        self._worker_configs_sensor.set_value(all_server_configs)

        for future in prepare_futures:
            yield future

        capture_start_futures = []
        for server in self._servers:
            capture_start_futures.append(server.capture_start())

        for future in capture_start_futures:
            yield future

        # At this point we do the data-suspect tracking start
        coherent_beam_tracker = self._katportal_client.get_sensor_tracker(
            "fbfuse", "fbfmc_{}_coherent_beam_data_suspect".format(
                self._product_id))
        incoherent_beam_tracker = self._katportal_client.get_sensor_tracker(
            "fbfuse", "fbfmc_{}_incoherent_beam_data_suspect".format(
                self._product_id))
        yield coherent_beam_tracker.start()
        yield incoherent_beam_tracker.start()

        @coroutine
        def wait_for_on_target():
            self._state_interrupt.clear()
            try:
                yield coherent_beam_tracker.wait_until(
                    False, self._state_interrupt)
                yield incoherent_beam_tracker.wait_until(
                    False, self._state_interrupt)
            except Interrupt:
                pass
            else:
                try:
                    yield self.disable_all_writers()
                    yield self.enable_writers()
                except Exception:
                    log.exception()
            self._parent.ioloop.add_callback(wait_for_off_target)

        @coroutine
        def wait_for_off_target():
            self._state_interrupt.clear()
            try:
                yield coherent_beam_tracker.wait_until(
                    True, self._state_interrupt)
                yield incoherent_beam_tracker.wait_until(
                    True, self._state_interrupt)
            except Interrupt:
                pass
            else:
                yield self.disable_all_writers()
            self._parent.ioloop.add_callback(wait_for_on_target)

        self._parent.ioloop.add_callback(wait_for_on_target)
        server_str = ",".join(["{s.hostname}:{s.port}".format(
            s=server) for server in self._servers])
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
        self._state_sensor.set_value(self.STOPPING)
        self._state_interrupt.set()
        yield self.disable_all_writers()
        capture_stop_futures = []
        for server in self._worker_config_map.keys():
            capture_stop_futures.append(server.capture_stop())
        for future in capture_stop_futures:
            yield future
        for server in self._worker_config_map.keys():
            self._parent._server_pool.deallocate(server)
        self._worker_config_map = {}
        self._servers_sensor.set_value("")
        self._state_sensor.set_value(self.READY)

