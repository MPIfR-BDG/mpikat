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
from katcp import Sensor, Message, KATCPClientResource
from katpoint import  Target
from mpikat.fbfuse_beam_manager import BeamManager
from mpikat.fbfuse_delay_engine import DelayEngine
from mpikat.fbfuse_config import FbfConfigurationManager
from mpikat.utils import parse_csv_antennas

log = logging.getLogger("mpikat.fbfuse_product_controller")

class FbfProductStateError(Exception):
    def __init__(self, expected_states, current_state):
        message = "Possible states for this operation are '{}', but current state is '{}'".format(
            expected_states, current_state)
        super(FbfProductStateError, self).__init__(message)

class FbfProductController(object):
    """
    Wrapper class for an FBFUSE product.
    """
    STATES = ["idle", "preparing", "ready", "starting", "capturing", "stopping"]
    IDLE, PREPARING, READY, STARTING, CAPTURING, STOPPING = STATES

    def __init__(self, parent, product_id, katpoint_antennas,
                 n_channels, streams, proxy_name, feng_config):
        """
        @brief      Construct new instance

        @param      parent            The parent FbfMasterController instance

        @param      product_id        The name of the product

        @param      katpoint_antennas A list of katpoint.Antenna objects

        @param      n_channels        The integer number of frequency channels provided by the CBF.

        @param      streams           A dictionary containing config keys and values describing the streams.

        @param      proxy_name        The name of the proxy associated with this subarray (used as a sensor prefix)

        @param      servers           A list of FbfWorkerServer instances allocated to this product controller
        """
        log.debug("Creating new FbfProductController with args: {}".format(
            ", ".join([str(i) for i in (parent, product_id, katpoint_antennas, n_channels,
                streams, proxy_name, feng_config)])))
        self._parent = parent
        self._product_id = product_id
        self._antennas = ",".join([a.name for a in katpoint_antennas])
        self._katpoint_antennas = katpoint_antennas
        self._antenna_map = {a.name: a for a in self._katpoint_antennas}
        self._n_channels = n_channels
        self._streams = streams
        self._proxy_name = proxy_name
        self._feng_config = feng_config
        self._servers = []
        self._beam_manager = None
        self._delay_engine = None
        self._coherent_beam_ip_range = None
        self._ca_client = None
        self._managed_sensors = []
        self._ip_allocations = []
        self._default_sb_config = {
            u'coherent-beams-nbeams':400,
            u'coherent-beams-tscrunch':16,
            u'coherent-beams-fscrunch':1,
            u'coherent-beams-antennas':self._antennas,
            u'coherent-beams-granularity':6,
            u'incoherent-beam-tscrunch':16,
            u'incoherent-beam-fscrunch':1,
            u'incoherent-beam-antennas':self._antennas,
            u'bandwidth':self._feng_config['bandwidth'],
            u'centre-frequency':self._feng_config['centre-frequency']}
        self.setup_sensors()

    def __del__(self):
        self.teardown_sensors()

    def info(self):
        """
        @brief    Return a metadata dictionary describing this product controller
        """
        out = {
            "antennas":self._antennas,
            "nservers":len(self.servers),
            "capturing":self.capturing,
            "streams":self._streams,
            "nchannels":self._n_channels,
            "proxy_name":self._proxy_name
        }
        return out

    def add_sensor(self, sensor):
        """
        @brief    Add a sensor to the parent object

        @note     This method is used to wrap calls to the add_sensor method
                  on the parent FbfMasterController instance. In order to
                  disambiguate between sensors from describing different products
                  the associated proxy name is used as sensor prefix. For example
                  the "servers" sensor will be seen by clients connected to the
                  FbfMasterController server as "<proxy_name>-servers" (e.g.
                  "FBFUSE_1-servers").
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

        @note     As this call is made only upon an FBFUSE configure call a mass inform
                  is required to let connected clients know that the proxy interface has
                  changed.
        """
        self._state_sensor = Sensor.discrete(
            "state",
            description = "Denotes the state of this FBF instance",
            params = self.STATES,
            default = self.IDLE,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._state_sensor)

        self._ca_address_sensor = Sensor.string(
            "configuration-authority",
            description = "The address of the server that will be deferred to for configurations",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._ca_address_sensor)

        self._available_antennas_sensor = Sensor.string(
            "available-antennas",
            description = "The antennas that are currently available for beamforming",
            default = self._antennas,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._available_antennas_sensor)

        self._bandwidth_sensor = Sensor.float(
            "bandwidth",
            description = "The bandwidth this product is configured to process",
            default = self._default_sb_config['bandwidth'],
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._bandwidth_sensor)

        self._nchans_sensor = Sensor.integer(
            "nchannels",
            description = "The number of channels to be processesed",
            default = self._n_channels,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._nchans_sensor)

        self._cfreq_sensor = Sensor.float(
            "centre-frequency",
            description = "The centre frequency of the band this product configured to process",
            default = self._default_sb_config['centre-frequency'],
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._cfreq_sensor)

        self._cbc_nbeams_sensor = Sensor.integer(
            "coherent-beam-count",
            description = "The number of coherent beams that this FBF instance can currently produce",
            default = self._default_sb_config['coherent-beams-nbeams'],
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._cbc_nbeams_sensor)

        self._cbc_nbeams_per_group = Sensor.integer(
            "coherent-beam-count-per-group",
            description = "The number of coherent beams packed into a multicast group",
            default = 1,
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._cbc_nbeams_per_group)

        self._cbc_ngroups = Sensor.integer(
            "coherent-beam-ngroups",
            description = "The number of multicast groups used for coherent beam transmission",
            default = 1,
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._cbc_ngroups)

        self._cbc_nbeams_per_server_set = Sensor.integer(
            "coherent-beam-nbeams-per-server-set",
            description = "The number of beams produced by each server set",
            default = 1,
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._cbc_nbeams_per_server_set)

        self._cbc_tscrunch_sensor = Sensor.integer(
            "coherent-beam-tscrunch",
            description = "The number time samples that will be integrated when producing coherent beams",
            default = self._default_sb_config['coherent-beams-tscrunch'],
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._cbc_tscrunch_sensor)

        self._cbc_fscrunch_sensor = Sensor.integer(
            "coherent-beam-fscrunch",
            description = "The number frequency channels that will be integrated when producing coherent beams",
            default = self._default_sb_config['coherent-beams-fscrunch'],
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._cbc_fscrunch_sensor)

        self._cbc_antennas_sensor = Sensor.string(
            "coherent-beam-antennas",
            description = "The antennas that will be used when producing coherent beams",
            default = self._default_sb_config['coherent-beams-antennas'],
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._cbc_antennas_sensor)

        self._cbc_mcast_groups_sensor = Sensor.string(
            "coherent-beam-multicast-groups",
            description = "Multicast groups used by this instance for sending coherent beam data",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._cbc_mcast_groups_sensor)

        self._ibc_nbeams_sensor = Sensor.integer(
            "incoherent-beam-count",
            description = "The number of incoherent beams that this FBF instance can currently produce",
            default = 1,
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._ibc_nbeams_sensor)

        self._ibc_tscrunch_sensor = Sensor.integer(
            "incoherent-beam-tscrunch",
            description = "The number time samples that will be integrated when producing incoherent beams",
            default = self._default_sb_config['incoherent-beam-tscrunch'],
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._ibc_tscrunch_sensor)

        self._ibc_fscrunch_sensor = Sensor.integer(
            "incoherent-beam-fscrunch",
            description = "The number frequency channels that will be integrated when producing incoherent beams",
            default = self._default_sb_config['incoherent-beam-fscrunch'],
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._ibc_fscrunch_sensor)

        self._ibc_antennas_sensor = Sensor.string(
            "incoherent-beam-antennas",
            description = "The antennas that will be used when producing incoherent beams",
            default = self._default_sb_config['incoherent-beam-antennas'],
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._ibc_antennas_sensor)

        self._ibc_mcast_group_sensor = Sensor.string(
            "incoherent-beam-multicast-group",
            description = "Multicast group used by this instance for sending incoherent beam data",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._ibc_mcast_group_sensor)

        self._servers_sensor = Sensor.string(
            "servers",
            description = "The worker server instances currently allocated to this product",
            default = ",".join(["{s.hostname}:{s.port}".format(s=server) for server in self._servers]),
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._servers_sensor)

        self._nserver_sets_sensor = Sensor.integer(
            "nserver-sets",
            description = "The number of server sets (independent subscriptions to the F-engines)",
            default = 1,
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._nserver_sets_sensor)

        self._nservers_per_set_sensor = Sensor.integer(
            "nservers-per-set",
            description = "The number of servers per server set",
            default = 1,
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._nservers_per_set_sensor)

        self._delay_engine_sensor = Sensor.string(
            "delay-engines",
            description = "The addresses of the delay engines serving this product",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._delay_engine_sensor)
        self._parent.mass_inform(Message.inform('interface-changed'))

    def teardown_sensors(self):
        """
        @brief    Remove all sensors created by this product from the parent server.

        @note     This method is required for cleanup to stop the FBF sensor pool
                  becoming swamped with unused sensors.
        """
        for sensor in self._managed_sensors:
            self._parent.remove_sensor(sensor)
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
    def state(self):
        return self._state_sensor.value()

    def _verify_antennas(self, antennas):
        """
        @brief      Verify that a set of antennas is available to this instance.

        @param      antennas   A CSV list of antenna names
        """
        antennas_set = set([ant.name for ant in self._katpoint_antennas])
        requested_antennas = set(antennas)
        return requested_antennas.issubset(antennas_set)

    def set_configuration_authority(self, hostname, port):
        if self._ca_client:
            self._ca_client.stop()
        self._ca_client = KATCPClientResource(dict(
            name = 'configuration-authority-client',
            address = (hostname, port),
            controlled = True))
        self._ca_client.start()
        self._ca_address_sensor.set_value("{}:{}".format(hostname, port))

    @coroutine
    def get_ca_sb_configuration(self, sb_id):
        yield self._ca_client.until_synced()
        try:
            response = yield self._ca_client.req.get_schedule_block_configuration(self._proxy_name, sb_id)
        except Exception as error:
            log.error("Request for SB configuration to CA failed with error: {}".format(str(error)))
            raise error
        try:
            config_dict = json.loads(response.reply.arguments[1])
        except Exception as error:
            log.error("Could not parse CA SB configuration with error: {}".format(str(error)))
            raise error
        self.set_sb_configuration(config_dict)

    def reset_sb_configuration(self):
        self._parent._server_pool.deallocate(self._servers)
        for ip_range in self._ip_allocations:
            self._parent._ip_pool.free(ip_range)

    def set_sb_configuration(self, config_dict):
        """
        @brief  Set the schedule block configuration for this product

        @param  config_dict  A dictionary specifying configuation parameters
        """
        self.reset_sb_configuration()
        config = deepcopy(self._default_sb_config)
        config.update(config_dict)
        # first we need to get one ip address for the incoherent beam
        ibc_mcast_group = self._parent._ip_pool.allocate(1)
        self._ip_allocations.append(ibc_mcast_group)
        self._ibc_mcast_group_sensor.set_value(ibc_mcast_group.format_katcp())
        largest_ip_range = self._parent._ip_pool.largest_free_range()
        nworkers_available = self._parent._server_pool.navailable()
        cm = FbfConfigurationManager(len(self._katpoint_antennas),
            self._feng_config['bandwidth'], self._n_channels,
            nworkers_available, largest_ip_range)
        requested_nantennas = len(parse_csv_antennas(config['coherent-beams-antennas']))
        mcast_config = cm.get_configuration(
            config['coherent-beams-tscrunch'],
            config['coherent-beams-fscrunch'],
            config['coherent-beams-nbeams'],
            requested_nantennas,
            config['bandwidth'],
            config['coherent-beams-granularity'])
        self._bandwidth_sensor.set_value(config['bandwidth'])
        self._cfreq_sensor.set_value(config['centre-frequency'])
        self._nchans_sensor.set_value(mcast_config['num_chans'])
        self._cbc_nbeams_sensor.set_value(mcast_config['num_beams'])
        self._cbc_nbeams_per_group.set_value(mcast_config['num_beams_per_mcast_group'])
        self._cbc_ngroups.set_value(mcast_config['num_mcast_groups'])
        self._cbc_nbeams_per_server_set.set_value(mcast_config['num_beams_per_worker_set'])
        self._cbc_tscrunch_sensor.set_value(config['coherent-beams-tscrunch'])
        self._cbc_fscrunch_sensor.set_value(config['coherent-beams-fscrunch'])
        self._cbc_antennas_sensor.set_value(config['coherent-beams-antennas'])
        self._ibc_tscrunch_sensor.set_value(config['incoherent-beam-tscrunch'])
        self._ibc_fscrunch_sensor.set_value(config['incoherent-beam-fscrunch'])
        self._ibc_antennas_sensor.set_value(config['incoherent-beam-antennas'])
        self._servers = self._parent._server_pool.allocate(mcast_config['num_workers_total'])
        server_str = ",".join(["{s.hostname}:{s.port}".format(s=server) for server in self._servers])
        self._servers_sensor.set_value(server_str)
        self._nserver_sets_sensor.set_value(mcast_config['num_worker_sets'])
        self._nservers_per_set_sensor.set_value(mcast_config['num_workers_per_set'])
        cbc_mcast_groups = self._parent._ip_pool.allocate(mcast_config['num_mcast_groups'])
        self._ip_allocations.append(cbc_mcast_groups)
        self._cbc_mcast_groups_sensor.set_value(cbc_mcast_groups.format_katcp())

    @coroutine
    def get_ca_target_configuration(self, target):
        def ca_target_update_callback(received_timestamp, timestamp, status, value):
            # TODO, should we really reset all the beams or should we have
            # a mechanism to only update changed beams
            config_dict = json.loads(value)
            self.reset_beams()
            for target_string in config_dict.get('beams',[]):
                target = Target(target_string)
                self.add_beam(target)
            for tiling in config_dict.get('tilings',[]):
                target  = Target(tiling['target']) #required
                freq    = float(tiling.get('reference_frequency', 1.4e9))
                nbeams  = int(tiling['nbeams'])
                overlap = float(tiling.get('overlap', 0.5))
                epoch   = float(tiling.get('epoch', time.time()))
                self.add_tiling(target, nbeams, freq, overlap, epoch)
        yield self._ca_client.until_synced()
        try:
            response = yield self._ca_client.req.target_configuration_start(self._proxy_name, target.format_katcp())
        except Exception as error:
            log.error("Request for target configuration to CA failed with error: {}".format(str(error)))
            raise error
        if not response.reply.reply_ok():
            error = Exception(response.reply.arguments[1])
            log.error("Request for target configuration to CA failed with error: {}".format(str(error)))
            raise error
        yield self._ca_client.until_synced()
        sensor = self._ca_client.sensor["{}_beam_position_configuration".format(self._proxy_name)]
        sensor.register_listener(ca_target_update_callback)
        self._ca_client.set_sampling_strategy(sensor.name, "event")

    def configure_coherent_beams(self, nbeams, antennas, fscrunch, tscrunch):
        """
        @brief      Set the configuration for coherent beams producted by this instance

        @param      nbeams          The number of beams that will be produced for the provided product_id

        @param      antennas        A comma separated list of physical antenna names. Only these antennas will be used
                                    when generating coherent beams (e.g. m007,m008,m009). The antennas provided here must
                                    be a subset of the antennas in the current subarray. If not an exception will be
                                    raised.

        @param      fscrunch        The number of frequency channels to integrate over when producing coherent beams.

        @param      tscrunch        The number of time samples to integrate over when producing coherent beams.
        """
        if not self.idle:
            raise FbfProductStateError([self.IDLE], self.state)
        if not self._verify_antennas(parse_csv_antennas(antennas)):
            raise AntennaValidationError("Requested antennas are not a subset of the current subarray")
        self._cbc_nbeams_sensor.set_value(nbeams)
        #need a check here to determine if this is a subset of the subarray antennas
        self._cbc_fscrunch_sensor.set_value(fscrunch)
        self._cbc_tscrunch_sensor.set_value(tscrunch)
        self._cbc_antennas_sensor.set_value(antennas)

    def configure_incoherent_beam(self, antennas, fscrunch, tscrunch):
        """
        @brief      Set the configuration for incoherent beams producted by this instance

        @param      antennas        A comma separated list of physical antenna names. Only these antennas will be used
                                    when generating incoherent beams (e.g. m007,m008,m009). The antennas provided here must
                                    be a subset of the antennas in the current subarray. If not an exception will be
                                    raised.

        @param      fscrunch        The number of frequency channels to integrate over when producing incoherent beams.

        @param      tscrunch        The number of time samples to integrate over when producing incoherent beams.
        """
        if not self.idle:
            raise FbfProductStateError([self.IDLE], self.state)
        if not self._verify_antennas(parse_csv_antennas(antennas)):
            raise AntennaValidationError("Requested antennas are not a subset of the current subarray")
        #need a check here to determine if this is a subset of the subarray antennas
        self._ibc_fscrunch_sensor.set_value(fscrunch)
        self._ibc_tscrunch_sensor.set_value(tscrunch)
        self._ibc_antennas_sensor.set_value(antennas)

    def _beam_to_sensor_string(self, beam):
        return beam.target.format_katcp()

    @coroutine
    def target_start(self, target):
        if self._ca_client:
            yield self.get_ca_target_configuration(target)
        else:
            log.warning("No configuration authority is set, using default beam configuration")

    @coroutine
    def target_stop(self):
        if self._ca_client:
            sensor_name = "{}_beam_position_configuration".format(self._proxy_name)
            self._ca_client.set_sampling_strategy(sensor_name, "none")

    @coroutine
    def prepare(self):
        """
        @brief      Prepare the beamformer for streaming

        @detail     This method evaluates the current configuration creates a new DelayEngine
                    and passes a prepare call to all allocated servers.
        """
        if not self.idle:
            raise FbfProductStateError([self.IDLE], self.state)
        self._state_sensor.set_value(self.PREPARING)

        # Here we need to parse the streams and assign beams to streams:
        #mcast_addrs, mcast_port = parse_stream(self._streams['cbf.antenna_channelised_voltage']['i0.antenna-channelised-voltage'])

        if not self._ca_client:
            log.warning("No configuration authority found, using default configuration parameters")
        else:
            #TODO: get the schedule block ID into this call from somewhere (configure?)
            yield self.get_ca_sb_configuration("default_subarray")


        cbc_antennas_names = parse_csv_antennas(self._cbc_antennas_sensor.value())
        cbc_antennas = [self._antenna_map[name] for name in cbc_antennas_names]
        self._beam_manager = BeamManager(self._cbc_nbeams_sensor.value(), cbc_antennas)
        self._delay_engine = DelayEngine("127.0.0.1", 0, self._beam_manager)
        self._delay_engine.start()

        for server in self._servers:
            # each server will take 4 consequtive multicast groups
            pass

        # set up delay engine
        # compile kernels
        # start streaming
        self._delay_engine_sensor.set_value(self._delay_engine.bind_address)


        # Need to tear down the beam sensors here
        self._beam_sensors = []
        for beam in self._beam_manager.get_beams():
            sensor = Sensor.string(
                "coherent-beam-{}".format(beam.idx),
                description="R.A. (deg), declination (deg) and source name for coherent beam with ID {}".format(beam.idx),
                default=self._beam_to_sensor_string(beam),
                initial_status=Sensor.UNKNOWN)
            beam.register_observer(lambda beam, sensor=sensor:
                sensor.set_value(self._beam_to_sensor_string(beam)))
            self._beam_sensors.append(sensor)
            self.add_sensor(sensor)
        self._state_sensor.set_value(self.READY)

        # Only make this call if the the number of beams has changed
        self._parent.mass_inform(Message.inform('interface-changed'))

    def start_capture(self):
        if not self.ready:
            raise FbfProductStateError([self.READY], self.state)
        self._state_sensor.set_value(self.STARTING)
        """
        futures = []
        for server in self._servers:
            futures.append(server.req.start_capture())
        for future in futures:
            try:
                response = yield future
            except:
                pass
        """
        self._state_sensor.set_value(self.CAPTURING)

    def stop_beams(self):
        """
        @brief      Stops the beamformer servers streaming.
        """
        if not self.capturing:
            return
        self._state_sensor.set_value(self.STOPPING)
        for server in self._servers:
            #yield server.req.deconfigure()
            pass
        self._state_sensor.set_value(self.IDLE)

    def add_beam(self, target):
        """
        @brief      Specify the parameters of one managed beam

        @param      target      A KATPOINT target object

        @return     Returns the allocated Beam object
        """
        valid_states = [self.READY, self.CAPTURING, self.STARTING]
        if not self.state in valid_states:
            raise FbfProductStateError(valid_states, self.state)
        return self._beam_manager.add_beam(target)

    def add_tiling(self, target, number_of_beams, reference_frequency, overlap, epoch):
        """
        @brief   Add a tiling to be managed

        @param      target      A KATPOINT target object

        @param      reference_frequency     The reference frequency at which to calculate the synthesised beam shape,
                                            and thus the tiling pattern. Typically this would be chosen to be the
                                            centre frequency of the current observation.

        @param      overlap         The desired overlap point between beams in the pattern. The overlap defines
                                    at what power point neighbouring beams in the tiling pattern will meet. For
                                    example an overlap point of 0.1 corresponds to beams overlapping only at their
                                    10%-power points. Similarly a overlap of 0.5 corresponds to beams overlapping
                                    at their half-power points. [Note: This is currently a tricky parameter to use
                                    when values are close to zero. In future this may be define in sigma units or
                                    in multiples of the FWHM of the beam.]

        @returns    The created Tiling object
        """
        valid_states = [self.READY, self.CAPTURING, self.STARTING]
        if not self.state in valid_states:
            raise FbfProductStateError(valid_states, self.state)
        tiling = self._beam_manager.add_tiling(target, number_of_beams, reference_frequency, overlap)
        tiling.generate(self._katpoint_antennas, epoch)
        return tiling

    def reset_beams(self):
        """
        @brief  reset and deallocate all beams and tilings managed by this instance

        @note   All tiling will be lost on this call and must be remade for subsequent observations
        """
        valid_states = [self.READY, self.CAPTURING, self.STARTING]
        if not self.state in valid_states:
            raise FbfProductStateError(valid_states, self.state)
        self._beam_manager.reset()