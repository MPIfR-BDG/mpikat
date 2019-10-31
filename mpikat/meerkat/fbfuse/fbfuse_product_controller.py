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
import struct
import base64
from copy import deepcopy
from tornado.gen import coroutine, Return
from tornado.locks import Event
from katcp import Sensor, Message, KATCPClientResource
from katpoint import Target, Antenna
from mpikat.core.ip_manager import ip_range_from_stream
from mpikat.core.utils import parse_csv_antennas, LoggingSensor
from mpikat.meerkat.katportalclient_wrapper import SubarrayActivityInterrupt
from mpikat.meerkat.fbfuse import (
    BeamManager,
    BeamAllocationError,
    DelayConfigurationServer,
    FbfConfigurationManager)

N_FENG_STREAMS_PER_WORKER = 4
COH_ANTENNA_GRANULARITY = 4

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
    STATES = ["idle", "preparing", "ready",
              "starting", "capturing", "stopping", "error"]
    IDLE, PREPARING, READY, STARTING, CAPTURING, STOPPING, ERROR = STATES

    def __init__(self, parent, product_id, katpoint_antennas,
                 n_channels, feng_streams, proxy_name, feng_config,
                 subarray_activity_tracker):
        """
        @brief      Construct new instance

        @param      parent            The parent FbfMasterController instance

        @param      product_id        The name of the product

        @param      katpoint_antennas A list of katpoint.Antenna objects

        @param      n_channels        The integer number of frequency channels provided by the CBF.

        @param      feng_streams      A string describing the multicast groups containing F-enging data
                                      (in the form: spead://239.11.1.150+15:7147)

        @param      proxy_name        The name of the proxy associated with this subarray (used as a sensor prefix)

        #NEED FENG CONFIG

        @param      servers           A list of FbfWorkerServer instances allocated to this product controller
        """
        self.log = logging.getLogger(
            "mpikat.fbfuse_product_controller.{}".format(product_id))
        self.log.debug("Creating new FbfProductController with args: {}".format(
            ", ".join([str(i) for i in (parent, product_id, katpoint_antennas, n_channels,
                                        feng_streams, proxy_name, feng_config)])))
        self._parent = parent
        self._product_id = product_id
        self._antennas = ",".join([a.name for a in katpoint_antennas])
        self._katpoint_antennas = katpoint_antennas
        self._antenna_map = {a.name: a for a in self._katpoint_antennas}
        self._n_channels = n_channels
        self._streams = ip_range_from_stream(feng_streams)
        self._proxy_name = proxy_name
        self._feng_config = feng_config
        self._servers = []
        self._beam_manager = None
        self._delay_config_server = None
        self._ca_client = None
        self._activity_tracker_interrupt = Event()
        self._activity_tracker = subarray_activity_tracker
        self._previous_sb_config = None
        self._managed_sensors = []
        self._beam_sensors = []
        self._ibc_mcast_group = None
        self._cbc_mcast_groups = None
        self._current_configuration = None
        self._default_sb_config = {
            u'coherent-beams-nbeams': 400,
            u'coherent-beams-tscrunch': 16,
            u'coherent-beams-fscrunch': 1,
            u'coherent-beams-antennas': self._antennas,
            u'coherent-beams-granularity': 6,
            u'incoherent-beam-tscrunch': 16,
            u'incoherent-beam-fscrunch': 1,
            u'incoherent-beam-antennas': self._antennas,
            u'bandwidth': self._feng_config['bandwidth'],
            u'centre-frequency': self._feng_config['centre-frequency']}
        self.setup_sensors()

    def __del__(self):
        self.teardown_sensors()

    def info(self):
        """
        @brief    Return a metadata dictionary describing this product controller
        """
        out = {
            "antennas": self._antennas,
            "nservers": len(self.servers),
            "state": self.state,
            "streams": self._streams.format_katcp(),
            "nchannels": self._n_channels,
            "proxy_name": self._proxy_name
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
            sensor.name = "{}{}".format(prefix, sensor.name)
            self._parent.add_sensor(sensor)
        self._managed_sensors.append(sensor)

    def setup_sensors(self):
        """
        @brief    Setup the default KATCP sensors.

        @note     As this call is made only upon an FBFUSE configure call a mass inform
                  is required to let connected clients know that the proxy interface has
                  changed.
        """
        self._state_sensor = LoggingSensor.discrete(
            "state",
            description="Denotes the state of this FBF instance",
            params=self.STATES,
            default=self.IDLE,
            initial_status=Sensor.NOMINAL)
        self._state_sensor.set_logger(self.log)
        self.add_sensor(self._state_sensor)

        self._ca_address_sensor = Sensor.string(
            "configuration-authority",
            description="The address of the server that will be deferred to for configurations",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ca_address_sensor)

        self._available_antennas_sensor = Sensor.string(
            "available-antennas",
            description="The antennas that are currently available for beamforming",
            default=json.dumps({antenna.name: antenna.format_katcp()
                                for antenna in self._katpoint_antennas}),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._available_antennas_sensor)

        self._phase_reference_sensor = Sensor.string(
            "phase-reference",
            description="A KATPOINT target string denoting the F-engine phasing centre",
            default="unset,radec,0,0",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._phase_reference_sensor)

        reference_antenna = Antenna("reference,{ref.lat},{ref.lon},{ref.elev}".format(
            ref=self._katpoint_antennas[0].ref_observer))
        self._reference_antenna_sensor = Sensor.string(
            "reference-antenna",
            description="A KATPOINT antenna string denoting the reference antenna",
            default=reference_antenna.format_katcp(),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._reference_antenna_sensor)

        self._bandwidth_sensor = Sensor.float(
            "bandwidth",
            description="The bandwidth this product is configured to process",
            default=self._default_sb_config['bandwidth'],
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._bandwidth_sensor)

        self._nchans_sensor = Sensor.integer(
            "nchannels",
            description="The number of channels to be processesed",
            default=self._n_channels,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nchans_sensor)

        self._cfreq_sensor = Sensor.float(
            "centre-frequency",
            description="The centre frequency of the band this product configured to process",
            default=self._default_sb_config['centre-frequency'],
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cfreq_sensor)

        self._cbc_nbeams_sensor = Sensor.integer(
            "coherent-beam-count",
            description="The number of coherent beams that this FBF instance can currently produce",
            default=self._default_sb_config['coherent-beams-nbeams'],
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_nbeams_sensor)

        self._cbc_nbeams_per_group = Sensor.integer(
            "coherent-beam-count-per-group",
            description="The number of coherent beams packed into a multicast group",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_nbeams_per_group)

        self._cbc_ngroups = Sensor.integer(
            "coherent-beam-ngroups",
            description="The number of multicast groups used for coherent beam transmission",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_ngroups)

        self._cbc_nbeams_per_server_set = Sensor.integer(
            "coherent-beam-nbeams-per-server-set",
            description="The number of beams produced by each server set",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_nbeams_per_server_set)

        self._cbc_tscrunch_sensor = Sensor.integer(
            "coherent-beam-tscrunch",
            description="The number time samples that will be integrated when producing coherent beams",
            default=self._default_sb_config['coherent-beams-tscrunch'],
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_tscrunch_sensor)

        self._cbc_fscrunch_sensor = Sensor.integer(
            "coherent-beam-fscrunch",
            description="The number frequency channels that will be integrated when producing coherent beams",
            default=self._default_sb_config['coherent-beams-fscrunch'],
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_fscrunch_sensor)

        self._cbc_nchans_per_partition_sensor = Sensor.integer(
            "coherent-beam-subband-nchans",
            description="The output number of frequency channels per partition for coherent beam data",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_nchans_per_partition_sensor)

        self._cbc_samples_per_heap_sensor = Sensor.integer(
            "coherent-beam-samples-per-heap",
            description="The output number of time samples per coherent beam heap",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_samples_per_heap_sensor)

        self._cbc_tsamp_sensor = Sensor.float(
            "coherent-beam-time-resolution",
            description="The time resolution of output coherent beam data",
            default=0.0,
            unit="s",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_tsamp_sensor)

        self._cbc_antennas_sensor = Sensor.string(
            "coherent-beam-antennas",
            description="The antennas that will be used when producing coherent beams",
            default=self._default_sb_config['coherent-beams-antennas'],
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_antennas_sensor)

        self._cbc_mcast_groups_sensor = Sensor.string(
            "coherent-beam-multicast-groups",
            description="Multicast groups used by this instance for sending coherent beam data",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_mcast_groups_sensor)

        self._cbc_mcast_groups_mapping_sensor = Sensor.string(
            "coherent-beam-multicast-group-mapping",
            description="Mapping of mutlicast group address to the coherent beams in that group",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_mcast_groups_mapping_sensor)

        self._cbc_mcast_group_data_rate_sensor = Sensor.float(
            "coherent-beam-multicast-groups-data-rate",
            description="The data rate in each coherent beam multicast group",
            default=0.0,
            unit="bits/s",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_mcast_group_data_rate_sensor)

        self._cbc_heap_size_sensor = Sensor.integer(
            "coherent-beam-heap-size",
            description="The coherent beam heap size in bytes",
            default=8192,
            unit="bytes",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._cbc_heap_size_sensor)

        self._cbc_idx1_step_sensor = Sensor.integer(
            "coherent-beam-idx1-step",
            description="The timestamp step between successive heap groups in coherent beam output data",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._cbc_idx1_step_sensor)

        self._cbc_data_suspect = Sensor.boolean(
            "coherent-beam-data-suspect",
            description="Indicates when data from the coherent beam is expected to be invalid",
            default=True,
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._cbc_data_suspect)

        self._ibc_nbeams_sensor = Sensor.integer(
            "incoherent-beam-count",
            description="The number of incoherent beams that this FBF instance can currently produce",
            default=1,
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._ibc_nbeams_sensor)

        self._ibc_tscrunch_sensor = Sensor.integer(
            "incoherent-beam-tscrunch",
            description="The number time samples that will be integrated when producing incoherent beams",
            default=self._default_sb_config['incoherent-beam-tscrunch'],
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ibc_tscrunch_sensor)

        self._ibc_fscrunch_sensor = Sensor.integer(
            "incoherent-beam-fscrunch",
            description="The number frequency channels that will be integrated when producing incoherent beams",
            default=self._default_sb_config['incoherent-beam-fscrunch'],
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ibc_fscrunch_sensor)

        self._ibc_nchans_per_partition_sensor = Sensor.integer(
            "incoherent-beam-subband-nchans",
            description="The output number of frequency channels per partition for incoherent beam data",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ibc_nchans_per_partition_sensor)

        self._ibc_samples_per_heap_sensor = Sensor.integer(
            "incoherent-beam-samples-per-heap",
            description="The output number of time samples per incoherent beam heap",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ibc_samples_per_heap_sensor)

        self._ibc_tsamp_sensor = Sensor.float(
            "incoherent-beam-time-resolution",
            description="The time resolution of output incoherent beam data",
            default=0.0,
            unit="s",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ibc_tsamp_sensor)

        self._ibc_antennas_sensor = Sensor.string(
            "incoherent-beam-antennas",
            description="The antennas that will be used when producing incoherent beams",
            default=self._default_sb_config['incoherent-beam-antennas'],
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ibc_antennas_sensor)

        self._ibc_mcast_group_sensor = Sensor.string(
            "incoherent-beam-multicast-group",
            description="Multicast group used by this instance for sending incoherent beam data",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ibc_mcast_group_sensor)

        self._ibc_mcast_group_data_rate_sensor = Sensor.float(
            "incoherent-beam-multicast-group-data-rate",
            description="The data rate in the incoherent beam multicast group",
            default=0.0,
            unit="bits/s",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ibc_mcast_group_data_rate_sensor)

        self._ibc_heap_size_sensor = Sensor.integer(
            "incoherent-beam-heap-size",
            description="The incoherent beam heap size in bytes",
            default=8192,
            unit="bytes",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._ibc_heap_size_sensor)

        self._ibc_idx1_step_sensor = Sensor.integer(
            "incoherent-beam-idx1-step",
            description="The timestamp step between successive heap groups in incoherent beam output data",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ibc_idx1_step_sensor)

        self._ibc_data_suspect = Sensor.boolean(
            "incoherent-beam-data-suspect",
            description="Indicates when data from the incoherent beam is expected to be invalid",
            default=True,
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._ibc_data_suspect)

        self._servers_sensor = Sensor.string(
            "servers",
            description="The worker server instances currently allocated to this product",
            default=",".join(["{s.hostname}:{s.port}".format(
                s=server) for server in self._servers]),
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._servers_sensor)

        self._nserver_sets_sensor = Sensor.integer(
            "nserver-sets",
            description="The number of server sets (independent subscriptions to the F-engines)",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nserver_sets_sensor)

        self._nservers_per_set_sensor = Sensor.integer(
            "nservers-per-set",
            description="The number of servers per server set",
            default=1,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nservers_per_set_sensor)

        self._delay_config_server_sensor = Sensor.string(
            "delay-config-server",
            description="The address of the delay configuration server for this product",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._delay_config_server_sensor)

        self._psf_png_sensor = Sensor.string(
            "psf_PNG",
            description="The PSF of the boresight beam for the coherent setup",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._psf_png_sensor)
        self._parent.mass_inform(Message.inform('interface-changed'))

    def teardown_sensors(self):
        """
        @brief    Remove all sensors created by this product from the parent server.

        @note     This method is required for cleanup to stop the FBF sensor pool
                  becoming swamped with unused sensors.
        """
        for sensor in self._managed_sensors:
            self._parent.remove_sensor(sensor)
        self._managed_sensors = []
        self._parent.mass_inform(Message.inform('interface-changed'))

    def teardown_beam_sensors(self):
        """
        @brief    Remove all beam sensors created by this product from the parent server.

        """
        for sensor in self._beam_sensors:
            self._parent.remove_sensor(sensor)
            self._managed_sensors.remove(sensor)
        self._beam_sensors = []
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

    def _verify_antennas(self, antennas):
        """
        @brief      Verify that a set of antennas is available to this instance.

        @param      antennas   A CSV list of antenna names
        """
        self.log.debug("Verifying antenna set: {}".format(antennas))
        antennas_set = set([ant.name for ant in self._katpoint_antennas])
        requested_antennas = set(antennas)
        return requested_antennas.issubset(antennas_set)

    def set_configuration_authority(self, hostname, port):
        """
        @brief      Set the CA for the product

        @param   hostname   The hostname or IP for the CA
        @param   port       The port the CA serves on
        """
        if self._ca_client:
            self._ca_client.stop()
        self._ca_client = KATCPClientResource(dict(
            name='configuration-authority-client',
            address=(hostname, port),
            controlled=True))
        self._ca_client.start()
        self._ca_address_sensor.set_value("{}:{}".format(hostname, port))

    @coroutine
    def get_sb_configuration(self, sb_id):
        if self._ca_client:
            config = yield self.get_ca_sb_configuration(sb_id)
        else:
            self.log.warning(
                "No configuration authority found, "
                "using default configuration parameters")
            config = self._default_sb_config
        raise Return(config)

    @coroutine
    def get_ca_sb_configuration(self, sb_id):
        self.log.debug(("Retrieving schedule block configuration"
                        " from configuration authority"))
        yield self._ca_client.until_synced(timeout=20.0)
        try:
            response = yield self._ca_client.req.get_schedule_block_configuration(self._product_id, sb_id, self._n_channels)
        except Exception as error:
            self.log.error(
                "Request for SB configuration to CA failed with error: {}".format(str(error)))
            raise error
        try:
            config_dict = json.loads(response.reply.arguments[1])
        except Exception as error:
            self.log.error(
                "Could not parse CA SB configuration with error: {}".format(str(error)))
            raise error
        self.log.debug(
            "Configuration authority returned: {}".format(config_dict))
        raise Return(config_dict)

    @coroutine
    def reset_sb_configuration(self):
        self.log.debug("Reseting schedule block configuration")
        self._previous_sb_config = None
        try:
            self.capture_stop()
        except Exception as error:
            self.log.warning(
                "Received error while attempting capture stop: {}".format(
                    str(error)))
        futures = []
        for server in self._servers:
            futures.append(server.deconfigure(timeout=30.0))
        for ii, future in enumerate(futures):
            try:
                yield future
            except Exception as error:
                log.exception(
                    "Unable to deconfigure server {}: {}".format(
                        self._servers[ii], str(error)))
        self._parent._server_pool.deallocate(self._servers)
        self._servers = []
        if self._ibc_mcast_group:
            self._parent._ip_pool.free(self._ibc_mcast_group)
        if self._cbc_mcast_groups:
            self._parent._ip_pool.free(self._cbc_mcast_groups)
        self._cbc_mcast_groups = None
        self._ibc_mcast_group = None
        if self._delay_config_server:
            self._delay_config_server.stop()
            self._delay_config_server = None
        self._beam_manager = None
        try:
            self._parent._feng_subscription_manager.unsubscribe(
                self._product_id)
        except KeyError:
            pass
        except Exception as error:
            log.warning("Could not unsubscribe F-eng mappings: {}".format(
                str(error)))

    @coroutine
    def set_error_state(self, message):
        yield self.reset_sb_configuration()
        self._state_sensor.set_value(self.ERROR)

    def _get_valid_antennas(self, antennas):
        antennas_set = set(antennas)
        subarray_set = set([ant.name for ant in self._katpoint_antennas])
        valid_set = subarray_set.intersection(antennas_set)
        diff_set = antennas_set.difference(valid_set)
        if len(diff_set) > 0:
            log.warning("Not all requested antennas available in subarray.")
            log.warning("Dropping antennas: {}".format(list(diff_set)))
        return sorted(list(valid_set))

    def _sanitise_coh_beam_ants(self, requested_antennas):
        antennas = self._get_valid_antennas(
            parse_csv_antennas(requested_antennas))
        remainder = len(antennas) % COH_ANTENNA_GRANULARITY
        if remainder != 0:
            log.warning(
                "Requested a non-mulitple-of-{} number of "
                "antennas for the coherent beam".format(
                    COH_ANTENNA_GRANULARITY))
            log.warning(
                "Dropping antennas {} from the coherent beam".format(
                    antennas[-remainder:])
                )
            antennas = antennas[:len(antennas) - remainder]
        if len(antennas) == 0:
            raise Exception("After sanitising coherent beam antennas, "
                            "no valid antennas remain")
        log.info("Sanitised coherent antennas: {}".format(antennas))
        return ",".join(antennas)

    def _sanitise_incoh_beam_ants(self, requested_antennas):
        antennas = self._get_valid_antennas(
            parse_csv_antennas(requested_antennas))
        log.info("Sanitised incoherent antennas: {}".format(antennas))
        return ",".join(antennas)

    @coroutine
    def set_sb_configuration(self, config_dict):
        """
        @brief  Set the schedule block configuration for this product

        @param  config_dict  A dictionary specifying configuation parameters, e.g.
                             @code
                                   {
                                   u'coherent-beams-nbeams':100,
                                   u'coherent-beams-tscrunch':22,
                                   u'coherent-beams-fscrunch':2,
                                   u'coherent-beams-antennas':'m007',
                                   u'coherent-beams-granularity':6,
                                   u'incoherent-beam-tscrunch':16,
                                   u'incoherent-beam-fscrunch':1,
                                   u'incoherent-beam-antennas':'m008'
                                   }
                             @endcode

        @detail Valid parameters for the configuration dictionary are as follows:

                 coherent-beams-nbeams      - The desired number of coherent beams to produce
                 coherent-beams-tscrunch    - The number of spectra to integrate in the coherent beamformer
                 coherent-beams-tscrunch    - The number of spectra to integrate in the coherent beamformer
                 coherent-beams-fscrunch    - The number of channels to integrate in the coherent beamformer
                 coherent-beams-antennas    - The specific antennas to use for the coherent beamformer
                 coherent-beams-granularity - The number of beams per output mutlicast group
                                              (an integer divisor or multiplier of this number will be used)
                 incoherent-beam-tscrunch   - The number of spectra to integrate in the incoherent beamformer
                 incoherent-beam-fscrunch   - The number of channels to integrate in the incoherent beamformer
                 incoherent-beam-antennas   - The specific antennas to use for the incoherent beamformer
                 centre-frequency           - The desired centre frequency in Hz
                 bandwidth                  - The desired bandwidth in Hz

        @note   FBFUSE reasonably assumes that the user does not know the possible configurations at
                any given time. As such it tries to satisfy the users request but will not throw an
                error if the requested configuration is not acheivable, instead opting to provide a
                reduced configuration. For example the user may request 1000 beams and 6 beams per
                multicast group but FBFUSE may configure to produce 860 beams and 24 beams per multicast
                group. If the user can only use 6 beams per multcast group, then in the 24-beam case
                they must subscribe to the same multicast group 4 times on different nodes.

        """
        self.log.info("Setting schedule block configuration")
        config = deepcopy(self._default_sb_config)
        config.update(config_dict)
        self.log.info("Configuring using: {}".format(config))
        config['coherent-beams-antennas'] = self._sanitise_coh_beam_ants(
            config['coherent-beams-antennas'])
        config['incoherent-beam-antennas'] = self._sanitise_incoh_beam_ants(
            config['incoherent-beam-antennas'])
        requested_cbc_antenna = parse_csv_antennas(
            config['coherent-beams-antennas'])
        requested_nantennas = len(requested_cbc_antenna)
        if not self._verify_antennas(requested_cbc_antenna):
            raise Exception("Requested coherent beam antennas are not a "
                            "subset of the available antennas")
        requested_ibc_antenna = parse_csv_antennas(
            config['incoherent-beam-antennas'])
        if not self._verify_antennas(requested_ibc_antenna):
            raise Exception("Requested incoherent beam antennas are not a"
                            " subset of the available antennas")
        # first we need to get one ip address for the incoherent beam
        self._ibc_mcast_group = self._parent._ip_pool.allocate(1)
        self._ibc_mcast_group_sensor.set_value(
            self._ibc_mcast_group.format_katcp())
        largest_ip_range = self._parent._ip_pool.largest_free_range()
        nworkers_available = self._parent._server_pool.navailable()
        cm = FbfConfigurationManager(
            len(self._katpoint_antennas), self._feng_config['bandwidth'],
            self._n_channels, nworkers_available, largest_ip_range)
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
        self._cbc_nbeams_per_group.set_value(
            mcast_config['num_beams_per_mcast_group'])
        self._cbc_ngroups.set_value(mcast_config['num_mcast_groups'])
        self._cbc_nbeams_per_server_set.set_value(
            mcast_config['num_beams_per_worker_set'])
        self._cbc_mcast_group_data_rate_sensor.set_value(
            mcast_config['data_rate_per_group'])
        self._cbc_tscrunch_sensor.set_value(config['coherent-beams-tscrunch'])
        self._cbc_fscrunch_sensor.set_value(config['coherent-beams-fscrunch'])
        self._cbc_antennas_sensor.set_value(config['coherent-beams-antennas'])
        self._ibc_tscrunch_sensor.set_value(config['incoherent-beam-tscrunch'])
        self._ibc_fscrunch_sensor.set_value(config['incoherent-beam-fscrunch'])
        self._ibc_antennas_sensor.set_value(config['incoherent-beam-antennas'])

        # Below are some convenience calculations
        coh_heap_size = 8192
        nsamps_per_coh_heap = (coh_heap_size / (cm.nchans_per_worker
                               * config['coherent-beams-fscrunch']))
        coh_timestamp_step = (config['coherent-beams-tscrunch']
                              * nsamps_per_coh_heap
                              * 2 * self._n_channels)
        coh_tsamp = (config['coherent-beams-tscrunch'] * self._n_channels
                     / self._feng_config['bandwidth'])
        self._cbc_nchans_per_partition_sensor.set_value(
            cm.nchans_per_worker / config['coherent-beams-fscrunch'])
        self._cbc_samples_per_heap_sensor.set_value(nsamps_per_coh_heap)
        self._cbc_tsamp_sensor.set_value(coh_tsamp)
        self._cbc_heap_size_sensor.set_value(coh_heap_size)
        self._cbc_idx1_step_sensor.set_value(coh_timestamp_step)

        incoh_heap_size = 8192
        nsamps_per_incoh_heap = (incoh_heap_size / (cm.nchans_per_worker
                                 * config['incoherent-beam-fscrunch']))
        incoh_timestamp_step = (config['incoherent-beam-tscrunch']
                                * nsamps_per_incoh_heap
                                * 2 * self._n_channels)
        incoh_tsamp = (config['incoherent-beam-tscrunch'] * self._n_channels
                       / self._feng_config['bandwidth'])
        self._ibc_nchans_per_partition_sensor.set_value(
            cm.nchans_per_worker / config['incoherent-beam-fscrunch'])
        self._ibc_samples_per_heap_sensor.set_value(nsamps_per_incoh_heap)
        self._ibc_tsamp_sensor.set_value(incoh_tsamp)
        self._ibc_heap_size_sensor.set_value(incoh_heap_size)
        self._ibc_idx1_step_sensor.set_value(incoh_timestamp_step)

        # This doesn't really belong here
        ibc_group_rate = (
            mcast_config['used_bandwidth'] / config['incoherent-beam-tscrunch']
            / config['coherent-beams-fscrunch'] * 8)
        self._ibc_mcast_group_data_rate_sensor.set_value(ibc_group_rate)
        self._servers = self._parent._server_pool.allocate(
            min(nworkers_available, mcast_config['num_workers_total']))
        self._servers = sorted(
            self._servers, key=lambda server: server.hostname)
        server_str = ",".join(["{s.hostname}:{s.port}".format(
            s=server) for server in self._servers])
        self._servers_sensor.set_value(server_str)
        self._nserver_sets_sensor.set_value(mcast_config['num_worker_sets'])
        self._nservers_per_set_sensor.set_value(
            mcast_config['num_workers_per_set'])
        self._cbc_mcast_groups = self._parent._ip_pool.allocate(
            mcast_config['num_mcast_groups'])
        self._cbc_mcast_groups_sensor.set_value(
            self._cbc_mcast_groups.format_katcp())
        self._current_configuration = cm
        raise Return(cm)

    @coroutine
    def get_ca_target_configuration(self, boresight_target):

        @coroutine
        def wait_for_track(callback):
            self.log.debug("Waiting for subarray to enter 'track' state")
            try:
                yield self._activity_tracker.wait_until(
                    "track", self._activity_tracker_interrupt)
            except SubarrayActivityInterrupt:
                self.log.debug("Interrupting callback waiting on 'track' state")
                pass
            except Exception as error:
                log.error(
                    "Unknown error while waiting on telescope to enter 'track' state: {}".format(
                        str(error)))
            else:
                callback()

        def ca_target_update_callback(received_timestamp, timestamp, status,
                                      value):
            # TODO, should we really reset all the beams or should we have
            # a mechanism to only update changed beams
            config_dict = json.loads(value)
            self._cbc_data_suspect.set_value(True)
            self.reset_beams()
            for target_string in config_dict.get('beams', []):
                target = Target(target_string)
                try:
                    self.add_beam(target)
                except BeamAllocationError as error:
                    log.warning(str(error))
            for tiling in config_dict.get('tilings', []):
                target = Target(tiling['target'])  # required
                freq = float(tiling.get('reference_frequency',
                                        self._cfreq_sensor.value()))
                nbeams = int(tiling['nbeams'])
                overlap = float(tiling.get('overlap', 0.5))
                epoch = float(tiling.get('epoch', time.time()))
                self.add_tiling(target, nbeams, freq, overlap, epoch)
            self._parent.ioloop.add_callback(
                wait_for_track,
                lambda: self._cbc_data_suspect.set_value(False))

        # Here we interrupt any active wait_for_track coroutines
        self._activity_tracker_interrupt.set()

        # Here we generate a plot from the PSF
        yield self._ca_client.until_synced(timeout=10.0)
        sensor_name = "{}_beam_position_configuration".format(self._product_id)
        if sensor_name in self._ca_client.sensor:
            self._ca_client.sensor[sensor_name].clear_listeners()
        try:
            response = yield self._ca_client.req.target_configuration_start(
                self._product_id, boresight_target.format_katcp())
        except Exception as error:
            self.log.error(("Request for target configuration to CA "
                            "failed with error: {}").format(str(error)))
            raise error
        if not response.reply.reply_ok():
            error = Exception(response.reply.arguments[1])
            self.log.error(("Request for target configuration to CA "
                            "failed with error: {}").format(str(error)))
            raise error
        yield self._ca_client.until_synced(timeout=10.0)

        # Clear the state on the interrupt event for wait_for_track coroutines
        self._activity_tracker_interrupt.clear()

        sensor = self._ca_client.sensor[sensor_name]
        sensor.register_listener(ca_target_update_callback)
        self._ca_client.set_sampling_strategy(sensor.name, "event")
        self._parent.ioloop.add_callback(
            wait_for_track,
            lambda: self._ibc_data_suspect.set_value(False))

    def _beam_to_sensor_string(self, beam):
        return beam.target.format_katcp()

    def _make_mcast_to_beam_map(self):
        mcast_to_beam_map = {}
        groups = [ip for ip in self._cbc_mcast_groups]
        idxs = [beam.idx for beam in self._beam_manager.get_beams()]
        for group in groups:
            self.log.debug("Allocating beams to {}".format(str(group)))
            key = "spead://{}:{}".format(str(group), self._cbc_mcast_groups.port)
            for _ in range(self._cbc_nbeams_per_group.value()):
                if key not in mcast_to_beam_map:
                    mcast_to_beam_map[key] = []
                value = idxs.pop(0)
                self.log.debug(
                    "--> Allocated {} to {}".format(value, key))
                mcast_to_beam_map[key].append(value)
        return mcast_to_beam_map

    def _create_beam_sensors(self):
        for beam in self._beam_manager.get_beams():
            sensor = Sensor.string(
                "coherent-beam-{}".format(beam.idx),
                description=("R.A. (deg), declination (deg) and source name "
                             "for coherent beam with ID {}").format(beam.idx),
                default=self._beam_to_sensor_string(beam),
                initial_status=Sensor.UNKNOWN)
            beam.register_observer(lambda beam, sensor=sensor:
                                   sensor.set_value(
                                    self._beam_to_sensor_string(beam)))
            self._beam_sensors.append(sensor)
            self.add_sensor(sensor)
        self._parent.mass_inform(Message.inform('interface-changed'))

    @coroutine
    def _make_beam_plot(self, target):
        try:
            png = self._beam_manager.generate_psf_png(
                target,
                self._cfreq_sensor.value(), time.time())
            self._psf_png_sensor.set_value(base64.b64encode(png))
        except Exception as error:
            log.exception("Unable to generate beamshape image: {}".format(
                error))

    @coroutine
    def target_start(self, target):
        self._ibc_data_suspect.set_value(True)
        self._cbc_data_suspect.set_value(True)
        self._phase_reference_sensor.set_value(target.format_katcp())
        self._delay_config_server._phase_reference_sensor.set_value(
            target.format_katcp())
        self._parent.ioloop.add_callback(self._make_beam_plot, target)
        if self._ca_client:
            self._parent.ioloop.add_callback(
                self.get_ca_target_configuration, target)
        else:
            self.log.warning("No configuration authority is set, "
                             "using default beam configuration")

    @coroutine
    def prepare(self, sb_id):
        """
        @brief      Prepare the beamformer for streaming

        @detail     This method evaluates the current configuration creates a new DelayEngine
                    and passes a prepare call to all allocated servers.
        """
        self.log.info("Preparing FBFUSE product")
        self._state_sensor.set_value(self.PREPARING)
        self.log.debug("Product moved to 'preparing' state")
        self._server_configs = {}
        try:
            sb_config = yield self.get_sb_configuration(sb_id)
        except Exception as error:
            log.exception("Failed to get SB configuration: {}".format(
                error))
            self.log.debug("Returning product to 'idle' state")
            self._state_sensor.set_value(self.IDLE)
            raise error

        if sb_config == self._previous_sb_config:
            self.log.info(("Configuration is unchanged, proceeding "
                           "with current configuration"))
            self._state_sensor.set_value(self.READY)
            self.log.info("Successfully prepared FBFUSE product")
            raise Return(None)

        # deallocate all multicast IPs and servers
        yield self.reset_sb_configuration()

        # Allocate IP addresses and servers
        cm = yield self.set_sb_configuration(sb_config)

        # Start the beam manager
        cbc_antennas_names = parse_csv_antennas(
            self._cbc_antennas_sensor.value())
        cbc_antennas = [self._antenna_map[name] for name in cbc_antennas_names]
        self._beam_manager = BeamManager(
            self._cbc_nbeams_sensor.value(), cbc_antennas)

        # Start the delay configuration server
        # This server provides the antenna set, beam positions and
        # phase reference for all worker servers
        self._delay_config_server = DelayConfigurationServer(
            self._parent.bind_address[0], 0, self._beam_manager)
        self._delay_config_server.start()
        self.log.info("Started delay engine at: {}".format(
            self._delay_config_server.bind_address))
        de_ip, de_port = self._delay_config_server.bind_address
        self._delay_config_server_sensor.set_value((de_ip, de_port))

        # Need to tear down the beam sensors here
        # Here calculate the beam to multicast map
        self.teardown_beam_sensors()
        self._create_beam_sensors()
        mcast_to_beam_map = self._make_mcast_to_beam_map()
        self._cbc_mcast_groups_mapping_sensor.set_value(
            json.dumps(mcast_to_beam_map))

        # Here we actually start to prepare the remote workers
        ip_splits = self._streams.split(N_FENG_STREAMS_PER_WORKER)

        # This is assuming lower sideband and bandwidth is always +ve
        fbottom = (self._feng_config['centre-frequency']
                   - self._feng_config['bandwidth'] / 2.)

        coherent_beam_config = {
            'tscrunch': self._cbc_tscrunch_sensor.value(),
            'fscrunch': self._cbc_fscrunch_sensor.value(),
            'antennas': self._cbc_antennas_sensor.value(),
            'nbeams': self._cbc_nbeams_sensor.value(),
            'destination': self._cbc_mcast_groups.format_katcp()
        }

        incoherent_beam_config = {
            'tscrunch': self._ibc_tscrunch_sensor.value(),
            'fscrunch': self._ibc_fscrunch_sensor.value(),
            'antennas': self._ibc_antennas_sensor.value(),
            'destination': self._ibc_mcast_group.format_katcp()
        }

        # Here can choose band priority based on number of available servers

        # Here we create a mapping of nodes to multicast groups
        manager = self._parent._feng_subscription_manager
        try:
            manager.unsubscribe(self._product_id)
        except KeyError:
            pass
        mapping, unused_servers, unused_ip_slits = manager.subscribe(
            ip_splits, self._servers, self._product_id)
        for server in unused_servers:
            log.warning("Worker {} is unused and will be deallocated".format(
                server))
            self._servers.remove(server)
        self._parent._server_pool.deallocate(unused_servers)
        server_str = ",".join(["{s.hostname}:{s.port}".format(
            s=server) for server in self._servers])
        self._servers_sensor.set_value(server_str)
        for ip_split in unused_ip_slits:
            log.warning(
                "Unable to allocate partition: {}".format(
                    ip_split.format_katcp()))

        prepare_futures = []
        for ii, (server, ip_range) in enumerate(mapping):
            group_start = struct.unpack("B", ip_range.base_ip.packed[-1])[0]
            chan0_idx = cm.nchans_per_group * group_start
            chan0_freq = fbottom + chan0_idx * cm.channel_bandwidth

            args = (
                ip_range.format_katcp(),
                cm.nchans_per_group,
                chan0_idx,
                chan0_freq,
                cm.channel_bandwidth,
                json.dumps(self._feng_config),
                json.dumps(coherent_beam_config),
                json.dumps(incoherent_beam_config),
                de_ip,
                de_port)
            self._server_configs[server] = args
            future = server.prepare(*args, timeout=120.0)
            prepare_futures.append(future)

        failure_count = 0
        for ii, future in enumerate(prepare_futures):
            try:
                yield future
            except Exception as error:
                log.error(
                    "Failed to configure server {} with error: {}".format(
                        self._servers[ii], str(error)))
                failure_count += 1

        if (failure_count == len(self._servers)) and not (len(self._servers) == 0):
            self._state_sensor.set_value(self.ERROR)
            self.log.info("Failed to prepare FBFUSE product")
        else:
            self._previous_sb_config = sb_config
            self._state_sensor.set_value(self.READY)
            self.log.info("Successfully prepared FBFUSE product")
        yield self.set_levels(20.0, 4.0)
        yield self._activity_tracker.start()

    @coroutine
    def deconfigure(self):
        """
        @brief  Deconfigure the product. To be called on a subarray deconfigure.

        @detail This is the final cleanup operation for the product, it should delete all sensors
                and ensure the release of all resource allocations.
        """
        try:
            yield self.reset_sb_configuration()
        except Exception as error:
            if self._servers:
                log.error("Warning servers are still allocated to this"
                          " product that cannot be freed for future use.")
            raise error
        self.teardown_sensors()
        self._state_sensor.set_value(self.IDLE)

    @coroutine
    def set_levels(self, input_level, output_level):
        if not self.ready:
            raise FbfProductStateError([self.READY], self.state)
        futures = []
        for server in self._servers:
            futures.append(server.set_levels(
                input_level, output_level))
        for ii, future in enumerate(futures):
            try:
                yield future
            except Exception as error:
                log.exception(
                    "Error when setting levels on server {}: {}".format(
                        self._servers[ii], str(error)))

    @coroutine
    def capture_start(self):
        if not self.ready:
            raise FbfProductStateError([self.READY], self.state)
        self._state_sensor.set_value(self.STARTING)
        self.log.debug("Product moved to 'starting' state")
        futures = []
        for server in self._servers:
            futures.append(server.capture_start())
        for ii, future in enumerate(futures):
            try:
                yield future
            except Exception as error:
                log.exception(
                    "Error when calling capture_start on server {}: {}".format(
                        self._servers[ii], str(error)))
                # What should be done with this server? Pop it from the list?
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
            raise Return(None)
        self._state_sensor.set_value(self.STOPPING)
        self.target_stop()
        futures = []
        for server in self._servers:
            futures.append(server.capture_stop(timeout=60.0))
        for ii, future in enumerate(futures):
            try:
                yield future
            except Exception as error:
                log.exception(
                    "Error when calling capture_stop on server {}: {}".format(
                        self._servers[ii], str(error)))
        self._state_sensor.set_value(self.READY)

    @coroutine
    def target_stop(self):
        if self._ca_client:
            sensor_name = "{}_beam_position_configuration".format(
                self._product_id)
            self._ca_client.set_sampling_strategy(sensor_name, "none")
        self._ibc_data_suspect.set_value(True)
        self._cbc_data_suspect.set_value(True)

    def add_beam(self, target):
        """
        @brief      Specify the parameters of one managed beam

        @param      target      A KATPOINT target object

        @return     Returns the allocated Beam object
        """
        valid_states = [self.READY, self.CAPTURING, self.STARTING]
        if self.state not in valid_states:
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
        if self.state not in valid_states:
            raise FbfProductStateError(valid_states, self.state)
        tiling = self._beam_manager.add_tiling(
            target, number_of_beams, reference_frequency, overlap)
        try:
            tiling.generate(epoch)
        except Exception as error:
            self.log.exception(
                "Failed to generate tiling pattern with error: {}".format(
                    str(error)))
        return tiling

    def reset_beams(self):
        """
        @brief  reset and deallocate all beams and tilings managed by this instance

        @note   All tiling will be lost on this call and must be remade for subsequent observations
        """
        valid_states = [self.READY, self.CAPTURING, self.STARTING]
        if self.state not in valid_states:
            raise FbfProductStateError(valid_states, self.state)
        self._beam_manager.reset()
