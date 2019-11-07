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
import os
import coloredlogs
from subprocess import Popen, PIPE, check_call
from optparse import OptionParser
from tornado.gen import coroutine
from tornado.ioloop import IOLoop, PeriodicCallback
from katcp import Sensor, AsyncDeviceServer, AsyncReply, KATCPClientResource
from katcp.kattypes import request, return_reply, Int, Str, Float
from mpikat.core.ip_manager import ip_range_from_stream
from mpikat.core.utils import LoggingSensor, parse_csv_antennas, lcm
from mpikat.meerkat.fbfuse import DelayBufferController
from mpikat.meerkat.fbfuse.fbfuse_mkrecv_config import (
    make_mkrecv_header, MkrecvStdoutHandler)
from mpikat.meerkat.fbfuse.fbfuse_mksend_config import (
    make_mksend_header)
from mpikat.meerkat.fbfuse.fbfuse_psrdada_cpp_wrapper import (
    compile_psrdada_cpp)
from mpikat.utils.process_tools import process_watcher, ManagedProcess
from mpikat.utils.db_monitor import DbMonitor

log = logging.getLogger("mpikat.fbfuse_worker_server")

PACKET_PAYLOAD_SIZE = 1024  # bytes
AVAILABLE_CAPTURE_MEMORY = 137438953472/2  # bytes
MAX_DADA_BLOCK_SIZE = 1e9  # bytes

MKRECV_CONFIG_FILENAME = "mkrecv_feng.cfg"
MKSEND_COHERENT_CONFIG_FILENAME = "mksend_coherent.cfg"
MKSEND_INCOHERENT_CONFIG_FILENAME = "mksend_incoherent.cfg"

EXEC_MODES = ["dryrun", "output_only", "input_output", "full"]
DRYRUN, OUTPUT_ONLY, INPUT_OUTPUT, FULL = EXEC_MODES

# For elegantly doing nothing instead of starting a process
DUMMY_CMD = ["tail", "-f", "/dev/null"]


def determine_feng_capture_order(antenna_to_feng_id_map,
                                 coherent_beam_config,
                                 incoherent_beam_config):
    # Need to sort the f-engine IDs into 4 states
    # 1. Incoherent but not coherent
    # 2. Incoherent and coherent
    # 3. Coherent but not incoherent
    # 4. Neither coherent nor incoherent
    #
    # We must catch all antennas as even in case 4 the data is required for the
    # transient buffer.
    #
    # To make this split, we first create the three sets, coherent,
    # incoherent and all.
    mapping = antenna_to_feng_id_map
    all_feng_ids = set(mapping.values())
    coherent_feng_ids = set(mapping[antenna] for antenna in parse_csv_antennas(
        coherent_beam_config['antennas']))
    incoherent_feng_ids = set(mapping[antenna] for antenna in parse_csv_antennas(
        incoherent_beam_config['antennas']))
    incoh_not_coh = incoherent_feng_ids.difference(coherent_feng_ids)
    incoh_and_coh = incoherent_feng_ids.intersection(coherent_feng_ids)
    coh_not_incoh = coherent_feng_ids.difference(incoherent_feng_ids)
    used_fengs = incoh_not_coh.union(incoh_and_coh).union(coh_not_incoh)
    unused_fengs = all_feng_ids.difference(used_fengs)
    # Output final order
    final_order = list(incoh_not_coh) + list(incoh_and_coh) + \
        list(coh_not_incoh) + list(unused_fengs)
    start_of_incoherent_fengs = 0
    end_of_incoherent_fengs = len(incoh_not_coh) + len(incoh_and_coh)
    start_of_coherent_fengs = len(incoh_not_coh)
    end_of_coherent_fengs = len(
        incoh_not_coh) + len(incoh_and_coh) + len(coh_not_incoh)
    start_of_unused_fengs = end_of_coherent_fengs
    end_of_unused_fengs = len(all_feng_ids)
    info = {
        "order": final_order,
        "incoherent_span": (start_of_incoherent_fengs, end_of_incoherent_fengs),
        "coherent_span": (start_of_coherent_fengs, end_of_coherent_fengs),
        "unused_span": (start_of_unused_fengs, end_of_unused_fengs)
    }
    return info


class FbfWorkerServer(AsyncDeviceServer):
    VERSION_INFO = ("fbf-control-server-api", 0, 1)
    BUILD_INFO = ("fbf-control-server-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    STATES = ["idle", "preparing", "ready",
              "starting", "capturing", "stopping", "error"]
    IDLE, PREPARING, READY, STARTING, CAPTURING, STOPPING, ERROR = STATES

    def __init__(self, ip, port, capture_interface, numa_node, exec_mode=FULL):
        """
        @brief       Construct new FbfWorkerServer instance

        @params  ip       The interface address on which the server should listen
        @params  port     The port that the server should bind to
        @params  de_ip    The IP address of the delay engine server
        @params  de_port  The port number for the delay engine server

        """
        self._dc_ip = None
        self._dc_port = None
        self._delay_client = None
        self._delays = None
        self._numa = numa_node
        self._exec_mode = exec_mode
        self._dada_input_key = "dada"
        self._dada_coh_output_key = "caca"
        self._dada_incoh_output_key = "baba"
        self._capture_interface = capture_interface
        self._capture_monitor = None

        self._input_level = 10.0
        self._output_level = 10.0
        self._partition_bandwidth = None
        self._centre_frequency = None

        super(FbfWorkerServer, self).__init__(ip, port)

    @coroutine
    def start(self):
        """Start FbfWorkerServer server"""
        super(FbfWorkerServer, self).start()

    @coroutine
    def stop(self):
        yield super(FbfWorkerServer, self).stop()

    def setup_sensors(self):
        """
        @brief    Set up monitoring sensors.

        Sensor list:
        - device-status
        - local-time-synced
        - fbf0-status
        - fbf1-status

        @note     The following sensors are made available on top of default
                  sensors implemented in AsynDeviceServer and its base classes.

                  device-status:      Reports the health status of the FBFUSE
                                      and associated devices:
                                      Among other things report HW failure, SW
                                      failure and observation failure.
        """
        self._device_status_sensor = Sensor.discrete(
            "device-status",
            description="Health status of FbfWorkerServer instance",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._device_status_sensor)

        self._state_sensor = LoggingSensor.discrete(
            "state",
            params=self.STATES,
            description="The current state of this worker instance",
            default=self.IDLE,
            initial_status=Sensor.NOMINAL)
        self._state_sensor.set_logger(log)
        self.add_sensor(self._state_sensor)

        self._capture_interface_sensor = Sensor.string(
            "capture-interface",
            description="The IP address of the NIC to be used for data capture",
            default=self._capture_interface,
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._capture_interface_sensor)

        self._delay_client_sensor = Sensor.string(
            "delay-engine-server",
            description="The address of the currently set delay engine",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._delay_client_sensor)

        self._antenna_capture_order_sensor = Sensor.string(
            "antenna-capture-order",
            description="The order in which the worker will capture antennas internally",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._antenna_capture_order_sensor)

        self._mkrecv_header_sensor = Sensor.string(
            "mkrecv-capture-header",
            description="The MKRECV/DADA header used for configuring capture with MKRECV",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._mkrecv_header_sensor)

        self._mksend_coh_header_sensor = Sensor.string(
            "mksend-coherent-beam-header",
            description="The MKSEND/DADA header used for configuring transmission of coherent beam data",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._mksend_coh_header_sensor)

        self._mksend_incoh_header_sensor = Sensor.string(
            "mksend-incoherent-beam-header",
            description="The MKSEND/DADA header used for configuring transmission of incoherent beam data",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._mksend_incoh_header_sensor)

        self._psrdada_cpp_args_sensor = Sensor.string(
            "psrdada-cpp-arguments",
            description="The command line arguments used to invoke psrdada_cpp",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._psrdada_cpp_args_sensor)

        self._mkrecv_heap_loss = Sensor.float(
            "feng-heap-loss",
            description=("The percentage if F-engine heaps lost "
                         "(within MKRECV statistics window)"),
            default=0.0,
            initial_status=Sensor.UNKNOWN,
            unit="%")
        self.add_sensor(self._mkrecv_heap_loss)

        self._ingress_buffer_percentage = Sensor.float(
            "ingress-buffer-fill-level",
            description=("The percentage fill level for the capture"
                         "buffer between MKRECV and PSRDADA_CPP"),
            default=0.0,
            initial_status=Sensor.UNKNOWN,
            unit="%")
        self.add_sensor(self._ingress_buffer_percentage)

        self._cb_egress_buffer_percentage = Sensor.float(
            "cb-egress-buffer-fill-level",
            description=("The percentage fill level for the transmission"
                         "buffer between PSRDADA_CPP and MKSEND (for "
                         "coherent beams)"),
            default=0.0,
            initial_status=Sensor.UNKNOWN,
            unit="%")
        self.add_sensor(self._cb_egress_buffer_percentage)

        self._ib_egress_buffer_percentage = Sensor.float(
            "ib-egress-buffer-fill-level",
            description=("The percentage fill level for the transmission"
                         "buffer between PSRDADA_CPP and MKSEND (for "
                         "incoherent beams)"),
            default=0.0,
            initial_status=Sensor.UNKNOWN,
            unit="%")
        self.add_sensor(self._ib_egress_buffer_percentage)

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

    def _system_call_wrapper(self, cmd):
        log.debug("System call: '{}'".format(" ".join(cmd)))
        check_call(cmd)

    @coroutine
    def _make_db(self, key, block_size, nblocks, timeout=120):
        try:
            yield self._destroy_db(key, timeout=20)
        except Exception as error:
            log.debug("Could not clean previous buffer (key={}): {}".format(
                key, str(error)))
        log.debug(("Building DADA buffer: key={}, block_size={}, "
                   "nblocks={}").format(key, block_size, nblocks))
        if self._exec_mode == FULL:
            cmdline = map(str, ["dada_db", "-k", key, "-b", block_size, "-n",
                          nblocks, "-l", "-p"])
            proc = Popen(cmdline, stdout=PIPE,
                         stderr=PIPE, shell=False,
                         close_fds=True)
            yield process_watcher(proc, name="make_db({})".format(key),
                                  timeout=timeout)
        else:
            log.warning(("Current execution mode disables "
                         "DADA buffer creation/destruction"))

    @coroutine
    def _destroy_db(self, key, timeout=20.0):
        log.debug("Destroying DADA buffer with key={}".format(key))
        if self._exec_mode == FULL:
            cmdline = map(str, ["dada_db", "-k", key, "-d"])
            proc = Popen(cmdline, stdout=PIPE,
                         stderr=PIPE, shell=False,
                         close_fds=True)
            yield process_watcher(proc, name="destroy_db({})".format(key),
                                  timeout=timeout)
        else:
            log.warning(("Current execution mode disables "
                         "DADA buffer creation/destruction"))

    @coroutine
    def _reset_db(self, key, timeout=5.0):
        log.debug("Resetting DADA buffer with key={}".format(key))
        if self._exec_mode == FULL:
            cmdline = map(str, ["dbreset", "-k", key])
            proc = Popen(cmdline, stdout=PIPE,
                         stderr=PIPE, shell=False,
                         close_fds=True)
            yield process_watcher(proc, name="reset_db({})".format(key),
                                  timeout=timeout)
        else:
            log.warning(("Current execution mode disables "
                         "DADA buffer reset"))

    def set_affinity(self, pid, core_spec):
        log.debug("Setting affinity for PID {} to {}".format(pid, core_spec))
        os.system("taskset -cp {} {}".format(core_spec, pid))

    @request(Float(), Float())
    @return_reply()
    def request_set_levels(self, req, input_level, output_level):
        """
        @brief    Set the input and output levels for FBFUSE

        @param      req             A katcp request object

        @param    input_level  The standard deviation of the data
                               from the F-engines.

        @param    output_level  The standard deviation of the data
                                output from FBFUSE.
        """
        self._input_level = input_level
        self._output_level = output_level
        return ("ok",)

    @request(Str(), Int(), Int(), Float(), Float(), Str(), Str(),
             Str(), Str(), Int())
    @return_reply()
    def request_prepare(self, req, feng_groups, nchans_per_group,
                        chan0_idx, chan0_freq, chan_bw, feng_config,
                        coherent_beam_config, incoherent_beam_config,
                        dc_ip, dc_port):
        """
        @brief      Prepare FBFUSE to receive and process data from a subarray

        @detail     REQUEST ?configure feng_groups, nchans_per_group, chan0_idx, chan0_freq,
                        chan_bw, mcast_to_beam_map, antenna_to_feng_id_map, coherent_beam_config,
                        incoherent_beam_config
                    Configure FBFUSE for the particular data products

        @param      req                 A katcp request object

        @param      feng_groups         The contiguous range of multicast groups to capture F-engine data from,
                                        the parameter is formatted in stream notation, e.g.: spead://239.11.1.150+3:7148

        @param      nchans_per_group    The number of frequency channels per multicast group

        @param      chan0_idx           The index of the first channel in the set of multicast groups

        @param      chan0_freq          The frequency in Hz of the first channel in the set of multicast groups

        @param      chan_bw             The channel bandwidth in Hz

        @param      feng_config    JSON dictionary containing general F-engine parameters.

                                        @code
                                           {
                                              'bandwidth': 856e6,
                                              'centre-frequency': 1200e6,
                                              'sideband': 'upper',
                                              'feng-antenna-map': {...},
                                              'sync-epoch': 12353524243.0,
                                              'nchans': 4096
                                           }

        @param      coherent_beam_config   A JSON object specifying the coherent beam configuration in the form:

                                           @code
                                              {
                                                'tscrunch':16,
                                                'fscrunch':1,
                                                'nbeams': 400,
                                                'antennas':'m007,m008,m009',
                                                'destination': 'spead://239.11.1.0+127:7148'
                                              }
                                           @endcode

        @param      incoherent_beam_config  A JSON object specifying the incoherent beam configuration in the form:

                                           @code
                                              {
                                                'tscrunch':16,
                                                'fscrunch':1,
                                                'antennas':'m007,m008,m009',
                                                'destination': 'spead://239.11.1.150:7148'
                                              }
                                           @endcode

        @return     katcp reply object [[[ !configure ok | (fail [error description]) ]]]
        """
        if not self.idle:
            return ("fail", "FBF worker not in IDLE state")

        log.info("Preparing worker server instance")
        try:
            feng_config = json.loads(feng_config)
        except Exception as error:
            msg = ("Unable to parse F-eng config with "
                   "error: {}").format(str(error))
            log.error("Prepare failed: {}".format(msg))
            return ("fail", msg)
        log.info("F-eng config: {}".format(feng_config))
        try:
            coherent_beam_config = json.loads(coherent_beam_config)
        except Exception as error:
            msg = ("Unable to parse coherent beam "
                   "config with error: {}").format(str(error))
            log.error("Prepare failed: {}".format(msg))
            return ("fail", msg)
        log.info("Coherent beam config: {}".format(coherent_beam_config))
        try:
            incoherent_beam_config = json.loads(incoherent_beam_config)
        except Exception as error:
            msg = ("Unable to parse incoherent beam "
                   "config with error: {}").format(str(error))
            log.error("Prepare failed: {}".format(msg))
            return ("fail", msg)
        log.info("Incoherent beam config: {}".format(incoherent_beam_config))

        @coroutine
        def configure():
            self._state_sensor.set_value(self.PREPARING)
            log.debug("Starting delay configuration server client")
            self._delay_client = KATCPClientResource(dict(
                name="delay-configuration-client",
                address=(dc_ip, dc_port),
                controlled=True))
            self._delay_client.start()

            log.info("Determining F-engine capture order")
            feng_capture_order_info = determine_feng_capture_order(
                feng_config['feng-antenna-map'], coherent_beam_config,
                incoherent_beam_config)
            log.info("F-engine capture order info: {}".format(feng_capture_order_info))
            feng_to_antenna_map = {
                value: key for key, value in
                feng_config['feng-antenna-map'].items()}
            antenna_capture_order_csv = ",".join(
                [feng_to_antenna_map[feng_id] for feng_id in
                 feng_capture_order_info['order']])
            self._antenna_capture_order_sensor.set_value(
                antenna_capture_order_csv)

            log.debug("Parsing F-engines to capture: {}".format(feng_groups))
            capture_range = ip_range_from_stream(feng_groups)
            ngroups = capture_range.count
            partition_nchans = nchans_per_group * ngroups
            worker_idx = chan0_idx / partition_nchans
            partition_bandwidth = partition_nchans * chan_bw
            self._partition_bandwidth = partition_bandwidth
            sample_clock = feng_config['bandwidth'] * 2
            timestamp_step = feng_config['nchans'] * 2 * 256
            frequency_ids = [chan0_idx + nchans_per_group *
                             ii for ii in range(ngroups)]
            nantennas = len(feng_capture_order_info['order'])
            heap_size = nchans_per_group * PACKET_PAYLOAD_SIZE
            heap_group_size = ngroups * heap_size * nantennas
            ngroups_data = int(MAX_DADA_BLOCK_SIZE / heap_group_size)
            ngroups_data = 2**((ngroups_data-1).bit_length())
            centre_frequency = chan0_freq + self._partition_bandwidth / 2.0
            self._centre_frequency = centre_frequency

            # Coherent beam timestamps
            coh_heap_size = 8192
            nsamps_per_coh_heap = (coh_heap_size / (partition_nchans
                                   * coherent_beam_config['fscrunch']))
            coh_timestamp_step = (coherent_beam_config['tscrunch']
                                  * nsamps_per_coh_heap
                                  * 2 * feng_config["nchans"])

            # Incoherent beam timestamps
            incoh_heap_size = 8192
            nsamps_per_incoh_heap = (incoh_heap_size / (partition_nchans
                                     * incoherent_beam_config['fscrunch']))
            incoh_timestamp_step = (incoherent_beam_config['tscrunch']
                                    * nsamps_per_incoh_heap * 2
                                    * feng_config["nchans"])

            timestamp_modulus = lcm(timestamp_step,
                                    lcm(incoh_timestamp_step,
                                        coh_timestamp_step)) * 4

            if self._exec_mode == FULL:
                dada_mode = 4
            else:
                dada_mode = 0
            mkrecv_config = {
                'dada_mode': dada_mode,
                'dada_key': self._dada_input_key,
                'sync_epoch': feng_config['sync-epoch'],
                'sample_clock': sample_clock,
                'mcast_sources': ",".join(
                    [str(group) for group in capture_range]),
                'mcast_port': capture_range.port,
                'interface': self._capture_interface,
                'timestamp_step': timestamp_step,
                'timestamp_modulus': timestamp_modulus,
                'ordered_feng_ids_csv': ",".join(
                    map(str, feng_capture_order_info['order'])),
                'frequency_partition_ids_csv': ",".join(
                    map(str, frequency_ids)),
                'ngroups_data': ngroups_data,
                'heap_size': heap_size
            }
            mkrecv_header = make_mkrecv_header(
                mkrecv_config, outfile=MKRECV_CONFIG_FILENAME)
            self._mkrecv_header_sensor.set_value(mkrecv_header)
            log.info("Determined MKRECV configuration:\n{}".format(
                mkrecv_header))

            coh_ip_range = ip_range_from_stream(
                coherent_beam_config['destination'])
            nbeams = coherent_beam_config['nbeams']
            nbeams_per_group = nbeams / coh_ip_range.count
            msg = "nbeams is not a mutliple of the IP range"
            assert nbeams % coh_ip_range.count == 0, msg
            """
            Note on data rates:
            For both the coherent and incoherent beams, we set the sending
            rate in MKSEND equal to 110% of the required data rate. This
            is a fudge to ensure that we send rapidly enough that MKSEND
            does not limit performance while at the same time ensuring that
            the burst rate out of the instrument is limited. This number
            may need to be tuned.
            """
            coh_data_rate = (partition_bandwidth
                             / coherent_beam_config['tscrunch']
                             / coherent_beam_config['fscrunch']
                             * nbeams_per_group * 1.1)
            heap_id_start = worker_idx * coh_ip_range.count
            log.debug("Determining MKSEND configuration for coherent beams")
            dada_mode = int(self._exec_mode == FULL)
            coherent_mcast_dest = coherent_beam_config['destination'].lstrip(
                "spead://").split(":")[0]
            mksend_coh_config = {
                'dada_key': self._dada_coh_output_key,
                'dada_mode': dada_mode,
                'interface': self._capture_interface,
                'data_rate': coh_data_rate,
                'mcast_port': coh_ip_range.port,
                'mcast_destinations': coherent_mcast_dest,
                'sync_epoch': feng_config['sync-epoch'],
                'sample_clock': sample_clock,
                'heap_size': coh_heap_size,
                'heap_id_start': heap_id_start,
                'timestamp_step': coh_timestamp_step,
                'beam_ids': "0:{}".format(nbeams),
                'multibeam': True,
                'subband_idx': chan0_idx,
                'heap_group': nbeams_per_group
            }
            mksend_coh_header = make_mksend_header(
                mksend_coh_config, outfile=MKSEND_COHERENT_CONFIG_FILENAME)
            log.info(("Determined MKSEND configuration for coherent beams:\n{}"
                      ).format(mksend_coh_header))
            self._mksend_coh_header_sensor.set_value(mksend_coh_header)

            log.debug("Determining MKSEND configuration for incoherent beams")
            incoh_data_rate = (
                partition_bandwidth / incoherent_beam_config['tscrunch']
                / incoherent_beam_config['fscrunch'] * 1.1)
            dada_mode = int(self._exec_mode == FULL)
            incoh_ip_range = ip_range_from_stream(
                incoherent_beam_config['destination'])
            coherent_mcast_dest = incoherent_beam_config['destination'].lstrip(
                "spead://").split(":")[0]
            mksend_incoh_config = {
                'dada_key': self._dada_incoh_output_key,
                'dada_mode': dada_mode,
                'interface': self._capture_interface,
                'data_rate': incoh_data_rate,
                'mcast_port': incoh_ip_range.port,
                'mcast_destinations': coherent_mcast_dest,
                'sync_epoch': feng_config['sync-epoch'],
                'sample_clock': sample_clock,
                'heap_size': incoh_heap_size,
                'heap_id_start': worker_idx,
                'timestamp_step': incoh_timestamp_step,
                'beam_ids': 0,
                'multibeam': False,
                'subband_idx': chan0_idx,
                'heap_group': 1
            }
            mksend_incoh_header = make_mksend_header(
                mksend_incoh_config, outfile=MKSEND_INCOHERENT_CONFIG_FILENAME)
            log.info("Determined MKSEND configuration for incoherent beam:\n{}".format(
                mksend_incoh_header))
            self._mksend_incoh_header_sensor.set_value(mksend_incoh_header)

            """
            Tasks:
                - compile kernels
                - create shared memory banks
            """
            # Here we create a future object for the psrdada_cpp compilation
            # this is the longest running setup task and so intermediate steps
            # such as dada buffer generation
            fbfuse_pipeline_params = {
                'total_nantennas': len(feng_capture_order_info['order']),
                'fbfuse_nchans': partition_nchans,
                'total_nchans': feng_config['nchans'],
                'coherent_tscrunch': coherent_beam_config['tscrunch'],
                'coherent_fscrunch': coherent_beam_config['fscrunch'],
                'coherent_nantennas': len(coherent_beam_config['antennas'].split(",")),
                'coherent_antenna_offset': feng_capture_order_info["coherent_span"][0],
                'coherent_nbeams': nbeams,
                'incoherent_tscrunch': incoherent_beam_config['tscrunch'],
                'incoherent_fscrunch': incoherent_beam_config['fscrunch']
            }
            psrdada_compilation_future = compile_psrdada_cpp(
                fbfuse_pipeline_params)

            log.info("Creating all DADA buffers")
            # Create capture data DADA buffer
            capture_block_size = ngroups_data * heap_group_size
            capture_block_count = int(AVAILABLE_CAPTURE_MEMORY
                                      / capture_block_size)
            log.debug("Creating dada buffer for input with key '{}'".format(
                "%s" % self._dada_input_key))
            input_make_db_future = self._make_db(
                self._dada_input_key, capture_block_size,
                capture_block_count)

            # Create coherent beam output DADA buffer
            coh_output_channels = (ngroups * nchans_per_group) / \
                coherent_beam_config['fscrunch']
            coh_output_samples = ngroups_data * \
                256 / coherent_beam_config['tscrunch']
            coherent_block_size = (nbeams * coh_output_channels
                                   * coh_output_samples)
            coherent_block_count = 32
            log.debug(("Creating dada buffer for coherent beam output "
                       "with key '{}'").format(
                       "%s" % self._dada_coh_output_key))
            coh_output_make_db_future = self._make_db(
                self._dada_coh_output_key, coherent_block_size,
                coherent_block_count)

            # Create incoherent beam output DADA buffer
            incoh_output_channels = ((ngroups * nchans_per_group)
                                     / incoherent_beam_config['fscrunch'])
            incoh_output_samples = ((ngroups_data * 256)
                                    / incoherent_beam_config['tscrunch'])
            incoherent_block_size = incoh_output_channels * incoh_output_samples
            incoherent_block_count = 32
            log.debug(("Creating dada buffer for incoherent beam "
                       "output with key '{}'").format(
                       "%s" % self._dada_incoh_output_key))
            incoh_output_make_db_future = self._make_db(
                self._dada_incoh_output_key, incoherent_block_size,
                incoherent_block_count)

            # Need to pass the delay buffer controller the F-engine capture
            # order but only for the coherent beams
            cstart, cend = feng_capture_order_info['coherent_span']
            coherent_beam_feng_capture_order = feng_capture_order_info[
                'order'][cstart:cend]
            coherent_beam_antenna_capture_order = [feng_to_antenna_map[
                idx] for idx in coherent_beam_feng_capture_order]

            # Start DelayBufferController instance
            # Here we are going to make the assumption that the server and processing all run in
            # one docker container that will be preallocated with the right CPU set, GPUs, memory
            # etc. This means that the configurations need to be unique by NUMA node... [Note: no
            # they don't, we can use the container IPC channel which isolates
            # the IPC namespaces.]
            #
            # Here we recreate the beam keys as they are handled by the BeamManager
            # instance in the product controller
            #
            beam_idxs = ["cfbf%05d" % (i) for i in range(nbeams)]
            self._delay_buf_ctrl = DelayBufferController(
                self._delay_client, beam_idxs,
                coherent_beam_antenna_capture_order, 1)
            yield self._delay_buf_ctrl.start()

            # By this point we require psrdada_cpp to have been compiled
            # as such we can yield on the future we created earlier
            yield psrdada_compilation_future

            # Now we can yield on dada buffer generation
            yield input_make_db_future
            yield coh_output_make_db_future
            yield incoh_output_make_db_future
            self._state_sensor.set_value(self.READY)
            log.info("Prepare request successful")
            req.reply("ok",)

        @coroutine
        def safe_configure():
            try:
                yield configure()
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))

        self.ioloop.add_callback(safe_configure)
        raise AsyncReply

    @request()
    @return_reply()
    def request_deconfigure(self, req):
        """
        @brief      Deconfigure the FBFUSE instance.

        @note       Deconfigure the FBFUSE instance. If FBFUSE uses katportalclient to get information
                    from CAM, then it should disconnect at this time.

        @param      req               A katcp request object

        @return     katcp reply object [[[ !deconfigure ok | (fail [error description]) ]]]
        """

        # Need to make sure everything is stopped
        # Call self.stop?

        # Need to delete all allocated DADA buffers:
        log.info("Received deconfigure request")
        @coroutine
        def deconfigure():
            log.info("Destroying allocated DADA buffers")
            try:
                yield self._destroy_db(self._dada_input_key)
                yield self._destroy_db(self._dada_coh_output_key)
                yield self._destroy_db(self._dada_incoh_output_key)
            except Exception as error:
                log.warning("Error while destroying DADA buffers: {}".format(
                    str(error)))
            log.info("Destroying delay buffers")
            del self._delay_buf_ctrl
            self._delay_buf_ctrl = None
            self._state_sensor.set_value(self.IDLE)
            log.info("Deconfigure request successful")
            req.reply("ok",)
        self.ioloop.add_callback(deconfigure)
        raise AsyncReply

    @request()
    @return_reply()
    def request_capture_start(self, req):
        """
        @brief      Prepare FBFUSE ingest process for data capture.

        @note       A successful return value indicates that FBFUSE is ready for data capture and
                    has sufficient resources available. An error will indicate that FBFUSE is not
                    in a position to accept data

        @param      req               A katcp request object


        @return     katcp reply object [[[ !capture-init ok | (fail [error description]) ]]]
        """
        log.info("Received capture-start request")
        try:
            self.capture_start()
        except Exception as error:
            log.exception("Error during capture start")
            return ("fail", str(error))
        else:
            log.info("Capture-start successful")
            return ("ok",)

    def capture_start(self):
        if not self.ready:
            raise Exception("FBF worker not in READY state")
        self._state_sensor.set_value(self.STARTING)
        # Create SPEAD transmitter for coherent beams

        if self._numa == 0:
            mksend_cpu_set = "7"
            psrdada_cpp_cpu_set = "6"
            mkrecv_cpu_set = "0-5"
        else:
            mksend_cpu_set = "14"
            psrdada_cpp_cpu_set = "15"
            mkrecv_cpu_set = "8-13"

        self._mksend_coh_proc = ManagedProcess(
            ["taskset", "-c", mksend_cpu_set, "mksend", "--header",
             MKSEND_COHERENT_CONFIG_FILENAME, "--quiet"])

        self._mksend_incoh_proc = ManagedProcess(
            ["taskset", "-c", mksend_cpu_set, "mksend", "--header",
             MKSEND_INCOHERENT_CONFIG_FILENAME, "--quiet"])

        # Start beamforming pipeline
        log.info("Starting PSRDADA_CPP beamforming pipeline")
        delay_buffer_key = self._delay_buf_ctrl.shared_buffer_key
        # Start beamformer instance
        psrdada_cpp_cmdline = [
            "taskset", "-c", psrdada_cpp_cpu_set,
            "fbfuse",
            "--input_key", self._dada_input_key,
            "--cb_key", self._dada_coh_output_key,
            "--ib_key", self._dada_incoh_output_key,
            "--delay_key_root", delay_buffer_key,
            "--cfreq", self._centre_frequency,
            "--bandwidth", self._partition_bandwidth,
            "--input_level", self._input_level,
            "--output_level", self._output_level,
            "--log_level", "info"]
        self._psrdada_cpp_args_sensor.set_value(
            " ".join(map(str, psrdada_cpp_cmdline)))
        log.debug(" ".join(map(str, psrdada_cpp_cmdline)))
        self._psrdada_cpp_proc = ManagedProcess(psrdada_cpp_cmdline)

        def update_heap_loss_sensor(curr, total, avg, window):
            self._mkrecv_heap_loss.set_value(100.0 - avg)

        # Create SPEAD receiver for incoming antenna voltages
        self._mkrecv_proc = ManagedProcess(
            ["taskset", "-c", mkrecv_cpu_set, "mkrecv_nt", "--header",
             MKRECV_CONFIG_FILENAME, "--quiet"],
            stdout_handler=MkrecvStdoutHandler(
                callback=update_heap_loss_sensor))

        def exit_check_callback():
            if not self._mkrecv_proc.is_alive():
                log.error("mkrecv_nt exited unexpectedly")
                self.ioloop.add_callback(self.capture_stop)
            if not self._psrdada_cpp_proc.is_alive():
                log.error("fbfuse pipeline exited unexpectedly")
                self.ioloop.add_callback(self.capture_stop)
            if not self._mksend_coh_proc.is_alive():
                log.error("mksend coherent exited unexpectedly")
                self.ioloop.add_callback(self.capture_stop)
            if not self._mksend_incoh_proc.is_alive():
                log.error("mksend incoherent exited unexpectedly")
                self.ioloop.add_callback(self.capture_stop)
            self._capture_monitor.stop()

        self._capture_monitor = PeriodicCallback(exit_check_callback, 1000)
        self._capture_monitor.start()

        def dada_callback(params):
            self._ingress_buffer_percentage.set_value(params["fraction-full"])

        # start DB monitors
        self._ingress_buffer_monitor = DbMonitor(
            self._dada_input_key,
            callback=dada_callback)
        self._ingress_buffer_monitor.start()
        self._cb_egress_buffer_monitor = DbMonitor(
            self._dada_input_key,
            callback=lambda params:
            self._cb_egress_buffer_percentage.set_value(
                params["fraction-full"]))
        self._cb_egress_buffer_monitor.start()
        self._ib_egress_buffer_monitor = DbMonitor(
            self._dada_input_key,
            callback=lambda params:
            self._ib_egress_buffer_percentage.set_value(
                params["fraction-full"]))
        self._ib_egress_buffer_monitor.start()
        self._state_sensor.set_value(self.CAPTURING)

    @request()
    @return_reply()
    def request_capture_stop(self, req):
        """
        @brief      Terminate the FBFUSE ingest process for the particular FBFUSE instance

        @note       This writes out any remaining metadata, closes all files, terminates any remaining processes and
                    frees resources for the next data capture.

        @param      req               A katcp request object

        @param      product_id        This is a name for the data product, used to track which subarray is being told to stop capture.
                                      For example "array_1_bc856M4k".

        @return     katcp reply object [[[ !capture-done ok | (fail [error description]) ]]]
        """
        log.info("Received capture-stop request")
        @coroutine
        def capture_stop_wrapper():
            try:
                yield self.capture_stop()
            except Exception as error:
                log.exception("Capture-stop request failed")
                req.reply("fail", str(error))
            else:
                log.info("Capture-stop request successful")
                req.reply("ok",)
        self.ioloop.add_callback(capture_stop_wrapper)
        raise AsyncReply

    @coroutine
    def capture_stop(self):
        if not self.capturing and not self.error:
            return
        log.info("Stopping capture")
        self._state_sensor.set_value(self.STOPPING)
        self._capture_monitor.stop()
        self._ingress_buffer_monitor.stop()
        self._cb_egress_buffer_monitor.stop()
        self._ib_egress_buffer_monitor.stop()
        log.info("Stopping MKRECV instance")
        self._mkrecv_proc.terminate()
        log.info("Stopping PSRDADA_CPP instance")
        self._psrdada_cpp_proc.terminate()
        log.info("Stopping MKSEND instances")
        self._mksend_incoh_proc.terminate()
        self._mksend_coh_proc.terminate()
        log.info("Resetting DADA buffers")
        reset_tasks = []
        reset_tasks.append(self._reset_db(
            self._dada_input_key, timeout=7.0))
        reset_tasks.append(self._reset_db(
            self._dada_coh_output_key, timeout=4.0))
        reset_tasks.append(self._reset_db(
            self._dada_incoh_output_key, timeout=5.0))
        for task in reset_tasks:
            try:
                yield task
            except Exception as error:
                log.warning("Error raised on DB reset: {}".format(str(error)))
        self._state_sensor.set_value(self.READY)


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
    parser.add_option('-n', '--numa', dest='numa', type=int,
                      help=('The ID of the current NUMA node '
                            '(used for setting core affinities)'))
    parser.add_option('', '--log_level', dest='log_level', type=str,
                      help='Port number of status server instance',
                      default="INFO")
    parser.add_option('', '--exec_mode', dest='exec_mode', type=str,
                      default="full", help='Set status server to exec_mode')
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
    log.info("Starting FbfWorkerServer instance")
    server = FbfWorkerServer(opts.host, opts.port, opts.cap_ip,
                             opts.numa, exec_mode=opts.exec_mode)
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
