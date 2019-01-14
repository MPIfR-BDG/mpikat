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
import tornado
import signal
import time
from subprocess import Popen, PIPE, check_call
from optparse import OptionParser
from tornado.gen import coroutine, Return, sleep
from katcp import Sensor, AsyncDeviceServer, AsyncReply, KATCPClientResource
from katcp.kattypes import request, return_reply, Int, Str, Discrete, Float

from mpikat.core.ip_manager import ip_range_from_stream
from mpikat.core.utils import LoggingSensor, parse_csv_antennas
from mpikat.meerkat.fbfuse import DelayBufferController
from mpikat.meerkat.fbfuse.fbfuse_mkrecv_config import make_mkrecv_header

log = logging.getLogger("mpikat.fbfuse_worker_server")

class FbfWorkerServer(AsyncDeviceServer):
    VERSION_INFO = ("fbf-control-server-api", 0, 1)
    BUILD_INFO = ("fbf-control-server-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]
    STATES = ["idle", "preparing", "ready", "starting", "capturing", "stopping", "error"]
    IDLE, PREPARING, READY, STARTING, CAPTURING, STOPPING, ERROR = STATES

    def __init__(self, ip, port, dummy=False):
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
        self._delay_client = None
        self._delays = None
        self._dummy = dummy
        self._dada_input_key = 0xdada
        self._dada_coh_output_key = 0xcaca
        self._dada_incoh_output_key = 0xbaba
        super(FbfWorkerServer, self).__init__(ip,port)

    @coroutine
    def start(self):
        """Start FbfWorkerServer server"""
        super(FbfWorkerServer,self).start()

    @coroutine
    def stop(self):
        yield self.deregister()
        yield super(FbfWorkerServer,self).stop()

    def setup_sensors(self):
        """
        @brief    Set up monitoring sensors.

        Sensor list:
        - device-status
        - local-time-synced
        - fbf0-status
        - fbf1-status

        @note     The following sensors are made available on top of default sensors
                  implemented in AsynDeviceServer and its base classes.

                  device-status:      Reports the health status of the FBFUSE and associated devices:
                                      Among other things report HW failure, SW failure and observation failure.
        """
        self._device_status_sensor = Sensor.discrete(
            "device-status",
            description = "Health status of FbfWorkerServer instance",
            params = self.DEVICE_STATUSES,
            default = "ok",
            initial_status = Sensor.NOMINAL)
        self.add_sensor(self._device_status_sensor)

        self._state_sensor = LoggingSensor.discrete(
            "state",
            params = self.STATES,
            description = "The current state of this worker instance",
            default = self.IDLE,
            initial_status = Sensor.NOMINAL)
        self._state_sensor.set_logger(log)
        self.add_sensor(self._state_sensor)

        self._delay_client_sensor = Sensor.string(
            "delay-engine-server",
            description = "The address of the currently set delay engine",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._delay_client_sensor)

        self._antenna_capture_order_sensor = Sensor.string(
            "antenna-capture-order",
            description = "The order in which the worker will capture antennas internally",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._antenna_capture_order_sensor)

        self._mkrecv_header_sensor = Sensor.string(
            "mkrecv-header",
            description = "The MKRECV/DADA header used for configuring capture with MKRECV",
            default = "",
            initial_status = Sensor.UNKNOWN)
        self.add_sensor(self._mkrecv_header_sensor)

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
        if self._dummy:
            log.debug("Server is running in dummy mode, system call will be ignored")
        else:
            check_call(cmd)

    def _determine_feng_capture_order(self, antenna_to_feng_id_map, coherent_beam_config, incoherent_beam_config):
        # Need to sort the f-engine IDs into 4 states
        # 1. Incoherent but not coherent
        # 2. Incoherent and coherent
        # 3. Coherent but not incoherent
        # 4. Neither coherent nor incoherent
        #
        # We must catch all antennas as even in case 4 the data is required for the
        # transient buffer.
        #
        # To make this split, we first create the three sets, coherent, incoherent and all.
        mapping = antenna_to_feng_id_map
        all_feng_ids = set(mapping.values())
        coherent_feng_ids = set(mapping[antenna] for antenna in parse_csv_antennas(coherent_beam_config['antennas']))
        incoherent_feng_ids = set(mapping[antenna] for antenna in parse_csv_antennas(incoherent_beam_config['antennas']))
        incoh_not_coh = incoherent_feng_ids.difference(coherent_feng_ids)
        incoh_and_coh = incoherent_feng_ids.intersection(coherent_feng_ids)
        coh_not_incoh = coherent_feng_ids.difference(incoherent_feng_ids)
        used_fengs = incoh_not_coh.union(incoh_and_coh).union(coh_not_incoh)
        unused_fengs = all_feng_ids.difference(used_fengs)
        # Output final order
        final_order = list(incoh_not_coh) + list(incoh_and_coh) + list(coh_not_incoh) + list(unused_fengs)
        start_of_incoherent_fengs = 0
        end_of_incoherent_fengs = len(incoh_not_coh) + len(incoh_and_coh)
        start_of_coherent_fengs = len(incoh_not_coh)
        end_of_coherent_fengs = len(incoh_not_coh) + len(incoh_and_coh) + len(coh_not_incoh)
        start_of_unused_fengs = end_of_coherent_fengs
        end_of_unused_fengs = len(all_feng_ids)
        info = {
            "order": final_order,
            "incoherent_span":(start_of_incoherent_fengs, end_of_incoherent_fengs),
            "coherent_span":(start_of_coherent_fengs, end_of_coherent_fengs),
            "unused_span":(start_of_unused_fengs, end_of_unused_fengs)
        }
        return info

    @request(Str(), Int(), Int(), Float(), Float(), Str(), Str(), Str(), Str(), Str(), Int())
    @return_reply()
    def request_prepare(self, req, feng_groups, nchans_per_group, chan0_idx, chan0_freq,
                        chan_bw, mcast_to_beam_map, feng_config, coherent_beam_config,
                        incoherent_beam_config, dc_ip, dc_port):
        """
        @brief      Prepare FBFUSE to receive and process data from a subarray

        @detail     REQUEST ?configure feng_groups, nchans_per_group, chan0_idx, chan0_freq,
                        chan_bw, mcast_to_beam_map, antenna_to_feng_id_map, coherent_beam_config,
                        incoherent_beam_config
                    Configure FBFUSE for the particular data products

        @param      req                 A katcp request object

        @param      feng_groups         The contiguous range of multicast groups to capture F-engine data from,
                                        the parameter is formatted in stream notation, e.g.: spead://239.11.1.150+3:7147

        @param      nchans_per_group    The number of frequency channels per multicast group

        @param      chan0_idx           The index of the first channel in the set of multicast groups

        @param      chan0_freq          The frequency in Hz of the first channel in the set of multicast groups

        @param      chan_bw             The channel bandwidth in Hz

        @param      mcast_to_beam_map   A JSON mapping between output multicast addresses and beam IDs. This is the sole
                                        authority for the number of beams that will be produced and their indexes. The map
                                        is in the form:

                                        @code
                                           {
                                              "spead://239.11.2.150:7147":"cfbf00001,cfbf00002,cfbf00003,cfbf00004",
                                              "spead://239.11.2.151:7147":"ifbf00001"
                                           }

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
                                                'antennas':'m007,m008,m009'
                                              }
                                           @endcode

        @param      incoherent_beam_config  A JSON object specifying the incoherent beam configuration in the form:

                                           @code
                                              {
                                                'tscrunch':16,
                                                'fscrunch':1,
                                                'antennas':'m007,m008,m009'
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
            return ("fail", "Unable to parse F-eng config with error: {}".format(str(error)))
        try:
            mcast_to_beam_map = json.loads(mcast_to_beam_map)
        except Exception as error:
            return ("fail", "Unable to parse multicast beam mapping with error: {}".format(str(error)))
        try:
            coherent_beam_config = json.loads(coherent_beam_config)
        except Exception as error:
            return ("fail", "Unable to parse coherent beam config with error: {}".format(str(error)))
        try:
            incoherent_beam_config = json.loads(incoherent_beam_config)
        except Exception as error:
            return ("fail", "Unable to parse incoherent beam config with error: {}".format(str(error)))

        @coroutine
        def configure():
            self._state_sensor.set_value(self.PREPARING)
            log.debug("Starting delay configuration server client")
            self._delay_client = KATCPClientResource(dict(
                name="delay-configuration-client",
                address=(dc_ip, dc_port),
                controlled=True))
            self._delay_client.start()

            log.debug("Determining F-engine capture order")
            feng_capture_order_info = self._determine_feng_capture_order(feng_config['feng-antenna-map'], coherent_beam_config,
                incoherent_beam_config)
            log.debug("Capture order info: {}".format(feng_capture_order_info))
            feng_to_antenna_map = {value:key for key,value in feng_config['feng-antenna-map'].items()}
            antenna_capture_order_csv = ",".join([feng_to_antenna_map[feng_id] for feng_id in feng_capture_order_info['order']])
            self._antenna_capture_order_sensor.set_value(antenna_capture_order_csv)

            log.debug("Parsing F-engines to capture: {}".format(feng_groups))
            capture_range = ip_range_from_stream(feng_groups)
            ngroups = capture_range.count
            partition_nchans = nchans_per_group * ngroups
            partition_bandwidth = partition_nchans * chan_bw
            npol = 2
            ndim = 2
            nbits = 8
            tsamp = 1.0 / (feng_config['bandwidth'] / feng_config['nchans'])
            sample_clock = feng_config['bandwidth'] * 2
            timestamp_step =  feng_config['nchans'] * 2 * 256 # WARNING: This is only valid in 4k mode
            frequency_ids = [chan0_idx+nchans_per_group*ii for ii in range(ngroups)] #WARNING: Assumes contigous groups
            mkrecv_config = {
                'frequency_mhz': (chan0_freq + feng_config['nchans']/2.0 * chan_bw) / 1e6,
                'bandwidth': partition_bandwidth,
                'tsamp_us': tsamp * 1e6,
                'bytes_per_second': partition_bandwidth * npol * ndim * nbits,
                'nchan': partition_nchans,
                'dada_key': self._dada_input_key,
                'nantennas': len(feng_capture_order_info['order']),
                'antennas_csv': antenna_capture_order_csv,
                'sync_epoch': feng_config['sync-epoch'],
                'sample_clock': sample_clock,
                'mcast_sources': ",".join([str(group) for group in capture_range]),
                'mcast_port': capture_range.port,
                'interface': "192.168.0.1",
                'timestamp_step': timestamp_step,
                'ordered_feng_ids_csv': ",".join(map(str, feng_capture_order_info['order'])),
                'frequency_partition_ids_csv': ",".join(map(str,frequency_ids))
            }
            mkrecv_header = make_mkrecv_header(mkrecv_config)
            self._mkrecv_header_sensor.set_value(mkrecv_header)
            log.info("Determined MKRECV configuration:\n{}".format(mkrecv_header))


            log.debug("Parsing beam to multicast mapping")
            incoherent_beam = None
            incoherent_beam_group = None
            coherent_beam_to_group_map = {}
            for group, beams in mcast_to_beam_map.items():
                for beam in beams.split(","):
                    if beam.startswith("cfbf"):
                        coherent_beam_to_group_map[beam] = group
                    if beam.startswith("ifbf"):
                        incoherent_beam = beam
                        incoherent_beam_group = group

            log.debug("Determined coherent beam to multicast mapping: {}".format(coherent_beam_to_group_map))
            if incoherent_beam:
                log.debug("Incoherent beam will be sent to: {}".format(incoherent_beam_group))
            else:
                log.debug("No incoherent beam specified")


            """
            Tasks:
                - compile kernels
                - create shared memory banks
            """
            # Compile beamformer
            # TBD

            # Need to come up with a good way to allocate keys for dada buffers

            # Create input DADA buffer
            log.debug("Creating dada buffer for input with key '{}'".format("%x"%self._dada_input_key))
            #self._system_call_wrapper(["dada_db","-k",self._dada_input_key,"-n","64","-l","-p"])

            # Create coherent beam output DADA buffer
            log.debug("Creating dada buffer for coherent beam output with key '{}'".format("%x"%self._dada_coh_output_key))
            #self._system_call_wrapper(["dada_db","-k",self._dada_coh_output_key,"-n","64","-l","-p"])

            # Create incoherent beam output DADA buffer
            log.debug("Creating dada buffer for incoherent beam output with key '{}'".format("%x"%self._dada_incoh_output_key))
            #self._system_call_wrapper(["dada_db","-k",self._dada_incoh_output_key,"-n","64","-l","-p"])

            # Create SPEAD transmitter for coherent beams
            # Call to MKSEND

            # Create SPEAD transmitter for incoherent beam
            # Call to MKSEND

            # Need to pass the delay buffer controller the F-engine capture order but only for the coherent beams
            cstart, cend = feng_capture_order_info['coherent_span']
            coherent_beam_feng_capture_order = feng_capture_order_info['order'][cstart:cend]
            coherent_beam_antenna_capture_order = [feng_to_antenna_map[idx] for idx in coherent_beam_feng_capture_order]


            # Start DelayBufferController instance
            # Here we are going to make the assumption that the server and processing all run in
            # one docker container that will be preallocated with the right CPU set, GPUs, memory
            # etc. This means that the configurations need to be unique by NUMA node... [Note: no
            # they don't, we can use the container IPC channel which isolates the IPC namespaces.]
            if not self._dummy:
                n_coherent_beams = len(coherent_beam_to_group_map)
                coherent_beam_antennas = parse_csv_antennas(coherent_beam_config['antennas'])
                self._delay_buffer_controller = DelayBufferController(self._delay_client,
                    coherent_beam_to_group_map.keys(),
                    coherent_beam_antenna_capture_order, 1)
                yield self._delay_buffer_controller.start()
            # Start beamformer instance
            # TBD

            # Define MKRECV configuration file

            # SPEAD receiver does not get started until a capture init call
            self._state_sensor.set_value(self.READY)
            req.reply("ok",)

        self.ioloop.add_callback(configure)
        raise AsyncReply

    @request(Str())
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

        @coroutine
        def deconfigure():
            log.info("Destroying dada buffer for input with key '{}'".format(self._dada_input_key))
            self._system_call_wrapper(["dada_db","-k",self._dada_input_key,"-d"])
            log.info("Destroying dada buffer for coherent beam output with key '{}'".format(self._dada_coh_output_key))
            self._system_call_wrapper(["dada_db","-k",self._dada_coh_output_key,"-n","64","-l","-p"])
            log.info("Destroying dada buffer for incoherent beam output with key '{}'".format(self._dada_incoh_output_key))
            self._system_call_wrapper(["dada_db","-k",self._dada_coh_output_key,"-n","64","-l","-p"])
            log.info("Destroying delay buffer controller")
            del self._delay_buffer_controller
            self._delay_buffer_controller = None
            req.reply("ok",)

        self.ioloop.add_callback(deconfigure)
        raise AsyncReply

    @request(Str())
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
        if not self.ready:
            return ("fail", "FBF worker not in READY state")
        # Here we start MKRECV running into the input dada buffer
        self._mkrecv_ingest_proc = Popen(["mkrecv","--config",self._mkrecv_config_filename], stdout=PIPE, stderr=PIPE)
        return ("ok",)

    @request(Str())
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
        if not self.capturing and not self.error:
            return ("ok",)

        @coroutine
        def stop_mkrecv_capture():
            #send SIGTERM to MKRECV
            log.info("Sending SIGTERM to MKRECV process")
            self._mkrecv_ingest_proc.terminate()
            self._mkrecv_timeout = 10.0
            log.info("Waiting {} seconds for MKRECV to terminate...".format(self._mkrecv_timeout))
            now = time.time()
            while time.time()-now < self._mkrecv_timeout:
                retval = self._mkrecv_ingest_proc.poll()
                if retval is not None:
                    log.info("MKRECV returned a return value of {}".format(retval))
                    break
                else:
                    yield sleep(0.5)
            else:
                log.warning("MKRECV failed to terminate in alloted time")
                log.info("Killing MKRECV process")
                self._mkrecv_ingest_proc.kill()
            req.reply("ok",)
        self.ioloop.add_callback(self.stop_mkrecv_capture)
        raise AsyncReply


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
    parser.add_option('-p', '--port', dest='port', type=long,
        help='Port number to bind to')
    parser.add_option('', '--log_level',dest='log_level',type=str,
        help='Port number of status server instance',default="INFO")
    parser.add_option('', '--dummy',action="store_true", dest='dummy',
        help='Set status server to dummy')
    parser.add_option('-n', '--nodes',dest='nodes', type=str, default=None,
        help='Path to file containing list of available nodes')
    (opts, args) = parser.parse_args()
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('mpikat.fbfuse_worker_server')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(opts.log_level.upper())
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting FbfWorkerServer instance")

    server = FbfWorkerServer(opts.host, opts.port, dummy=opts.dummy)
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(server.bind_address))
    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()