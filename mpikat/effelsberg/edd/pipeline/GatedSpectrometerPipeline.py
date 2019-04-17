"""
Copyright (c) 2019 Tobias Winchen <twinchen@mpifr-bonn.mpg.de>

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
from pipeline_register import register_pipeline
from mpikat.utils.process_tools import ManagedProcess

import tornado
import signal
import coloredlogs
from optparse import OptionParser
#import tempfile
import json
from tornado.gen import Return, coroutine
from tornado.ioloop import PeriodicCallback
import os
import time
from astropy.time import Time
from mpikat.effelsberg.edd.pipeline.dada import render_dada_header, make_dada_key_string
from mpikat.effelsberg.edd.edd_scpi_interface import EddScpiInterface
import shlex
import threading
import base64
from katcp import Sensor, AsyncDeviceServer, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str
import tempfile

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.GatedSpectrometerPipeline")
log.setLevel('DEBUG')

PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "error"]

DEFAULT_CONFIG = {
        "input_bit_depth" : 12,                         # Input bit-depth
        "samples_per_heap": 4096,                       # this needs to be consistent with the mkrecv configuration
        "samples_per_block": 512*1024*1024,             # 512 Mega sampels per buffer block to allow high res  spectra
        "enabled_polarizations" : ["polarization_0"],
        "sample_clock" : 2600000000,
        "sync_time" : 1554915838,

        "fft_length": 1024*1024,
        "naccumulate": 512,
        "output_bit_depth": 32,
        "input_level": 100,
        "output_level": 100,

        "null_output": False,                           # Disable sending of data for testing purposes

        "polarization_0" :
        {
            "ibv_if": "10.10.1.10",
            "mcast_sources": "225.0.0.162 225.0.0.163 225.0.0.164 225.0.0.165",
            "mcast_dest": "225.0.0.182 225.0.0.183",        #two destinations gate on/off
            "port_rx": "7148",
            "port_tx": "7150",
            "dada_key": 'dada',              # output keysa are the reverse!
            "numa_node": 1                   # we only have on ethernet interface on numa node 1
        },
         "polarization_1" :
        {
            "ibv_if": "10.10.1.11",
            "mcast_sources": "225.0.0.166 225.0.0.167 225.0.0.168 225.0.0.169",
            "mcast_dest": "225.0.0.184 225.0.0.185",        #two destinations, one for on, one for off
            "port_rx": "7148",
            "port_tx": "7150",
            "dada_key": 'dadc',
            "numa_node": 1                   # we only have on ethernet interface on numa node 1
        }
    }

# static configuration for mkrec. all items that can be configured are passed
# via cmdline
mkrecv_header = """
## Dada header configuration
HEADER          DADA
HDR_VERSION     1.0
HDR_SIZE        4096
DADA_VERSION    1.0

## MKRECV configuration
PACKET_SIZE         8400
IBV_VECTOR          -1          # IBV forced into polling mode
IBV_MAX_POLL        10

DADA_MODE           4    # The mode, 4=full dada functionality

SAMPLE_CLOCK_START  0 # This is updated with the sync-time of the packetiser to allow for UTC conversion from the sample clock

NTHREADS            32
NHEAPS              64
NGROUPS_TEMP        65536

#SPEAD specifcation for EDD packetiser data stream
NINDICES            1      # Although there is more than one index, we are only receiving one polarisation so only need to specify the time index

# The first index item is the running timestamp
IDX1_ITEM           0      # First item of a SPEAD heap
IDX1_STEP           4096   # The difference between successive timestamps. This is the number of sampels per heap

# Add side item to buffer
SCI_LIST            7
"""

# static configuration for mksend. all items that can be configured are passed
# via cmdline
mksend_header = """
HEADER          DADA
HDR_VERSION     1.0
HDR_SIZE        4096
DADA_VERSION    1.0

# MKSEND CONFIG
NETWORK_MODE  1
PACKET_SIZE 8400
IBV_VECTOR   -1          # IBV forced into polling mode
IBV_MAX_POLL 10

#number of heaps with the same time stamp.
HEAP_COUNT 1
HEAP_ID_START   1
HEAP_ID_OFFSET  1
HEAP_ID_STEP    13

NSCI            0
NITEMS          5
ITEM1_ID        5632    # timestamp, slowest index
ITEM1_INDEX     1

ITEM2_ID        5633    # polarization

ITEM2_ID        5633    # noise diode identifier
ITEM2_LIST      0,1     # on/off value
ITEM2_INDEX     2

ITEM3_ID        5634    # number of channels in dataset

ITEM4_ID        5635
ITEM4_LIST      42,23   # number of samples ndiode on/off - currently dummy data but should be read from buffer in the future
ITEM4_INDEX     2

ITEM5_ID        5636 # payload item (empty step, list, index and sci)
"""


@register_pipeline("GatedSpectrometerPipeline")
class GatedSpectrometerPipeline(AsyncDeviceServer):
    """@brief gated spectrometer pipeline class."""
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]

    CONTROL_MODES = ["KATCP", "SCPI"]
    KATCP, SCPI = CONTROL_MODES

    def __init__(self, ip, port, scpi_ip, scpi_port):
        """@brief initialize the pipeline."""
        self.callbacks = set()
        self._state = "idle"
        self._sensors = []
        self._control_mode = self.KATCP
        self._scpi_ip = scpi_ip
        self._scpi_port = scpi_port
        self._scpi_interface = None
        self._config = None
        self._subprocesses = []
        self._dada_buffers = []
        super(GatedSpectrometerPipeline, self).__init__(ip, port)


    @property
    def sensors(self):
        return self._sensors


    def notify(self):
        """@brief callback function."""
        for callback in self.callbacks:
            callback(self._state, self)


    @property
    def state(self):
        """@brief property of the pipeline state."""
        return self._state


    @state.setter
    def state(self, value):
        self._state = value
        self._pipeline_sensor_status.set_value(self._state)
        self.notify()


    def start(self):
        """
        @brief    Start the server
        """
        super(GatedSpectrometerPipeline, self).start()
        self._scpi_interface = EddScpiInterface(
            self, self._scpi_ip, self._scpi_port, self.ioloop)


    def stop(self):
        """
        @brief    Stop the server
        """
        self._scpi_interface.stop()
        self._scpi_interface = None
        super(GatedSpectrometerPipeline, self).stop()


    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
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
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._edd_scpi_interface_addr_sensor)
        self._device_status = Sensor.discrete(
            "device-status",
            description="Health status of device",
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._device_status)
        self._pipeline_sensor_status = Sensor.discrete(
            "pipeline-status",
            description="Status of the pipeline",
            params=PIPELINE_STATES,
            default="idle",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._pipeline_sensor_status)


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
        if not mode in self.CONTROL_MODES:
            raise UnknownControlMode("Unknown mode '{}', valid modes are '{}' ".format(
                mode, ", ".join(self.CONTROL_MODES)))
        else:
            self._control_mode = mode
        if self._control_mode == self.SCPI:
            self._scpi_interface.start()
        else:
            self._scpi_interface.stop()
        self._control_mode_sensor.set_value(self._control_mode)


    def _decode_capture_stdout(self, stdout, callback):
        log.debug('{}'.format(str(stdout)))


    def _handle_execution_stderr(self, stderr, callback):
        log.info(stderr)


    def _handle_error_state(self, errorstate, caller):
        """
        Sets the error state.
        """
        log.error("Errror handle called. Errorstate = {} from {}".format(errorstate, caller))
        self.state =  "error"


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
    def configure(self, config_json):
        """@brief destroy any ring buffer and create new ring buffer."""
        """
        @brief   Configure the EDD gated spectrometer

        @param   config_json    A JSON dictionary object containing configuration information

        @detail  The configuration dictionary is highly flexible. An example is below:
                 @code
                     {
                         "nbeams": 1,
                         "nchans": 2048,
                         "freq_res": "something in Hz"
                         "integration_time": 1.0,
                         "mc_address": "255.0.0.152+8"
                         "mc_port": 7148
                     }
                 @endcode
        """
        log.info("Configuring EDD backend for processing")
        log.debug("Configuration string: '{}'".format(config_json))
        if self.state != "idle":
            log.error('Cannot configure pipeline. Pipeline state {}.'.format(self.state))
            return

        self.state = "configuring"

        # Merge retrieved config into default via recursive dict merge
        def __updateConfig(oldo, new):
            old = oldo.copy()
            for k in new:
                if isinstance(old[k], dict):
                    old[k] = __updateConfig(old[k], new[k])
                else:
                    old[k] = new[k]
            return old

        if isinstance(config_json, str):
            cfg = json.loads(config_json)
        elif isinstance(config_json, dict):
            cfg = config_json
        else:
            raise RuntimeError("Cannot handle config type {}. Config has to bei either json formatted string or dict!".format(type(config_json)))
        self._config = __updateConfig(DEFAULT_CONFIG, cfg)


        log.info("Received configuration:\n" + json.dumps(self._config, indent=4))

        try:
            self.deconfigure()
        except Exception as error:
            raise RuntimeError(str(error))

        # calculate input buffer parameters
        self.input_heapSize =  self._config["samples_per_heap"] * self._config['input_bit_depth'] / 8
        nHeaps = self._config["samples_per_block"] / self._config["samples_per_heap"]
        input_bufferSize = nHeaps * (self.input_heapSize + 64 / 8)
        log.debug('Input dada parameters created from configuration:\n\
                heap size:        {} byte\n\
                heaps per block:  {}\n\
                buffer size:      {} byte'.format(self.input_heapSize, nHeaps, input_bufferSize))

        # calculate output buffer parameters
        nSlices = self._config["samples_per_block"] / self._config['fft_length'] /  self._config['naccumulate']
        nChannels = self._config['fft_length'] / 2 + 1
        output_bufferSize = nSlices * 2 * nChannels * self._config['output_bit_depth'] / 8

        output_heapSize = nChannels * self._config['output_bit_depth'] / 8
        bufferTime = float(self._config["samples_per_block"])  / self._config["sample_clock"]
        rate = output_heapSize / (bufferTime / nSlices) * 8 # bps
        rate *= 1.10        # set rate to 110% of expected rate

        log.debug('Output parameters calculated from configuration:\n\
                spectra per block:  {} \n\
                nChannels:          {} \n\
                buffer size:        {} byte \n\
                heap size:          {} byte\n\
                rate (110%):        {} bps'.format(nSlices, nChannels, output_bufferSize, output_heapSize, rate))

        def create_ring_buffer(bufferSize, blocks, key):
            cmd = "numactl --cpubind=1 --membind=1 dada_db -k {key} -n {blocks} -b {bufferSize} -p -l".format(key=key, blocks=blocks, bufferSize=bufferSize)
            log.debug("Running command: {0}".format(cmd))
            p = ManagedProcess(cmd)
            while p.is_alive():
                # wait for buffer to be created
                time.sleep(0.1)
            #_create_ring_buffer.stdout_callbacks.add(
            #    self._decode_capture_stdout)
            #_create_ring_buffer._process.wait()

        for i,k in enumerate(self._config['enabled_polarizations']):
            bufferName = self._config[k]['dada_key']
            ofname = bufferName[::-1]
            self._dada_buffers.append(bufferName)
            self._dada_buffers.append(ofname)

            # configure dada buffer
            create_ring_buffer(input_bufferSize, 64, bufferName)
            create_ring_buffer(output_bufferSize, 8, ofname)

            log_level='debug'

            # Configure + launch gated spectrometer
            cmd = "numactl --cpubind=1 --membind=1 gated_spectrometer --nsidechannelitems=1 --input_key={dada_key} --speadheap_size={heapSize} --selected_sidechannel=0 --nbits={input_bit_depth} --fft_length={fft_length} --naccumulate={naccumulate} --input_level={input_level} --output_bit_depth={output_bit_depth} --output_level={output_level} -o {ofname} --log_level={log_level} --output_type=dada".format(dada_key=bufferName, log_level=log_level, ofname=ofname, heapSize=self.input_heapSize, **self._config)
            # here should be a smarter system to parse the options from the
            # controller to the program without redundant typing of options
            log.debug("Command to run: {}".format(cmd))

            if not self._config["null_output"]:
                # Configure output
                gated_cli = ManagedProcess(cmd, env={"CUDA_VISIBLE_DEVICES":str(1)}) #HOTFIX set to numa node 1
                #gated_cli.stdout_callbacks.add( self._decode_capture_stdout)
                #gated_cli.stderr_callbacks.add( self._handle_execution_stderr)
                #gated_cli.error_callbacks.add(self._handle_error_state)
                self._subprocesses.append(gated_cli)

                mksend_header_file = tempfile.NamedTemporaryFile(delete=False)
                mksend_header_file.write(mksend_header)
                mksend_header_file.close()

                cfg = self._config.copy()
                cfg.update(self._config[k])

                cmd = "numactl --cpubind={numa_node} --membind={numa_node} mksend --header {mksend_header} --dada-key {ofname} --ibv-if {ibv_if} --port {port_tx} --sync-epoch {sync_time} --sample-clock {sample_clock} --item1-step {fft_length} --item2-list {polarization} --item3-list {nChannels} --rate {rate} --heap-size {heap_size} {mcast_dest}".format(mksend_header=mksend_header_file.name,
                        ofname=ofname, polarization=i, nChannels=nChannels,
                        rate=rate, heap_size=output_heapSize, **cfg)

                mks = ManagedProcess(cmd)
                #mks.stdout_callbacks.add( self._decode_capture_stdout)
                #mks.stderr_callbacks.add( self._handle_execution_stderr)
                #mks.error_callbacks.add(self._handle_error_state)
                self._subprocesses.append(mks)

        self.state = "ready"


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
    def capture_start(self, config_json=""):
        """@brief start the dspsr instance then turn on dada_junkdb instance."""
        log.info("Starting EDD backend")
        if self.state != "ready":
            log.error("pipleine state is not in state = ready, but in state = {} - cannot start the pipeline".format(self.state))
            return

        self.state = "starting"
        try:
            mkrecvheader_file = tempfile.NamedTemporaryFile(delete=False)
            log.debug("Creating mkrec header file: {}".format(mkrecvheader_file.name))
            mkrecvheader_file.write(mkrecv_header)

            mkrecvheader_file.write("HEAP_SIZE {}\n".format(self.input_heapSize))

            #mkrecvheader_file.write("\n#GATED_PARAMETERS\n")
            #for k, v in self._config['gated_cli_args'].items():
            #    mkrecvheader_file.write("{} {}\n".format(k, v))

            mkrecvheader_file.write("\n#OTHER PARAMETERS\n")
            mkrecvheader_file.write("samples_per_block {}\n".format(self._config["samples_per_block"]))
            #mkrecvheader_file.write("n_channels {}\n".format(self._config["gated_cli_args"]["fft_length"] / 2 + 1 ))
            #mkrecvheader_file.write("integration_time {} # [s] fft_length * naccumulate / sampling_frequency (2.6GHz)\n".format(self._config["gated_cli_args"]["fft_length"] * self._config["gated_cli_args"]["naccumulate"] / 2.6E9 ))
            mkrecvheader_file.write("\n#PARAMETERS ADDED AUTOMATICALLY BY MKRECV\n")
            mkrecvheader_file.close()

            self.mkrec_cmd = []
            for i,k in enumerate(self._config['enabled_polarizations']):
                cfg = self._config.copy()
                cfg.update(self._config[k])
                cmd = "numactl --cpubind=1 --membind=1 mkrecv_nt --quiet --header {mkrecv_header} --dada-key {dada_key} \
                --sync-epoch {sync_time} --sample-clock {sample_clock} \
                --ibv-if {ibv_if} --port {port_rx} {mcast_sources}".format(mkrecv_header=mkrecvheader_file.name,
                        **cfg )
                self.mkrec_cmd.append(ManagedProcess(cmd))



            #for k in self.mkrec_cmd:
            #    k.error_callbacks.add(self._handle_error_state)
            #    k.stdout_callbacks.add( self._decode_capture_stdout)
            #    k.stderr_callbacks.add( self._handle_execution_stderr)

            #self._subprocesses.append(self.mkrec_cmd)
        except Exception as e:
            log.error("Error starting pipeline: {}".format(e))
            self.state = "error"
        else:
            self.state = "running"


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
        """@brief stop the dada_junkdb and dspsr instances."""
        log.info("Stoping EDD backend")
        if self.state != 'running':
            log.warning("pipleine state is not in state = running but in state {}".format(self.state))
            #return
        log.debug("Stopping")

        # stop mkrec process
        log.debug("Stopping mkrecv processes ...")
        for proc in self.mkrec_cmd:
            proc.terminate()
        # This will terminate also the gated spectromenter automatically

        log.debug("Stopping remaining processes ...")
        for proc in self._subprocesses:
            proc.terminate()

        self.state = "idle"


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
        """@brief deconfigure the dspsr pipeline."""
        log.info("Deconfiguring EDD backend")
        self.state = "deconfiguring"

        log.debug("Destroying dada buffers")
        for k in self._dada_buffers:
            cmd = "dada_db -d -k {0}".format(k)
            log.debug("Running command: {0}".format(cmd))
            _destory_ring_buffer = ExecuteCommand(
                cmd, outpath=None, resident=False)
            _destory_ring_buffer.stdout_callbacks.add(
                self._decode_capture_stdout)
            _destory_ring_buffer._process.wait()

        self._dada_buffers = []
        self.state = "idle"


@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


if __name__ == "__main__":
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str, default='localhost',
                      help='Host interface to bind to')
    parser.add_option('-p', '--port', dest='port', type=int, default=1235,
                      help='Port number to bind to')
    parser.add_option('', '--scpi-interface', dest='scpi_interface', type=str,
                      help='The interface to listen on for SCPI requests',
                      default="")
    parser.add_option('', '--scpi-port', dest='scpi_port', type=int,
                      help='The port number to listen on for SCPI requests')
    parser.add_option('', '--scpi-mode', dest='scpi_mode', action="store_true",
                      help='Activate the SCPI interface on startup')
    parser.add_option('', '--log-level', dest='log_level', type=str,
                      help='Port number of status server instance', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    logger.setLevel(opts.log_level.upper())

    log.setLevel(opts.log_level.upper())
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s "
             "- %(filename)s:%(lineno)s] %(message)s"),
        level=opts.log_level.upper(),
        logger=logger)
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting GatedSpectrometerPipeline instance")
    server = GatedSpectrometerPipeline(
        opts.host, opts.port,
        opts.scpi_interface, opts.scpi_port)
    log.info("Created GatedSpectrometerPipeline instance")
    signal.signal(
        signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, server))

    def start_and_display():
        log.info("Starting GatedSpectrometerPipeline server")
        server.start()
        log.debug("Started GatedSpectrometerPipeline server")
        if opts.scpi_mode:
            log.debug("SCPI mode")
            server.set_control_mode(server.SCPI)
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(
                server.bind_address))\

    ioloop.add_callback(start_and_display)
    ioloop.start()
