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
from ExecuteCommand import ExecuteCommand
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
#from mpikat.core.master_controller import MasterController
#from mpikat.katportalclient_wrapper import KatportalClientWrapper
#from mpikat.utils import check_ntp_sync

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.GatedSpectrometerPipeline")
log.setLevel('DEBUG')

PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "error"]

#DADA_BUFFERS = ['dada', 'dadc']
#Hotfix: disbale second buffer
DADA_BUFFERS = ['dada']

DEFAULT_CONFIG = {
        "base_output_dir": os.getcwd(),
        "nbits" : 12,
        "samples_per_heap": 4096,  # this is from mksend / mkrecv configuration
        "samples_per_block": 512*1024*1024, # 512 Mega sampels per buffer block to allow high res  spectra 
        "enabled_polarizations" : ["polarization_0"],
        "dada_db_params":
        {
            "args": "-n 8 -p -l"  # The buffersize is calculated from the samples_per heap and the bit depth
        },
        "gated_cli_args":
        {
            "fft_length": 1024,
            "naccumulate": 512,
            "input_level": 100,
            "output_level": 100,
            "output_bit_depth": 32,
            "null_output": False            # Write outptu to /dev/null for testing purposes
        },
        "mkrecv":
        {
            "polarization_0" :
            {
                "ibv_if": "10.10.1.10",
                "mcast_sources": "225.0.0.152 225.0.0.153 225.0.0.154 225.0.0.155",
                "port": "7148",
                "dada_key": 'dada',     # Ugly but simplest way to have global bugger def here
                "numa_node": 1                   # we only have on ethernet interface on numa node 1
            },
             "polarization_1" :
            {
                "ibv_if": "10.10.1.11",
                "mcast_sources": "225.0.0.156 225.0.0.157 225.0.0.158 225.0.0.159",
                "port": "7148",
                "dada_key": 'dadc',
                "numa_node": 1                   # we only have on ethernet interface on numa node 1
            }
        }
    }


mkrecv_header = """
# This header file contains the MKRECV configuration for capture of 1 polarisation
# from the Effelsberg EDD system. It specifies all 4 mcast groups of one IF channel.
PACKET_SIZE 8400
IBV_VECTOR   -1          # IBV forced into polling mode
IBV_MAX_POLL 10
PORT         7148

DADA_MODE    4                       # The mode, 4=full dada functionality
BYTES_PER_SECOND unset

SAMPLE_CLOCK_START 0 # This should be updated with the sync-time of the packetiser to allow for UTC conversion from the sample clock

SYNC_TIME   1550678891.0
SAMPLE_CLOCK 2600000000

NTHREADS 32
NHEAPS 64
NGROUPS_TEMP 65536

#SPEAD specifcation for EDD packetiser data stream
NINDICES    1   # Although there is more than one index, we are only receiving one polarisation so only need to specify the time index
# The first index item is the running timestamp
IDX1_ITEM   0      # First item of a SPEAD heap
IDX1_STEP   4096 # The difference between successive timestamps
# Add side item to buffer
SCI_LIST    7
"""



class EddConfigurationError(Exception):
    pass


class UnknownControlMode(Exception):
    pass


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
        #self._volumes = ["/tmp/:/scratch/"]
        self._config = None
        #self._source_config = None
        #self._dspsr = None
        self._subprocesses = []
        self._dada_buffers = []
        #self.setup_sensors()
        #super(GatedSpectrometerPipeline, self).__init__(ip, port, None)
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

#    def _save_capture_stdout(self, stdout, callback):
#        with open("{}.par".format(self._source_config["source-name"]), "a") as file:
#            file.write('{}\n'.format(str(stdout)))

    def _handle_execution_returncode(self, returncode, callback):
        log.debug(returncode)

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

        # Merge retrieved config into default
        def __updateConfig(oldo, new):
            old = oldo.copy()
            for k in new:
                if isinstance(old[k], dict):
                    old[k] = __updateConfig(old[k], new[k])
                else:
                    old[k] = new[k]
            return old
        cfg = config_json
        #cfg = json.loads(config_json)
        self._config = __updateConfig(DEFAULT_CONFIG, cfg)

        try:
            self.deconfigure()
        except Exception as error:
            raise RuntimeError(str(error))

        output_filename_base = "GATED_RUN_{:.0f}_POL_".format(time.time())

        logging.debug('Setting dada parameters according to bit depth')
        bitdepth = self._config["nbits"]
        self.heapSize =  self._config["samples_per_heap"] * bitdepth / 8

        # We store 512 mega samples in the buffer
        nSamples = self._config["samples_per_block"]
        nHeaps = nSamples / self._config["samples_per_heap"]

        self.input_bufferSize = nHeaps * (self.heapSize + 64 / 8) 
        nSlices = nSamples / self._config['gated_cli_args']['fft_length'] /  self._config['gated_cli_args']['naccumulate']
        nChannels = self._config['gated_cli_args']['fft_length'] / 2 + 1
        self.output_bufferSize = nSlices * 2 * nChannels * self._config['gated_cli_args']['output_bit_depth'] / 8

        def create_ring_buffer(bufferSize, key):
            args ="-b {} {}".format(bufferSize, self._config["dada_db_params"]["args"])
            cmd = "numactl --cpubind=1 --membind=1 dada_db -k {key} {args}".format(key=key, args=args)
            log.debug("Running command: {0}".format(cmd))
            _create_ring_buffer = ExecuteCommand(
                cmd, outpath=None, resident=False)
            _create_ring_buffer.stdout_callbacks.add(
                self._decode_capture_stdout)
            _create_ring_buffer._process.wait()


        for i,k in enumerate(self._config['enabled_polarizations']):
            bufferName = self._config['mkrecv'][k]['dada_key']
            ofname = bufferName[::-1]
            self._dada_buffers.append(bufferName)
            self._dada_buffers.append(ofname)

            # configure dada buffer
            create_ring_buffer(self.input_bufferSize, bufferName)
            create_ring_buffer(self.output_bufferSize, ofname)

            #if self._config["gated_cli_args"]["null_output"]:
            #    log.info("Null output selected! No output will be written!")
            #    ofname = "/dev/null"
            #else:
            odirname = output_filename_base + str(i) 
            os.mkdir(odirname)
            #log.info("Output polarisation {} to file {}".format(i, ofname))

            log_level='debug'
            # Configure + launch gated spectrometer

            cmd = "numactl --cpubind=1 --membind=1 gated_spectrometer --nsidechannelitems=1 --input_key={dada_key} --speadheap_size={heapSize} --selected_sidechannel=0 --nbits={nbits} --fft_length={fft_length} --naccumulate={naccumulate} --input_level={input_level} --output_bit_depth={output_bit_depth} --output_level={output_level} -o {ofname} --log_level={log_level} --output_type=dada".format(dada_key=bufferName, log_level=log_level, ofname=ofname, nbits=bitdepth, heapSize=self.heapSize, **self._config["gated_cli_args"])
            # here should be a smarter system to parse the options from the
            # controller to the program without redundant typing of options
            log.debug("Command to run: {}".format(cmd))

            #log.warning(" NOE XECUTION OF GATED SPECTROMETER FOR TESTING!!!")
            gated_cli = ExecuteCommand(cmd, outpath=None, resident=True, env={"CUDA_VISIBLE_DEVICES":str(1)}) #HOTFIX set to numa node 1
            gated_cli.stdout_callbacks.add( self._decode_capture_stdout)
            gated_cli.stderr_callbacks.add( self._handle_execution_stderr)
            gated_cli.error_callbacks.add(self._handle_error_state)
            self._subprocesses.append(gated_cli)

            cmd = "numactl --cpubind=1 --membind=1 dada_dbdisk -k {key} -D {odir} -v".format(key=ofname, odir=odirname)
            dadadiskcmd = ExecuteCommand(cmd, outpath=None, resident=True, env={"CUDA_VISIBLE_DEVICES":str(1)})
            dadadiskcmd.stdout_callbacks.add( self._decode_capture_stdout)
            dadadiskcmd.stderr_callbacks.add( self._handle_execution_stderr)
            self._subprocesses.append(dadadiskcmd)
            # Allow crash here as only temporary solution of using disk
#            dadadiskcmd.error_callbacks.add(self._handle_error_state)

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

            mkrecvheader_file.write("HEAP_SIZE {}\n".format(self.heapSize))

            mkrecvheader_file.write("\n#GATED_PARAMETERS\n")
            for k, v in self._config['gated_cli_args'].items():
                mkrecvheader_file.write("{} {}\n".format(k, v))

            mkrecvheader_file.write("\n#OTHER PARAMETERS\n")
            mkrecvheader_file.write("samples_per_block {}\n".format(self._config["samples_per_block"]))
            mkrecvheader_file.write("n_channels {}\n".format(self._config["gated_cli_args"]["fft_length"] / 2 + 1 ))
            mkrecvheader_file.write("integration_time {} # [s] fft_length * naccumulate / sampling_frequency (2.6GHz)\n".format(self._config["gated_cli_args"]["fft_length"] * self._config["gated_cli_args"]["naccumulate"] / 2.6E9 ))
 #           mkrecvheader_file.write("FILE_SIZE {}\n".format(512*1024*1024))

            mkrecvheader_file.write("\n#PARAMETERS ADDED AUTOMATICALLY BY MKRECV\n")

            mkrecvheader_file.close()

            self.mkrec_cmd = []


            for i,k in enumerate(self._config['enabled_polarizations']):
                cfg = self._config['mkrecv'][k]
                cmd = "numactl --cpubind=1 --membind=1 mkrecv_nt --quiet --header {mkrecv_header} --dada-key {dada_key} --ibv-if {ibv_if} --port {port} {mcast_sources}".format(mkrecv_header=mkrecvheader_file.name, **cfg )
                self.mkrec_cmd.append(ExecuteCommand(cmd, outpath=None, resident=True))


            for k in self.mkrec_cmd:
                k.error_callbacks.add(self._handle_error_state)
                k.stdout_callbacks.add( self._decode_capture_stdout)
                k.stderr_callbacks.add( self._handle_execution_stderr)

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
            log.error("pipleine state is not in state = running but in state {}, nothing to stop".format(self.state))
            return
        log.debug("Stopping")

        def __stopProcess(proc, timeout=10):
            log.debug("Terminating process: {}".format(proc._command))
            retval = proc._process.poll()
            if retval is not None:
                log.debug("Already terminated with return value of {}".format(retval))
                return

            proc.set_finish_event()
            proc.finish()
            log.debug("  Waiting {} seconds for process to terminate...".format(timeout))
            now = time.time()
            while time.time() - now < timeout:
                retval = proc._process.poll()
                if retval is not None:
                    log.debug("Returned a return value of {}".format(retval))
                    break
                else:
                    time.sleep(0.5)
            if proc._process.poll() is None:
                log.warning("Failed to terminate proc {} in alloted time".format(proc._command))
                log.info("Killing process")
                proc._process.kill()

        # stop mkrec process
        log.debug("Stopping mkrecv processes")
        for proc in self.mkrec_cmd:
            __stopProcess(proc)

        log.debug("Stopping remaining processes ..")
        for proc in self._subprocesses:
            __stopProcess(proc)
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


def main():
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
    logging.getLogger('mpikat').setLevel(logging.DEBUG)
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
                server.bind_address))
    ioloop.add_callback(start_and_display)
    ioloop.start()


    #logging.info("Starting pipeline instance")
    #server = GatedSpectrometerPipeline()
    #server.configure(json.dumps(DEFAULT_CONFIG))
    #server.start("")
    #server.stop()
    #server.deconfigure()

if __name__ == "__main__":
    main()

