"""
Copyright (c) 2019 Jason Wu <jwu@mpifr-bonn.mpg.de>

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
import tempfile
import json
import os
import time
from mpikat.utils.process_tools import ManagedProcess, command_watcher
from mpikat.utils.process_monitor import SubprocessMonitor
from mpikat.utils.sensor_watchdog import SensorWatchdog
from mpikat.utils.db_monitor import DbMonitor
from mpikat.utils.mkrecv_stdout_parser import MkrecvSensors
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig
from mpikat.effelsberg.edd.EDDDataStore import EDDDataStore
import mpikat.utils.numa as numa
from astropy.time import Time
import astropy.units as u
from astropy.coordinates import SkyCoord
from subprocess import PIPE, Popen
from mpikat.effelsberg.edd.edd_digpack_client import DigitiserPacketiserClient
from mpikat.effelsberg.edd.pipeline.dada import render_dada_header, make_dada_key_string
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig
import shlex
import threading
import base64
import tornado
import coloredlogs
import signal
import astropy.units as units
from optparse import OptionParser
from tornado.gen import coroutine
#from katcp import AsyncDeviceServer, Message, AsyncReply
from katcp import AsyncDeviceServer, Message, Sensor, AsyncReply, KATCPClientResource
from katcp.kattypes import request, return_reply, Str

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.pipeline")
log.setLevel('DEBUG')

RUN = True

#PIPELINES = {}

BLANK_IMAGE = "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"
PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "error"]
#        "args": "-n 64 -b 67108864 -p -l",
DEFAULT_CONFIG = {
    "base_output_dir": os.getcwd(),
    "dspsr_params":
    {
        "args": "-L 10 -r -minram 1024"
    },
    "db_params":
    {
        "size" : 409600000,
        "number": 32
    },
    "dada_header_params":
    {
        "filesize": 32000000000,
        "telescope": "Effelsberg",
        "instrument": "EDD",
        "frequency_mhz": 1200.0,
        "receiver_name": "P217",
        "mc_source": "225.0.0.110+3,225.0.0.114+3",
        "bandwidth": 800,
        "tsamp": 0.000625,
        "mode": "PSR",
        "nbit": 8,
        "ndim": 1,
        "npol": 2,
        "nchan": 1,
        "resolution": 1,
        "dsb": 1,
        "ra": "123",
        "dec": "-10"
    },
    "pipeline_config":
    {
        "mode": "Pipeline_focus_cabin",
        "mc_source": "225.0.0.110+3,225.0.0.114+3",
        "digpack_ip": "134.104.70.65",
        "digpack_port": 7147,
        "nbits": 8,
        "mc_streaming_port": 7148,
        "sampling_rate": 3200000000,
        "predecimation_factor": 2,
        "flip_band": 1,
        "numa": 0,
        "central_freq": 1200,
        "interface": 0,
        "bandwidth": 800,
        "sync_epoch" : 0
    },
    "source_config":
    {
        "source-name" : "J1939+2134",
        "nchannels" : 1024,
        "nbins" : 1024,
        "ra" : "294.910416667",
        "dec" : "21.10725"
    }
}

NUMA_MODE = {
    0: ("0-9", "10", "11,12,13,14"),
    1: ("18-28", "29", "30,31,32,33")
}
INTERFACE = {0: "10.10.1.14", 1: "10.10.1.15",
             2: "10.10.1.16", 3: "10.10.1.17"}

"""
Central frequency of each band should be with BW of 162.5
239.2.1.150 2528.90625
239.2.1.151 2366.40625
239.2.1.152 2203.9075
239.2.1.153 2041.40625
239.2.1.154 1878.90625
239.2.1.155 1716.405
239.2.1.156 1553.9075
239.2.1.157 1391.40625
"""

sensors = {"ra": 123, "dec": -10, "source-name": "J1939+2134",
           "scannum": 0, "subscannum": 1}


def is_accessible(path, mode='r'):
    """
    Check if the file or directory at `path` can
    be accessed by the program using `mode` open flags.
    """
    try:
        f = open(path, mode)
        f.close()
    except IOError:
        return False
    return True


def parse_tag(source_name):
    split = source_name.split("_")
    if len(split) == 1:
        return "default"
    else:
        return split[-1]



class ExecuteCommand(object):

    def __init__(self, command, outpath=None, resident=False):
        self._command = command
        self._resident = resident
        self._outpath = outpath
        self.stdout_callbacks = set()
        self.stderr_callbacks = set()
        self.error_callbacks = set()
        self.fscrunch_callbacks = set()
        self.tscrunch_callbacks = set()
        self.profile_callbacks = set()
        self._monitor_threads = []
        self._process = None
        self._executable_command = None
        self._monitor_thread = None
        self._stdout = None
        self._stderr = None
        self._error = False
        self._finish_event = threading.Event()

        if not self._resident:
            self._finish_event.set()

        self._executable_command = shlex.split(self._command)

        if RUN:
            try:
                self._process = Popen(self._executable_command,
                                      stdout=PIPE,
                                      stderr=PIPE,
                                      bufsize=1,
                                      # shell=True,
                                      universal_newlines=True)
            except Exception as error:
                log.exception("Error while launching command: {}".format(
                    self._executable_command))
                self.error = True
            if self._process == None:
                self._error = True
            self.pid = self._process.pid
            # log.debug("PID of {} is {}".format(
            #    self._executable_command, self.pid))
            self._monitor_thread = threading.Thread(
                target=self._execution_monitor)
            self._stderr_monitor_thread = threading.Thread(
                target=self._stderr_monitor)
            self._monitor_thread.start()
            self._stderr_monitor_thread.start()
            if self._outpath is not None:
                self._png_monitor_thread = threading.Thread(
                    target=self._png_monitor)
                self._png_monitor_thread.start()

    def __del__(self):
        class_name = self.__class__.__name__

    def set_finish_event(self):
        if not self._finish_event.isSet():
            self._finish_event.set()

    def finish(self):
        if RUN:
            self._process.send_signal(signal.SIGINT)
            # self._process.terminate()
            self._monitor_thread.join()
            self._stderr_monitor_thread.join()
            if self._outpath is not None:
                self._png_monitor_thread.join()

    def stdout_notify(self):
        for callback in self.stdout_callbacks:
            callback(self._stdout, self)

    @property
    def stdout(self):
        return self._stdout

    @stdout.setter
    def stdout(self, value):
        self._stdout = value
        self.stdout_notify()

    def stderr_notify(self):
        for callback in self.stderr_callbacks:
            callback(self._stderr, self)

    @property
    def stderr(self):
        return self._stderr

    @stderr.setter
    def stderr(self, value):
        self._stderr = value
        self.stderr_notify()

    def fscrunch_notify(self):
        for callback in self.fscrunch_callbacks:
            callback(self._fscrunch, self)

    @property
    def fscrunch(self):
        return self._fscrunch

    @fscrunch.setter
    def fscrunch(self, value):
        self._fscrunch = value
        self.fscrunch_notify()

    def tscrunch_notify(self):
        for callback in self.tscrunch_callbacks:
            callback(self._tscrunch, self)

    @property
    def tscrunch(self):
        return self._tscrunch

    @tscrunch.setter
    def tscrunch(self, value):
        self._tscrunch = value
        self.tscrunch_notify()

    def profile_notify(self):
        for callback in self.profile_callbacks:
            callback(self._profile, self)

    @property
    def profile(self):
        return self._profile

    @profile.setter
    def profile(self, value):
        self._profile = value
        self.profile_notify()

    def error_notify(self):
        for callback in self.error_callbacks:
            callback(self)

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, value):
        self._error = value
        self.error_notify()

    def _execution_monitor(self):
        # Monitor the execution and also the stdout
        if RUN:
            while self._process.poll() == None:
                stdout = self._process.stdout.readline().rstrip("\n\r")
                if stdout != b"":
                    if (not stdout.startswith("heap")) & (not stdout.startswith("mark")) & (not stdout.startswith("[")) & (not stdout.startswith("-> parallel")) & (not stdout.startswith("-> sequential")):
                        self.stdout = stdout
                    # print self.stdout, self._command

            if not self._finish_event.isSet():
                # For the command which runs for a while, if it stops before
                # the event is set, that means that command does not
                # successfully finished
                stdout = self._process.stdout.read()
                stderr = self._process.stderr.read()
                log.error(
                    "Process exited unexpectedly with return code: {}".format(self._process.returncode))
                log.error("exited unexpectedly, stdout = {}".format(stdout))
                log.error("exited unexpectedly, stderr = {}".format(stderr))
                log.error("exited unexpectedly, cmd = {}".format(self._command))
                #self.error = True

    def _stderr_monitor(self):
        if RUN:
            while self._process.poll() == None:
                stderr = self._process.stderr.readline().rstrip("\n\r")
                if stderr != b"":
                    self.stderr = stderr
            if not self._finish_event.isSet():
                # For the command which runs for a while, if it stops before
                # the event is set, that means that command does not
                # successfully finished
                stdout = self._process.stdout.read()
                stderr = self._process.stderr.read()
                log.error(
                    "Process exited unexpectedly with return code: {}".format(self._process.returncode))
                log.error("exited unexpectedly, stdout = {}".format(stdout))
                log.error("exited unexpectedly, stderr = {}".format(stderr))
                log.error("exited unexpectedly, cmd = {}".format(self._command))
                self.error = True

    def _png_monitor(self):
        if RUN:
            while self._process.poll() == None:
                # while not self._finish_event.isSet():
                log.debug("Accessing archive PNG files")
                try:
                    with open("{}/fscrunch.png".format(self._outpath), "rb") as imageFile:
                        self.fscrunch = base64.b64encode(imageFile.read())
                except Exception as error:
                    log.debug(error)
                    #log.debug("fscrunch.png is not ready")
                try:
                    with open("{}/tscrunch.png".format(self._outpath), "rb") as imageFile:
                        self.tscrunch = base64.b64encode(imageFile.read())
                except Exception as error:
                    log.debug(error)
                    #log.debug("tscrunch.png is not ready")
                try:
                    with open("{}/profile.png".format(self._outpath), "rb") as imageFile:
                        self.profile = base64.b64encode(imageFile.read())
                except Exception as error:
                    log.debug(error)
                    #log.debug("profile.png is not ready")
                time.sleep(7)


class EddPulsarPipelineKeyError(Exception):
    pass

class EddPulsarPipelineError(Exception):
    pass

class EddPulsarPipeline(EDDPipeline):
    """
    @brief Interface object which accepts KATCP commands

    """
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")

    def __init__(self, ip, port):
        """@brief initialize the pipeline."""
        EDDPipeline.__init__(self, ip, port, DEFAULT_CONFIG)
        self.__numa_node_pool = []
        self.mkrec_cmd = []
        self._dada_buffers = ["dada","dadc"]
        self._dspsr = None
        self._mkrecv_ingest_proc = None
        self._archive_directory_monitor = None


    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
    	EDDPipeline.setup_sensors(self)
        self._tscrunch = Sensor.string(
            "tscrunch_PNG",
            description="tscrunch png",
            default=BLANK_IMAGE,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._tscrunch)

        self._fscrunch = Sensor.string(
            "fscrunch_PNG",
            description="fscrunch png",
            default=BLANK_IMAGE,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._fscrunch)

        self._profile = Sensor.string(
            "profile_PNG",
            description="pulse profile png",
            default=BLANK_IMAGE,
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._profile)

        self._central_freq = Sensor.string(
            "_central_freq",
            description="_central_freq",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._central_freq)

        self._source_name_sensor = Sensor.string(
            "target_name",
            description="target name",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._source_name_sensor)

        self._nchannels = Sensor.string(
            "_nchannels",
            description="_nchannels",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nchannels)

        self._nbins = Sensor.string(
            "_nbins",
            description="_nbins",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._nbins)

        self._time_processed = Sensor.string(
            "_time_processed",
            description="_time_processed",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._time_processed)

    def _decode_capture_stdout(self, stdout, callback):
        log.debug('{}'.format(str(stdout)))

    def _error_treatment(self, callback):
        self.stop_pipeline_with_mkrecv_crashed()

    def _save_capture_stdout(self, stdout, callback):
        with open("{}.par".format(self._config["source_config"]["source-name"]), "a") as file:
            file.write('{}\n'.format(str(stdout)))

    def _handle_execution_returncode(self, returncode, callback):
        log.debug(returncode)

    def _handle_execution_stderr(self, stderr, callback):
        if bool(stderr[:8] == "Finished") & bool("." not in stderr):
            self._time_processed.set_value(stderr)
            log.debug(stderr)
        if bool(stderr[:8] != "Finished"):
            log.info(stderr)

    def _handle_eddpolnmerge_stderr(self, stderr, callback):
        log.debug(stderr)

    def _add_tscrunch_to_sensor(self, png_blob, callback):
        self._tscrunch.set_value(png_blob)

    def _add_fscrunch_to_sensor(self, png_blob, callback):
        self._fscrunch.set_value(png_blob)

    def _add_profile_to_sensor(self, png_blob, callback):
        self._profile.set_value(png_blob)

    @coroutine
    def _create_ring_buffer(self, bufferSize, blocks, key, numa_node):
         """
         @brief Create a ring buffer of given size with given key on specified numa node.
                Adds and register an appropriate sensor to thw list
         """
         # always clear buffer first. Allow fail here
         yield command_watcher("dada_db -d -k {key}".format(key=key), allow_fail=True)

         cmd = "numactl --cpubind={numa_node} --membind={numa_node} dada_db -k {key} -n {blocks} -b {bufferSize} -p -l".format(key=key, blocks=blocks, bufferSize=bufferSize, numa_node=numa_node)
         log.debug("Running command: {0}".format(cmd))
         yield command_watcher(cmd)

         #M = DbMonitor(key, self._buffer_status_handle)
         #M.start()
         #self._dada_buffers.append({'key': key, 'monitor': M})

    def _buffer_status_handle(self, status):
        """
        @brief Process a change in the buffer status
        """
        pass


    @coroutine
    def configure(self, config_json):
        log.info("Configuring EDD backend for processing")
        log.debug("Configuration string: '{}'".format(config_json))
        if self.state != "idle":
            log.warning(
                "Configure received while in state: {} - deconfigureing first ...".format(self.state))
            try:
                log.debug("Deconfiguring pipeline before configuring")
                self.deconfigure()
            except Exception as error:
                raise EddPulsarPipelineError(str(error))

        self.state = "configuring"
        yield self.set(config_json)

        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)

        self.numa_number = self._config["pipeline_config"]["numa"]
        pipeline_name = self._config["pipeline_config"]["mode"]
        log.debug("Pipeline name = {}".format(pipeline_name))


        yield self._create_ring_buffer(self._config["db_params"]["size"], self._config["db_params"]["number"], "dada", self.numa_number)
        yield self._create_ring_buffer(self._config["db_params"]["size"], self._config["db_params"]["number"], "dadc", self.numa_number)

        self._subprocessMonitor = SubprocessMonitor()

        self.state = "ready"
        log.info("Pipeline configured")

    @coroutine
    def measurement_prepare(self, config_json):
        log.info("checking status")
        if self.state != "ready":
            log.debug("pipeline is not in ready state")
            if self.state == "capturing":
                log.debug(
                    "pipeline is still captureing, issuing stop now and will start shortly")
                yield self.measurement_stop()
            if self.state == "starting":
                log.debug("pipeline is starting, do not send multiple start")
                return

        self.state = "configuring"
        yield self.set(config_json)
        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)
        self.pulsar_flag = is_accessible(
            '/tmp/epta/{}.par'.format(self._config['source_config']["source-name"][1:]))
        if ((parse_tag(self._config['source_config']["source-name"]) == "default") or (parse_tag(self._config['source_config']["source-name"]) != "R")) and (not self.pulsar_flag):
            if (parse_tag(self._config['source_config']["source-name"]) != "FB"):
                error = "source is not pulsar or calibrator"
                raise EddPulsarPipelineError(error)
        self._fscrunch.set_value(BLANK_IMAGE)
        self._tscrunch.set_value(BLANK_IMAGE)
        self._profile.set_value(BLANK_IMAGE)
        log.info("starting pipeline")
        self._state = "starting"
        self._timer = Time.now()
        self._central_freq.set_value(str(self._config['pipeline_config']["central_freq"]))
        self._source_name_sensor.set_value(self._config['source_config']["source-name"])
        self._nchannels.set_value(self._config['source_config']["nchannels"])
        self._nbins.set_value(self._config['source_config']["nbins"])
        self.cpu_numbers = NUMA_MODE[self.numa_number][2]
        self.cuda_number = self.numa_number
        c = SkyCoord("{} {}".format(self._config['source_config'][
                     "ra"], self._config['source_config']["dec"]), unit=(u.deg, u.deg))
        header = self._config["dada_header_params"]
        header["ra"] = c.to_string("hmsdms").split(" ")[0].replace(
            "h", ":").replace("m", ":").replace("s", "")
        header["dec"] = c.to_string("hmsdms").split(" ")[1].replace(
            "d", ":").replace("m", ":").replace("s", "")
        header["key"] = self._dada_buffers[0]
        header["mc_source"] = self._config['pipeline_config']["mc_source"]
        header["frequency_mhz"] = self._config['pipeline_config']["central_freq"]
        header["bandwidth"] = self._config['pipeline_config']["bandwidth"]
        header["mc_streaming_port"] = self._config['pipeline_config']["mc_streaming_port"]
        header["interface"] = INTERFACE[self._config['pipeline_config']["interface"]]
        header["sync_time"] = self._config['pipeline_config']["sync_epoch"]
        header["sample_clock"] = float(
            self._config['pipeline_config']["sampling_rate"]) / float(self._config['pipeline_config']["predecimation_factor"])
        header["tsamp"] = 1 / (2.0 * self._config['pipeline_config']["bandwidth"])
        header["source_name"] = self._config['source_config']["source-name"]
        header["obs_id"] = "{0}_{1}".format(
            sensors["scannum"], sensors["subscannum"])

        tstr = Time.now().isot.replace(":", "-")
        tdate = tstr.split("T")[0]
        ####################################################
        #SETTING UP THE INPUT AND SCRUNCH DATA DIRECTORIES #
        ####################################################
        try:
            self.in_path = os.path.join("/media/scratch/jason/dspsr_output/", tdate, self._config['source_config']["source-name"],
                                   str(self._config['pipeline_config']["central_freq"]), tstr, "raw_data")
            self.out_path = os.path.join(
                "/media/scratch/jason/dspsr_output/", tdate, self._config['source_config']["source-name"], str(self._config['pipeline_config']["central_freq"]), tstr, "combined_data")
            log.debug("Creating directories")
            log.debug("in path {}".format(self.in_path))
            log.debug("in path {}".format(self.out_path))
            if not os.path.isdir(self.in_path):
                os.makedirs(self.in_path)
            if not os.path.isdir(self.out_path):
                os.makedirs(self.out_path)
            os.chdir(self.in_path)
            log.debug("Change to workdir: {}".format(os.getcwd()))
            log.debug("Current working directory: {}".format(os.getcwd()))
        except Exception as error:
        	raise EddPulsarPipelineError(str(error))

        os.chdir("/tmp/")

        ####################################################
        #CREATING THE PREDICTOR WITH TEMPO2                #
        ####################################################
        self.pulsar_flag_with_R = is_accessible(
            '/tmp/epta/{}.par'.format(self._config['source_config']["source-name"][1:-2]))
        log.debug("{}".format(
            (parse_tag(self._config['source_config']["source-name"]) == "default") & self.pulsar_flag))
        if (parse_tag(self._config['source_config']["source-name"]) == "default") & is_accessible('/tmp/epta/{}.par'.format(self._config['source_config']["source-name"][1:])):
            cmd = 'numactl -m {} taskset -c {} tempo2 -f /tmp/epta/{}.par -pred'.format(self.numa_number, NUMA_MODE[self.numa_number][1], self._config['source_config']["source-name"][1:]).split()
            cmd.append("Effelsberg {} {} {} {} 24 2 3599.999999999".format(Time.now().mjd - 1, Time.now().mjd + 1, float(self._config["pipeline_config"]["central_freq"]) - 200, float(self._config["pipeline_config"]["central_freq"]) + 200))
            log.debug("Command to run: {}".format(cmd))
            yield command_watcher(cmd, )
            attempts = 0
            retries = 5
            while True:
                if attempts >= retries:
                    error = "could not read t2pred.dat"
                    raise EddPulsarPipelineError(error)
                else:
                    time.sleep(1)
                    if is_accessible('{}/t2pred.dat'.format(os.getcwd())):
                        log.debug('found {}/t2pred.dat'.format(os.getcwd()))
                        break
                    else:
                        attempts += 1
        self.dada_header_file = tempfile.NamedTemporaryFile(
            mode="w",
            prefix="edd_dada_header_",
            suffix=".txt",
            dir="/tmp/",
            delete=False)
        log.debug(
            "Writing dada header file to {0}".format(
                self.dada_header_file.name))
        header_string = render_dada_header(header)
        self.dada_header_file.write(header_string)
        self.dada_key_file = tempfile.NamedTemporaryFile(
            mode="w",
            prefix="dada_keyfile_",
            suffix=".key",
            dir="/tmp/",
            delete=False)
        log.debug("Writing dada key file to {0}".format(
            self.dada_key_file.name))
        key_string = make_dada_key_string(self._dada_buffers[1])
        self.dada_key_file.write(make_dada_key_string(self._dada_buffers[1]))
        log.debug("Dada key file contains:\n{0}".format(key_string))
        self.dada_header_file.close()
        self.dada_key_file.close()

        attempts = 0
        retries = 5
        while True:
            if attempts >= retries:
                error = "could not read dada_key_file"
                raise EddPulsarPipelineError(error)
            else:
                time.sleep(1)
                if is_accessible('{}'.format(self.dada_key_file.name)):
                    log.debug('found {}'.format(self.dada_key_file.name))
                    break
                else:
                    attempts += 1
        self._state = "ready"

    @coroutine
    def measurement_start(self):
        log.info("checking status")
        if self._state != "ready":
            log.debug("pipeline is not int ready state")
            if self._state == "running":
                log.debug(
                    "pipeline is still running, issuing stop now and will start shortly")
                yield self.stop_pipeline()
            if self._state == "starting":
                log.debug("pipeline is starting, do not send multiple start")
                return

        ####################################################
        #STARTING DSPSR                                    #
        ####################################################
        os.chdir(self.in_path)
        log.debug("pulsar_flag = {}".format(self.pulsar_flag))
        log.debug("source_name = {}".format(self._config['source_config']["source-name"]))

        if (parse_tag(self._config['source_config']["source-name"]) == "default") and self.pulsar_flag:
            cmd = "numactl -m {numa} dspsr {args} {nchan} {nbin} -fft-bench -x 8192 -cpu {cpus} -cuda {cuda_number} -P {predictor} -N {name} -E {parfile} {keyfile}".format(
                numa=self.numa_number,
                args=self._config["dspsr_params"]["args"],
                nchan="-F {}:D".format(self._config['source_config']["nchannels"]),
                nbin="-b {}".format(self._config['source_config']["nbins"]),
                name=self._config['source_config']["source-name"],
                predictor="/tmp/t2pred.dat",
                parfile="/tmp/epta/{}.par".format(self._config['source_config']["source-name"][1:]),
                cpus=self.cpu_numbers,
                cuda_number=self.cuda_number,
                keyfile=self.dada_key_file.name)

        elif parse_tag(self._config['source_config']["source-name"]) == "R":
            cmd = "numactl -m {numa} dspsr -L 10 -c 1.0 -D 0.0001 -r -minram 1024 -fft-bench {nchan} -cpu {cpus} -N {name} -cuda {cuda_number}  {keyfile}".format(
                numa=self.numa_number,
                args=self._config["dspsr_params"]["args"],
                nchan="-F {}:D".format(self._config['source_config']["nchannels"]),
                name=self._config['source_config']["source-name"],
                cpus=self.cpu_numbers,
                cuda_number=self.cuda_number,
                keyfile=self.dada_key_file.name)

        elif parse_tag(self._config['source_config']["source-name"]) == "FB":
            cmd = "numactl -m {numa} taskset -c {cpus} digifil -threads 4 -F {nchan} -b8 -d 1 -I 0 -t {nbins} {keyfile}".format(
                numa=self.numa_number,
                nchan="{}".format(self._config['source_config']["nchannels"]),
                nbin="{}".format(self._config['source_config']["nbins"]),
                cpus=self.cpu_numbers,
                keyfile=self.dada_key_file.name)
        else:
            error = "source is unknown"
            raise EddPulsarPipelineError(error)
        """
        elif (parse_tag(self._config['source_config']["source-name"]) == "R") and (not self.pulsar_flag) and (not self.pulsar_flag_with_R):
            if (self._config['source_config']["source-name"][:2] == "3C" and self._config['source_config']["source-name"][-3:] == "O_R") or (self._config['source_config']["source-name"][:3] == "NGC" and self._config['source_config']["source-name"][-4:]=="ON_R"):
                cmd = "numactl -m {numa} dspsr -L 10 -c 1.0 -D 0.0001 -r -minram 1024 -set type=FluxCal-On -fft-bench {nchan} -cpu {cpus} -N {name} -cuda {self.cuda_number}  {keyfile}".format(
                    numa=self.numa_number,
                    args=self._config["dspsr_params"]["args"],
                    nchan="-F {}:D".format(self._config['source_config']["nchannels"]),
                    name=self._config['source_config']["source-name"],
                    cpus=self.cpu_numbers,
                    self.cuda_number=self.cuda_number,
                    keyfile=dada_key_file.name)
            elif (self._config['source_config']["source-name"][:3] == "NGC" and self._config['source_config']["source-name"][-5:] == "OFF_R") or (self._config['source_config']["source-name"][:2] == "3C" and self._config['source_config']["source-name"][-3:] == "N_R") or (self._config['source_config']["source-name"][:2] == "3C" and self._config['source_config']["source-name"][-3:] == "S_R"):
                cmd = "numactl -m {numa} dspsr -L 10 -c 1.0 -D 0.0001 -r -minram 1024 -set type=FluxCal-Off -fft-bench {nchan} -cpu {cpus} -N {name} -cuda {self.cuda_number}  {keyfile}".format(
                    numa=self.numa_number,
                    args=self._config["dspsr_params"]["args"],
                    nchan="-F {}:D".format(self._config['source_config']["nchannels"]),
                    name=self._config['source_config']["source-name"],
                    cpus=self.cpu_numbers,
                    self.cuda_number=self.cuda_number,
                    keyfile=dada_key_file.name)
        """

        #cmd = "numactl -m {} dbnull -k dadc".format(self.numa_number)
        log.debug("Running command: {0}".format(cmd))
        log.info("Staring DSPSR")
        self._dspsr = ManagedProcess(cmd)
        self._subprocessMonitor.add(self._dspsr, self._subprocess_error)
        ####################################################
        #STARTING EDDPolnMerge                             #
        ####################################################
        cmd = "numactl -m {numa} taskset -c {cpu} edd_merge --log_level=info".format(
            numa=self.numa_number, cpu=NUMA_MODE[self.numa_number][1])
        log.debug("Running command: {0}".format(cmd))
        log.info("Staring EDDPolnMerge")
        self._polnmerge_proc = ManagedProcess(cmd)
        self._subprocessMonitor.add(self._polnmerge_proc, self._subprocess_error)
        ####################################################
        #STARTING MKRECV                                   #
        ####################################################
        cmd = "numactl -m {numa} taskset -c {cpu} mkrecv_nt --header {dada_header} --dada-mode 4 --quiet".format(
            numa=self.numa_number, cpu=NUMA_MODE[self.numa_number][0], dada_header=self.dada_header_file.name)
        log.debug("Running command: {0}".format(cmd))
        log.info("Staring MKRECV")
        self._mkrecv_ingest_proc = ManagedProcess(cmd)
        self._subprocessMonitor.add(self._mkrecv_ingest_proc, self._subprocess_error)
        ####################################################
        #STARTING ARCHIVE MONITOR                          #
        ####################################################
        cmd = "python /src/mpikat/mpikat/effelsberg/edd/pipeline/archive_directory_monitor.py -i {} -o {}".format(
            self.in_path, self.out_path)
        log.debug("Running command: {0}".format(cmd))
        log.info("Staring archive monitor")
        self._archive_directory_monitor = ManagedProcess(cmd)
        self._subprocessMonitor.add(self._archive_directory_monitor, self._subprocess_error)
        cmd = "python /src/mpikat/mpikat/effelsberg/edd/pipeline/png_katcp_server.py -H 134.104.70.66 -p 10000 --path {}".format(self.out_path)
        log.debug("Running command: {0}".format(cmd))
        log.info("Staring archive monitor")
        self._archive_sensor = ManagedProcess(cmd)

        self._subprocessMonitor.add(self._archive_sensor, self._subprocess_error)
        self._subprocessMonitor.start()
        self._timer = Time.now() - self._timer
        log.info("Took {} s to start".format(self._timer * 86400))
        self._state = "running"
        log.info("Starting capturing")

    @coroutine
    def measurement_stop(self):
        """@brief stop the dada_junkdb and dspsr instances."""
        if self._state != "running":
            log.warning("pipeline is not captureing, can't stop now, current state = {}".format(
                self._state))
        self._state = "stopping"
        if self._subprocessMonitor is not None:
            self._subprocessMonitor.stop()


#        try:
        log.debug("Stopping")
        self._timeout = 10
        #process = [self._mkrecv_ingest_proc,
        #           self._polnmerge_proc, self._archive_directory_monitor]
        process = [self._mkrecv_ingest_proc,
                   self._polnmerge_proc,
                   self._archive_directory_monitor,
                   self._archive_sensor]


        for proc in process:
            #time.sleep(2)
            proc.terminate(timeout=1)
            #proc.set_finish_event()
            #proc.finish()
            """
            log.debug(
                "Waiting {} seconds for proc to terminate...".format(self._timeout))
            now = time.time()
            while time.time() - now < self._timeout:
                retval = proc._process.poll()
                if retval is not None:
                    log.debug(
                        "Returned a return value of {}".format(retval))
                    break
                else:
                    time.sleep(0.5)
            else:
                log.warning(
                    "Failed to terminate proc in alloted time")
                log.info("Killing process")
                proc._process.kill()
           	"""
            
        if (parse_tag(self._config['source_config']["source-name"]) == "default") & self.pulsar_flag:
            os.remove("/tmp/t2pred.dat")

        log.info("reset DADA buffer")
        #self._dada_buffers[1]['monitor'].stop()
        yield self._create_ring_buffer(self._config["db_params"]["size"], self._config["db_params"]["number"], "dadc", self.numa_number)

        del self._subprocessMonitor
        #except Exception as error:
        #    raise EddPulsarPipelineError(str(error))

        
        self._state = "ready"
        log.info("Pipeline Stopped")

    @coroutine
    def stop_pipeline_with_mkrecv_crashed(self):
        """@brief stop the dada_junkdb and dspsr instances."""
        try:
            os.kill(self._polnmerge_proc.pid, signal.SIGTERM)
        except Exception as error:
            log.error("cannot kill _polnmerge_proc_pid, {}".format(error))
        try:
            os.kill(self._archive_directory_monitor.pid, signal.SIGTERM)
        except Exception as error:
            log.error("cannot kill _archive_directory_monitor, {}".format(error))
        try:
            os.kill(self._dspsr.pid, signal.SIGTERM)
        except Exception as error:
            log.error("cannot kill _dspsr, {}".format(error))
        if (parse_tag(self._config['source_config']["source-name"]) == "default") & self.pulsar_flag:
            os.remove("/tmp/t2pred.dat")

        log.debug("deleting buffers")
        #self._dada_buffers[0]['monitor'].stop()
        #self._dada_buffers[1]['monitor'].stop()
        yield self._create_ring_buffer(self._config["db_params"]["size"], self._config["db_params"]["number"], "dada", self.numa_number)
        yield self._create_ring_buffer(self._config["db_params"]["size"], self._config["db_params"]["number"], "dadc", self.numa_number)
        self._state = "ready"

    @coroutine
    def deconfigure(self):
        """@brief deconfigure the dspsr pipeline."""
        log.info("Deconfiguring pipeline")
        log.debug("Destroying dada buffers")

        for k in self._dada_buffers:
            #k['monitor'].stop()
            cmd = "dada_db -d -k {0}".format(k)
            log.debug("Running command: {0}".format(cmd))
            yield command_watcher(cmd)
        #self._fscrunch.set_value(BLANK_IMAGE)
        #self._tscrunch.set_value(BLANK_IMAGE)
        #self._profile.set_value(BLANK_IMAGE)
        log.info("Deconfigured pipeline")
        self._state = "idle"


if __name__ == "__main__":
    launchPipelineServer(EddPulsarPipeline)
