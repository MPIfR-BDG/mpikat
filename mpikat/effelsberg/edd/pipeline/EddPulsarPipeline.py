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
from mpikat.utils.process_tools import ManagedProcess, command_watcher
from mpikat.utils.process_monitor import SubprocessMonitor
import mpikat.utils.numa as numa

from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig
from mpikat.effelsberg.edd.EDDDataStore import EDDDataStore
from mpikat.effelsberg.edd.pipeline.dada_rnt import render_dada_header, make_dada_key_string
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig
from mpikat.effelsberg.edd.pipeline.EddPulsarPipeline_blank_image import BLANK_IMAGE

import logging
import sys
import shlex
import shutil
import os
import base64
import time
from subprocess import Popen, PIPE
import logging
import tempfile
import json
import os
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from astropy.time import Time
import astropy.units as u
from astropy.coordinates import SkyCoord

from katcp import Sensor
from katcp.kattypes import request, return_reply, Int, Str

import tornado
from tornado.gen import coroutine

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.pipeline")
log.setLevel('DEBUG')

DEFAULT_CONFIG = {
    "id": "PulsarPipeline",
    "type": "PulsarPipeline",
    "epta_directory": "epta",                       # Data will be read from /mnt/epta_directory
    "input_data_streams":
    {
        "polarization_0":
        {
            "source": "",
            "description": "",
            "format": "MPIFR_EDD_Packetizer:1",
            "ip": "225.0.0.150+3",
            "port": "7148",
            "bit_depth": 8,
            "sample_rate": 3200000000,
            "sync_time": 1581164788.0,
            "samples_per_heap": 4096,
            "band_flip": 0,
            "predecimation_factor": 2,
            "central_freq": 1200
        },
        "polarization_1":
        {
            "source": "",
            "description": "",
            "format": "MPIFR_EDD_Packetizer:1",
            "ip": "225.0.0.154+3",
            "port": "7148",
            "bit_depth": 8,
            "sample_rate": 3200000000,
            "sync_time": 1581164788.0,
            "samples_per_heap": 4096,
            "band_flip": 1,
            "predecimation_factor": 2,
            "central_freq": 1200,
        }
    },
    "source_config":  # this should be passed in measurement-prepare
    {
        "source-name": "J1939+2134",
        "nchannels": 1024,
        "nbins": 1024,
        "ra": "294.910416667",
        "dec": "21.10725"
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
    "dspsr_params":
    {
        "args": "-L 10 -r -minram 1024"
    },
    "db_params":
    {
        "size": 409600000,
        "number": 32
    }
}

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


class ArchiveAdder(FileSystemEventHandler):
    def __init__(self, output_dir):
        super(ArchiveAdder, self).__init__()
        self.output_dir = output_dir
        self.first_file = True
        self.freq_zap_list = ""
        self.time_zap_list = ""

    def _syscall(self, cmd):
        log.info("Calling: {}".format(cmd))
        proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        proc.wait()
        if proc.returncode != 0:
            log.error(proc.stderr.read())
        else:
            log.debug("Call success")

    def fscrunch(self, fname):
        # frequency scrunch done here all fscrunch archive
        self._syscall("paz {} -e zapped {}".format(self.freq_zap_list, fname))
        self._syscall(
            "pam -F -e fscrunch {}".format(fname.replace(".ar", ".zapped")))
        return fname.replace(".ar", ".fscrunch")

    def first_tscrunch(self, fname):
        self._syscall("paz {} -e first {}".format(self.freq_zap_list, fname))

    def update_freq_zaplist(self, zaplist):
        self.freq_zap_list = "-F '0 1' "
        for item in range(len(zaplist.split(","))):
            self.freq_zap_list = str(
                self.freq_zap_list) + " -F '{}' ".format(zaplist.split(",")[item])

        self.freq_zap_list = self.freq_zap_list.replace(":", " ")
        log.info("Latest frequency zaplist {}".format(self.freq_zap_list))

    def update_time_zaplist(self, zaplist):
        self.time_zap_list = ""
        for item in range(len(zaplist.split(":"))):
            self.time_zap_list = str(
                self.time_zap_list) + " {}".format(zaplist.split(":")[item])

        #self.time_zap_list = self.time_zap_list.replace(":", " ")
        log.info("Latest time zaplist {}".format(self.time_zap_list))

    def process(self, fname):
        fscrunch_fname = self.fscrunch(fname)
        if self.first_file:
            log.info("First file in set. Copying to sum.?scrunch.")
            shutil.copy2(fscrunch_fname, "sum.fscrunch")
            self.first_tscrunch(fname)
            shutil.copy2(fname.replace(".ar", ".first"), "sum.tscrunch")
            os.remove(fname.replace(".ar", ".first"))
            self.first_file = False
        else:
            self._syscall("psradd -T -inplace sum.tscrunch {}".format(fname))
            # update fscrunch here with the latest list, cannot go backward
            # (i.e. cannot redo zap)
            self._syscall("paz {} -m sum.tscrunch".format(self.freq_zap_list))
            self._syscall(
                "psradd -inplace sum.fscrunch {}".format(fscrunch_fname))
            self._syscall(
                "paz -w '{}' -m sum.fscrunch".format(self.time_zap_list))
            self._syscall(
                "psrplot -p freq+ -j dedisperse -D ../combined_data/tscrunch.png/png sum.tscrunch")
            self._syscall(
                "pav -DFTp sum.fscrunch  -g ../combined_data/profile.png/png")
            #-y 1,`psrstat -Q -c nsubint sum.fscrunch | awk '{print $2-1}'` trying to grab the no of intergrations, failed
            self._syscall(
                "pav -FYp sum.fscrunch  -g ../combined_data/fscrunch.png/png")
            log.info("removing {}".format(fscrunch_fname))
        os.remove(fscrunch_fname)
        os.remove(fscrunch_fname.replace(".fscrunch", ".zapped"))
        log.info("Accessing archive PNG files")

    def on_created(self, event):
        log.info("New file created: {}".format(event.src_path))
        try:
            fname = event.src_path
            log.info(fname.find('.ar.') != -1)
            if fname.find('.ar.') != -1:
                log.info(
                    "Passing archive file {} for processing".format(fname[0:-9]))
                time.sleep(1)
                self.process(fname[0:-9])
        except Exception as error:
            log.error(error)


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
        self.mkrec_cmd = []
        self._dada_buffers = ["dada", "dadc"]
        self._dspsr = None
        self._mkrecv_ingest_proc = None
        self._archive_directory_monitor = None

        # Pick first available numa node. Disable non-available nodes via
        # EDD_ALLOWED_NUMA_NODES environment variable
        self.numa_number = numa.getInfo().keys()[0]
        self.__core_sets = {'mkrecv': ["0-3"], 'single': ["10"], 'dspsr': "11,12,13,14"}
        if len(numa.getInfo()[self.numa_number]['isolated_cores']) >= 4:
            self.__core_sets['mkrecv'] = numa.getInfo()[self.numa_number]['isolated_cores'][:4]
        else:
            self.__core_sets['mkrecv'] = numa.getInfo()[self.numa_number]['cores'][:4]
        self.__core_sets['single'] = numa.getInfo()[self.numa_number]['cores'][5]
        self.__core_sets['dspsr'] = numa.getInfo()[self.numa_number]['cores'][6:10]

        log.debug("Subprocess core settings:")
        for k,v in self.__core_sets.items():
            log.debug(" - {}: {}".format(k, ",".join(v)))

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

        self._time_processed = Sensor.string(
            "_time_processed",
            description="_time_processed",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._time_processed)

        self._time_processed = Sensor.string(
            "_time_processed",
            description="_time_processed",
            default="N/A",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._time_processed)

#        if "F1" in values:
#            spin_period = 1000.0 / float(values["F"])
#            self._spin_period.set_value(spin_period) # spin period in ms
#        if "DM" in values:
#            self._dm.set_value(float(values["DM"]))
#        if "PB" in values:
#            self._pb.set_value(24.0 * float(values["PB"])) # obrital period in hrs


    def _decode_capture_stdout(self, stdout, callback):
        log.debug('{}'.format(str(stdout)))


    def _error_treatment(self, callback):
        self.stop_pipeline_with_mkrecv_crashed()


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


    @coroutine
    def _png_monitor(self):
        try:
            processed_seconds = int(
                os.popen("ls {}/*ar | wc -l".format(self.in_path)).read())
            self._time_processed.set_value(
                "{} s".format(processed_seconds * 10))
            log.info("processed {}s".format(processed_seconds * 10))
        except Exception as error:
            log.debug(error)
        log.info("reading png from : {}".format(self.out_path))
        try:
            log.info("reading {}/fscrunch.png".format(self.out_path))
            with open("{}/fscrunch.png".format(self.out_path), "rb") as imageFile:
                image_fscrunch = base64.b64encode(imageFile.read())
                self._fscrunch.set_value(image_fscrunch)
        except Exception as error:
            log.debug(error)
        try:
            log.info("reading {}/tscrunch.png".format(self.out_path))
            with open("{}/tscrunch.png".format(self.out_path), "rb") as imageFile:
                image_tscrunch = base64.b64encode(imageFile.read())
                self._tscrunch.set_value(image_tscrunch)
        except Exception as error:
            log.debug(error)
        try:
            log.info("reading {}/profile.png".format(self.out_path))
            with open("{}/profile.png".format(self.out_path), "rb") as imageFile:
                image_profile = base64.b64encode(imageFile.read())
                self._profile.set_value(image_profile)
        except Exception as error:
            log.debug(error)
        return


    @coroutine
    def _create_ring_buffer(self, bufferSize, blocks, key, numa_node):
        """
        @brief Create a ring buffer of given size with given key on specified numa node.
               Adds and register an appropriate sensor to thw list
        """
        # always clear buffer first. Allow fail here
        yield command_watcher("dada_db -d -k {key}".format(key=key), allow_fail=True)

        cmd = "numactl --cpubind={numa_node} --membind={numa_node} dada_db -k {key} -n {blocks} -b {bufferSize} -p -l".format(
            key=key, blocks=blocks, bufferSize=bufferSize, numa_node=numa_node)
        log.debug("Running command: {0}".format(cmd))
        yield command_watcher(cmd)

        #M = DbMonitor(key, self._buffer_status_handle)
        # M.start()
        #self._dada_buffers.append({'key': key, 'monitor': M})


    @coroutine
    def _reset_ring_buffer(self, key, numa_node):
        """
        @brief Create a ring buffer of given size with given key on specified numa node.
               Adds and register an appropriate sensor to thw list
        """
        # always clear buffer first. Allow fail here
        cmd = "numactl --cpubind={numa_node} --membind={numa_node} dbreset -k {key} --log_level debug".format(
            numa_node=numa_node, key=key)
        log.debug("Running command: {0}".format(cmd))
        yield command_watcher(cmd, allow_fail=True)


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
        self.sync_epoch = self._config["input_data_streams"]["polarization_0"]["sync_time"]
        log.info("sync_epoch = {}".format(self.sync_epoch))
        yield self._create_ring_buffer(self._config["db_params"]["size"], self._config["db_params"]["number"], "dada", self.numa_number)
        yield self._create_ring_buffer(self._config["db_params"]["size"], self._config["db_params"]["number"], "dadc", self.numa_number)

        self.epta_dir = os.path.join("/mnt/", self._config["epta_directory"])
        if not os.path.isdir(self.epta_dir):
            log.error("Not a directory {} !".format(self.epta_dir))
            raise RuntimeError("Epta directory is no directory: {}".format(self.epta_dir))

        self.state = "ready"
        log.info("Pipeline configured")


    @coroutine
    def measurement_prepare(self, config_json):
        log.info("checking status")
        if self.state != "ready":
            log.debug("pipeline is not in ready state")
            if self.state == "running":
                log.debug(
                    "pipeline is still configuring, issuing stop now and will start shortly")
                yield self.measurement_stop()
            if self.state == "configuring":
                log.debug("pipeline is starting, do not send multiple start")
                return
        self._subprocessMonitor = SubprocessMonitor()
        self.state = "measurement_starting"
        config_json = json.loads(config_json)
        cfs = json.dumps(config_json, indent=4)
        log.info("Final configuration:\n" + cfs)
        #
        self._source_name = config_json["source-name"]
        self.nchan = config_json["nchannels"]
        self.nbins = config_json["nbins"]
        central_freq = self._config['input_data_streams'][
            'polarization_0']["central_freq"]
        #Check if source is a pulsar or calibrator, if not error

        epta_file = os.path.join(self.epta_dir, '{}.par'.format(self._source_name[1:]))
        log.debug("Checking epta file {}".format(epta_file))
        self.pulsar_flag = is_accessible(epta_file)
        if ((parse_tag(self._source_name) == "default") or (parse_tag(self._source_name) != "R")) and (not self.pulsar_flag):
            if (parse_tag(self._source_name) != "FB"):
                error = "source is not pulsar or calibrator"
                raise EddPulsarPipelineError(error)

        log.info("starting pipeline")
        self._state = "measurement_starting"
        self._timer = Time.now()

        #Setting blank image
        self._fscrunch.set_value(BLANK_IMAGE)
        self._tscrunch.set_value(BLANK_IMAGE)
        self._profile.set_value(BLANK_IMAGE)

        #writing mkrecv header
        self._central_freq.set_value(str(central_freq))
        self._source_name_sensor.set_value(self._source_name)
        self._nchannels.set_value(self.nchan)
        self._nbins.set_value(self.nbins)

        self.cuda_number = numa.getInfo()[self.numa_number]['gpus'][0]
        c = SkyCoord("{} {}".format(self._config['source_config'][
                     "ra"], self._config['source_config']["dec"]), unit=(u.deg, u.deg))
        header = self._config["dada_header_params"]
        header["ra"] = c.to_string("hmsdms").split(" ")[0].replace(
            "h", ":").replace("m", ":").replace("s", "")
        header["dec"] = c.to_string("hmsdms").split(" ")[1].replace(
            "d", ":").replace("m", ":").replace("s", "")
        header["key"] = self._dada_buffers[0]
        header["mc_source"] = self._config['input_data_streams']['polarization_0'][
            "ip"] + "," + self._config['input_data_streams']['polarization_1']["ip"]
        header["frequency_mhz"] = central_freq
        bandwidth = self._config['input_data_streams']['polarization_0'][
            "sample_rate"] / self._config['input_data_streams']['polarization_0']["predecimation_factor"] / 2 / 1e6
        header["bandwidth"] = bandwidth
        header["mc_streaming_port"] = self._config[
            'input_data_streams']['polarization_0']["port"]
        header["interface"] = numa.getFastestNic(self.numa_number)[1]['ip']
        header["sync_time"] = self.sync_epoch
        header["sample_clock"] = float(self._config['input_data_streams']['polarization_0'][
                                       "sample_rate"] / self._config['input_data_streams']['polarization_0']["predecimation_factor"])
        header["tsamp"] = 1 / (2.0 * bandwidth)
        header["source_name"] = self._source_name
        header["obs_id"] = "{0}_{1}".format(
            sensors["scannum"], sensors["subscannum"])
        tstr = Time.now().isot.replace(":", "-")
        tdate = tstr.split("T")[0]
        ####################################################
        #SETTING UP THE INPUT AND SCRUNCH DATA DIRECTORIES #
        ####################################################
        try:
            self.in_path = os.path.join("/mnt/dspsr_output/",
                                        tdate, self._source_name, str(central_freq), tstr, "raw_data")
            self.out_path = os.path.join(
                "/mnt/dspsr_output/", tdate, self._source_name, str(central_freq), tstr, "combined_data")
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
        self.pulsar_flag_with_R = is_accessible(os.path.join(self.epta_dir, '{}.par'.format(self._source_name[1:-2])))
        log.debug("{}".format((parse_tag(self._source_name) == "default") & self.pulsar_flag))
        if (parse_tag(self._source_name) == "default") & is_accessible(epta_file):
            cmd = 'numactl -m {} taskset -c {} tempo2 -f {} -pred'.format(
                self.numa_number, self.__core_sets['single'],
                epta_file).split()

            # ToDo: Hardcoded values for Effelsberg have to go
            cmd.append("Effelsberg {} {} {} {} 24 2 3599.999999999".format(Time.now().mjd - 1, Time.now().mjd + 1, float(
                central_freq) - 200, float(central_freq) + 200))
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

        #porting pulsar pars to katcp sensors by reading the par file.
        #values = {}
        #with open(epta_file) as fh:
        #    for line in fh:
        #        par, value = filter(None, line.strip().split(' '))[0:2]
        #        values[par] = value.strip()
        #if "F" in values:
        #    spin_period = 1000.0 / float(values["F"])
        #    self._spin_period.set_value(spin_period) # spin period in ms
        #if "F1" in values:
        #    spin_period = 1000.0 / float(values["F1"])
        #    self._spin_period.set_value(spin_period) # spin period in ms
        #if "DM" in values:
        #    self._dm.set_value(float(values["DM"]))
        #if "PB" in values:
        #    self._pb.set_value(24.0 * float(values["PB"])) # obrital period in hrs

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
            if self._state == "measurement_starting":
                log.debug("pipeline is starting, do not send multiple start")
                return
        ####################################################
        #STARTING DSPSR                                    #
        ####################################################
        os.chdir(self.in_path)
        log.debug("pulsar_flag = {}".format(self.pulsar_flag))
        log.debug("source_name = {}".format(
            self._source_name))

        epta_file = os.path.join(self.epta_dir, '{}.par'.format(self._source_name[1:]))
        if (parse_tag(self._source_name) == "default") and self.pulsar_flag:
            cmd = "numactl -m {numa} dspsr {args} {nchan} {nbin} -fft-bench -x 8192 -cpu {cpus} -cuda {cuda_number} -P {predictor} -N {name} -E {parfile} {keyfile}".format(
                numa=self.numa_number,
                args=self._config["dspsr_params"]["args"],
                nchan="-F {}:D".format(self.nchan),
                nbin="-b {}".format(self.nbins),
                name=self._source_name,
                predictor="/tmp/t2pred.dat",
                parfile=epta_file,
                cpus=",".join(self.__core_sets['dspsr']),
                cuda_number=self.cuda_number,
                keyfile=self.dada_key_file.name)

        elif parse_tag(self._source_name) == "R":
            cmd = "numactl -m {numa} dspsr -L 10 -c 1.0 -D 0.0001 -r -minram 1024 -fft-bench {nchan} -cpu {cpus} -N {name} -cuda {cuda_number}  {keyfile}".format(
                numa=self.numa_number,
                args=self._config["dspsr_params"]["args"],
                nchan="-F {}:D".format(
                    self.nchan),
                name=self._source_name,
                cpus=",".join(self.__core_sets['dspsr']),
                cuda_number=self.cuda_number,
                keyfile=self.dada_key_file.name)

#        elif parse_tag(self._source_name) == "FB":
#            cmd = "numactl -m {numa} taskset -c {cpus} digifil -threads 4 -F {nchan} -b8 -d 1 -I 0 -t {nbins} {keyfile}".format(
#                numa=self.numa_number,
# nchan="{}".format(self._config['source_config']["nchannels"]),
#                nbin="{}".format(self._config['source_config']["nbins"]),
#                cpus=self.cpu_numbers,
#                keyfile=self.dada_key_file.name)
        else:
            error = "source is unknown"
            raise EddPulsarPipelineError(error)
        """
        elif (parse_tag(self._source_name) == "R") and (not self.pulsar_flag) and (not self.pulsar_flag_with_R):
            if (self._source_name[:2] == "3C" and self._source_name[-3:] == "O_R") or (self._source_name[:3] == "NGC" and self._source_name[-4:]=="ON_R"):
                cmd = "numactl -m {numa} dspsr -L 10 -c 1.0 -D 0.0001 -r -minram 1024 -set type=FluxCal-On -fft-bench {nchan} -cpu {cpus} -N {name} -cuda {self.cuda_number}  {keyfile}".format(
                    numa=self.numa_number,
                    args=self._config["dspsr_params"]["args"],
                    nchan="-F {}:D".format(self._config['source_config']["nchannels"]),
                    name=self._source_name,
                    cpus=self.cpu_numbers,
                    self.cuda_number=self.cuda_number,
                    keyfile=dada_key_file.name)
            elif (self._source_name[:3] == "NGC" and self._source_name[-5:] == "OFF_R") or (self._source_name[:2] == "3C" and self._source_name[-3:] == "N_R") or (self._source_name[:2] == "3C" and self._source_name[-3:] == "S_R"):
                cmd = "numactl -m {numa} dspsr -L 10 -c 1.0 -D 0.0001 -r -minram 1024 -set type=FluxCal-Off -fft-bench {nchan} -cpu {cpus} -N {name} -cuda {self.cuda_number}  {keyfile}".format(
                    numa=self.numa_number,
                    args=self._config["dspsr_params"]["args"],
                    nchan="-F {}:D".format(self._config['source_config']["nchannels"]),
                    name=self._source_name,
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
            numa=self.numa_number, cpu=self.__core_sets['single'])
        log.debug("Running command: {0}".format(cmd))
        log.info("Staring EDDPolnMerge")
        self._polnmerge_proc = ManagedProcess(cmd)
        self._subprocessMonitor.add(
            self._polnmerge_proc, self._subprocess_error)
        ####################################################
        #STARTING MKRECV                                   #
        ####################################################
        cmd = "numactl -m {numa} taskset -c {cpu} mkrecv_rnt --header {dada_header} --quiet".format(
            numa=self.numa_number, cpu=",".join(self.__core_sets['mkrecv']), dada_header=self.dada_header_file.name)
        log.debug("Running command: {0}".format(cmd))
        log.info("Staring MKRECV")
        self._mkrecv_ingest_proc = ManagedProcess(cmd)
        self._subprocessMonitor.add(
            self._mkrecv_ingest_proc, self._subprocess_error)
        ####################################################
        #STARTING ARCHIVE MONITOR                          #
        ####################################################
        # cmd = "python /src/mpikat/mpikat/effelsberg/edd/pipeline/archive_directory_monitor.py -i {} -o {}".format(
        #    self.in_path, self.out_path)
        #log.debug("Running command: {0}".format(cmd))
        log.info("Staring archive monitor")
        self.archive_observer = Observer()
        self.archive_observer.daemon = False
        log.info("Input directory: {}".format(self.in_path))
        log.info("Output directory: {}".format(self.out_path))
        log.info("Setting up ArchiveAdder handler")
        self.handler = ArchiveAdder(self.out_path)
#        self.handler.update_freq_zaplist(self._freq_zaplist_sensor.value())
#        self.handler.update_time_zaplist(self._time_zaplist_sensor.value())
        self.archive_observer.schedule(
            self.handler, self.in_path, recursive=False)
        log.info("Starting directory monitor")
        self.archive_observer.start()

        #self._archive_directory_monitor = ManagedProcess(cmd)
        # self._subprocessMonitor.add(
        #    self._archive_directory_monitor, self._subprocess_error)
        self._png_monitor_callback = tornado.ioloop.PeriodicCallback(
            self._png_monitor, 5000)
        self._png_monitor_callback.start()
        self._subprocessMonitor.start()
        self._timer = Time.now() - self._timer
        log.debug("Took {} s to start".format(self._timer * 86400))
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
        log.debug("Stopping")
        self._png_monitor_callback.stop()
        process = [self._mkrecv_ingest_proc,
                   self._polnmerge_proc]
        for proc in process:
            proc.terminate(timeout=1)
        if (parse_tag(self._source_name) == "default") & self.pulsar_flag:
            os.remove("/tmp/t2pred.dat")
        log.info("reset DADA buffer")
        log.info("Resetting dadc buffer")
        # yield self._reset_ring_buffer("dadc", self.numa_number)
        #cmd = "dbreset -k {0} --log_level debug".format("dadc")
        #log.debug("Running command: {0}".format(cmd))
        # yield command_watcher(cmd)
        yield self._create_ring_buffer(self._config["db_params"]["size"], self._config["db_params"]["number"], "dada", self.numa_number)
        yield self._create_ring_buffer(self._config["db_params"]["size"], self._config["db_params"]["number"], "dadc", self.numa_number)
        del self._subprocessMonitor
        self._state = "ready"
        log.info("Pipeline Stopped")


    @coroutine
    def deconfigure(self):
        """@brief deconfigure the dspsr pipeline."""
        log.info("Deconfiguring pipeline")
        log.debug("Destroying dada buffers")

        for k in self._dada_buffers:
            # k['monitor'].stop()
            cmd = "dada_db -d -k {0}".format(k)
            log.debug("Running command: {0}".format(cmd))
            yield command_watcher(cmd)
        # self._fscrunch.set_value(BLANK_IMAGE)
        # self._tscrunch.set_value(BLANK_IMAGE)
        # self._profile.set_value(BLANK_IMAGE)
        log.info("Deconfigured pipeline")
        self._state = "idle"


    @request(Str())
    @return_reply()
    def request_freq_zaplist(self, req, zaplist):
        """
        @brief      Add freq zaplist

        """
        @coroutine
        def zaplist_wrapper():
            try:
                yield self.freq_zaplist(zaplist)
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(zaplist_wrapper)
        raise AsyncReply


    def freq_zaplist(self, zaplist):
        """
        @brief     Add zap list to Katcp sensor
        """
        self._freq_zaplist_sensor.set_value(zaplist)
        try:
            self.handler.update_freq_zaplist(zaplist)
        except:
            pass
        return


    @request(Str())
    @return_reply()
    def request_time_zaplist(self, req, zaplist):
        """
        @brief      Add freq zaplist

        """
        @coroutine
        def zaplist_wrapper():
            try:
                yield self.time_zaplist(zaplist)
            except Exception as error:
                log.exception(str(error))
                req.reply("fail", str(error))
            else:
                req.reply("ok")
        self.ioloop.add_callback(zaplist_wrapper)
        raise AsyncReply


    def time_zaplist(self, zaplist):
        """
        @brief     Add zap list to Katcp sensor
        """
        self._time_zaplist_sensor.set_value(zaplist)
        try:
            self.handler.update_time_zaplist(zaplist)
        except:
            pass
        return


if __name__ == "__main__":
    launchPipelineServer(EddPulsarPipeline)
