#!/usr/bin/env python

import coloredlogs
import ConfigParser
import json
import numpy as np
import socket
import struct
import time
import shlex
from subprocess import PIPE, Popen, check_output
from inspect import currentframe, getframeinfo
from astropy.time import Time
from astropy.coordinates import EarthLocation, SkyCoord, AltAz
import astropy.units as units
import logging
from katcp import Sensor
import argparse
import threading
import inspect
import os
from math import floor
from fcntl import fcntl, F_GETFL, F_SETFL
from os import O_NONBLOCK
import sys
import sympy as sym
from sympy.physics.vector import vlatex
from sympy.printing import latex
from astropy.coordinates import ICRS, Galactic, FK4, FK5
import math
from astropy.utils import iers

iers.conf.auto_download = False  # Turn the download off
iers.conf.auto_max_age = None    # Enable prediction

# Updates:
# 1. Check the directory exist or not, create it if not;
# 2. Check the header template exist or not, raise error if not;
# 3. The cleanup works as expected now;
# 4. Add callbacks for stderr and returncode for class ExecuteCommand, done;
# 5. Check the memory size before execute, done;
# 6. Cleanup the log, almost done;
# 7. Remove print, done
# 8. add reuseaddr in pipeline and also capture, done
# 9. self._utc_start_process, to put the dada_dbregister into capture does not help, now synced, more test;
# 10. add lock to counter to protect it
# 11. Remove NVPROF, SOD
# 12. Unblock the stdout and stderr
# 13. Lock to pretect the handle
# 14. Use the same header template for different configurations
# 15. capture software also updated to use the same header
# 16. Force to error clean
# 17. Read from json and check the parameters there
# 18. Created modes for March run
# 19. Fresh iers data base at configure

log = logging.getLogger('mpikat.effelsberg.paf.pipeline')
log.setLevel('DEBUG')
# log.setLevel('INFO')

# Configuration of input for different number of beams
INPUT_1BEAM = {"input_nbeam":                  1,
               "input_nchunk_per_port":        16,
               "input_ports":                  [[17100, 17101, 17102]]
               }

INPUT_2BEAM = {"input_nbeam":                  2,
               "input_nchunk_per_port":       11,
               "input_ports":                 [[17100, 17101, 17102], [17103, 17104, 17105]]
               }

# We can turn the configuratio directory here to json in future
# Configuration of PAF system, including GPU servers
SYSTEM_CONFIG = {"paf_nchan_per_chunk":    	     7,        # MHz
                 "paf_over_samp_rate":               (32.0 / 27.0),
                 "paf_period":                       27,       # Seconds
                 "paf_ndf_per_chunk_per_period":     250000,
                 "paf_nsamp_per_df":                 128,
                 "paf_nchunk_per_beam":              48,

                 "paf_df_res":                       1.08E-4,  # Seconds
                 "paf_df_dtsz":      	             7168,     # Bytes
                 "paf_df_pktsz":     	             7232,
                 "paf_df_hdrsz":     	             64,
                 "paf_freq":                         [950.5, 1340.5, 1550.5],

                 "pacifix_ncpu_per_numa_node":       10,
                 "pacifix_memory_limit_per_numa_node":  60791751475,  # has 10% spare
                 }

# Configuration for pipelines
PIPELINE_CONFIG = {"execution":                    1,
                   "root_software":                "/phased-array-feed/",
                   #"root_software":                "/home/pulsar/xinping/phased-array-feed/",
                   "root_runtime":                 "/beegfs/DENG/",
                   "rbuf_ndf_per_chunk_per_block": 16384,  # For all ring buffers
                   "tbuf_ndf_per_chunk_per_block": 128,  # Only need for capture

                   # Configuration of input
                   "input_source_default":       "UNKNOW_00:00:00.00_00:00:00.00",
                   "input_dada_hdr_fname":       "dada_header_template_PAF.txt",
                   # To put baseband data from file
                   "input_keys":                 ["dada", "dadc"],
                   "input_nblk":                 5,
                   "input_nreader":              1,
                   "input_nbyte":                2,
                   "input_npol":                 2,
                   "input_ndim":                 2,
                   "input_software_name":        "capture_main",
                   "input_cpu_bind":             1,
                   "input_pad":                  0,
                   "input_check_ndf_per_chunk":  1024,

                   # Configuration of GPU kernels
                   "gpu_ndf_per_chunk_per_stream": 1024,
                   "gpu_nstream":                  2,

                   # To put filterbank data
                   "search_keys":         ["dbda", "dbdc"],
                   "search_nblk":         200,
                   "search_nchan":        1024,
                   "search_nchan":        512,
                   "search_cufft_nx":     128,
                   "search_cufft_nx":     256,
                   "search_nbyte":        1,
                   "search_npol":         1,
                   "search_ndim":         1,
                   "search_heimdall":     0,
                   "search_dbdisk":       1,
                   "search_monitor":      1,
                   "search_spectrometer": 1,
                   "search_detect_thresh": 10,
                   "search_dm":           [1, 3000],
                   "search_zap_chans":    [],
                   "search_software_name": "baseband2filterbank_main",

                   # To put independent or commensal spectral data
                   "spectrometer_keys":           ["dcda", "dcdc"],
                   "spectrometer_nblk":           2,
                   "spectrometer_nreader":        1,
                   "spectrometer_cufft_nx":       1024,
                   "spectrometer_nbyte":          4,
                   "spectrometer_ndata_per_samp": 4,
                   "spectrometer_ptype":          2,
                   #"spectrometer_ip":    	  '239.3.1.2',
                   #"spectrometer_port":    	  2,
                   "spectrometer_ip":             "10.17.0.2",
                   "spectrometer_port":           17106,
                   "spectrometer_dbdisk":         0,
                   "spectrometer_monitor":        1,
                   "spectrometer_accumulate_nblk": 3,
                   "spectrometer_software_name":  "baseband2spectral_main",

                   # Spectral parameters for the simultaneous spectral output from fold and search mode
                   # The rest configuration of this output is the same as the
                   # normal spectrometer mode
                   "simultaneous_spectrometer_start_chunk":    26,
                   "simultaneous_spectrometer_nchunk":         5,

                   # To put processed baseband data
                   "fold_keys":           ["ddda", "dddc"],
                   "fold_nblk":           2,
                   "fold_cufft_nx":       64,
                   "fold_nbyte":          1,
                   "fold_npol":           2,
                   "fold_ndim":           2,
                   "fold_dspsr":          1,
                   "fold_dbdisk":         0,
                   "fold_monitor":        1,
                   "fold_spectrometer":   0,
                   "fold_subint":         10,
                   "fold_software_name":  "baseband2baseband_main",

                   # To put monitor data
                   "monitor_keys":            ["deda", "dedc"],
                   #"monitor_ip":      	      '239.3.1.1',
                   #"monitor_port":           2,
                   "monitor_ip":             "10.17.0.1",
                   "monitor_port":            17107,
                   "monitor_ptype":           2,

                   "tel_lat":                 50.524722,
                   "tel_lon":                 6.883611,
                   "tel_alt":                 0,

                   }

PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "error"]

# Epoch of BMF timing system, it updates every 0.5 year, [UTC datetime,
# EPOCH in BMF packet header]
EPOCHS = [
    [Time("2025-07-01T00:00:00", format='isot', scale='utc'), 51],
    [Time("2025-01-01T00:00:00", format='isot', scale='utc'), 50],
    [Time("2024-07-01T00:00:00", format='isot', scale='utc'), 49],
    [Time("2024-01-01T00:00:00", format='isot', scale='utc'), 48],
    [Time("2023-07-01T00:00:00", format='isot', scale='utc'), 47],
    [Time("2023-01-01T00:00:00", format='isot', scale='utc'), 46],
    [Time("2022-07-01T00:00:00", format='isot', scale='utc'), 45],
    [Time("2022-01-01T00:00:00", format='isot', scale='utc'), 44],
    [Time("2021-07-01T00:00:00", format='isot', scale='utc'), 43],
    [Time("2021-01-01T00:00:00", format='isot', scale='utc'), 42],
    [Time("2020-07-01T00:00:00", format='isot', scale='utc'), 41],
    [Time("2020-01-01T00:00:00", format='isot', scale='utc'), 40],
    [Time("2019-07-01T00:00:00", format='isot', scale='utc'), 39],
    [Time("2019-01-01T00:00:00", format='isot', scale='utc'), 38],
    [Time("2018-07-01T00:00:00", format='isot', scale='utc'), 37],
    [Time("2018-01-01T00:00:00", format='isot', scale='utc'), 36],
]


class PipelineError(Exception):
    pass

PIPELINES = {}


def register_pipeline(name):
    def _register(cls):
        PIPELINES[name.lower()] = cls
        return cls
    return _register


class ExecuteCommand(object):

    def __init__(self, command, execution=True, process_index=None):
        self._command = command
        self._execution = execution
        self._process_index = process_index
        self.stdout_callbacks = set()
        self.returncode_callbacks = set()
        self._monitor_threads = []
        self._process = None
        self._executable_command = None
        self._stdout = None
        self._returncode = None
        self._terminate_event = threading.Event()
        self._terminate_event.clear()

        log.debug(self._command)
        self._executable_command = shlex.split(self._command)

        if self._execution:
            try:
                self._process = Popen(self._executable_command,
                                      stdout=PIPE,
                                      stderr=PIPE,
                                      bufsize=1,
                                      universal_newlines=True)
                flags = fcntl(self._process.stdout, F_GETFL)  # Noblock
                fcntl(self._process.stdout, F_SETFL, flags | O_NONBLOCK)
                flags = fcntl(self._process.stderr, F_GETFL)
                fcntl(self._process.stderr, F_SETFL, flags | O_NONBLOCK)

            except Exception as error:
                log.exception("Error while launching command: {} with error {}".format(
                    self._command, error))
                self.returncode = self._command + "; RETURNCODE is: ' 1'"
            if self._process == None:
                self.returncode = self._command + "; RETURNCODE is: ' 1'"

            # Start monitors
            self._monitor_threads.append(
                threading.Thread(target=self._process_monitor))

            for thread in self._monitor_threads:
                thread.start()

    def __del__(self):
        class_name = self.__class__.__name__

    def finish(self):
        if self._execution:
            for thread in self._monitor_threads:
                thread.join()

    def terminate(self):
        if self._execution:
            if self._process.poll() == None:
                self._terminate_event.set()
                self._process.terminate()

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

    def returncode_notify(self):
        for callback in self.returncode_callbacks:
            callback(self._returncode, self)

    @property
    def returncode(self):
        return self._returncode

    @returncode.setter
    def returncode(self, value):
        self._returncode = value
        self.returncode_notify()

    def _process_monitor(self):
        if self._execution:
            while self._process.poll() == None and (not self._terminate_event.is_set()):
                try:
                    stdout = self._process.stdout.readline().rstrip("\n\r")
                    if stdout != b"":
                        if self._process_index != None:
                            self.stdout = stdout + \
                                "; PROCESS_INDEX is " + \
                                str(self._process_index)
                        else:
                            self.stdout = stdout
                except:
                    pass

                try:
                    stderr = self._process.stderr.readline().rstrip("\n\r")
                    if stderr != b"":
                        pass
                except:
                    pass

            if self._process.returncode and (not self._terminate_event.is_set()):
                self.returncode = self._command + \
                    "; RETURNCODE is: " +\
                    str(self._process.returncode)


class Pipeline(object):

    def __init__(self):
        self._sensors = []
        self.callbacks = set()
        self._ready_counter = 0
        self._aberrant_counter = 0
        self._ready_lock = threading.Lock()
        self._aberrant_lock = threading.Lock()
        self.setup_sensors()

        self._paf_period = SYSTEM_CONFIG["paf_period"]
        self._paf_df_res = SYSTEM_CONFIG["paf_df_res"]
        self._paf_freq = SYSTEM_CONFIG["paf_freq"]
        self._paf_df_dtsz = SYSTEM_CONFIG["paf_df_dtsz"]
        self._paf_df_pktsz = SYSTEM_CONFIG["paf_df_pktsz"]
        self._paf_df_hdrsz = SYSTEM_CONFIG["paf_df_hdrsz"]
        self._paf_over_samp_rate = SYSTEM_CONFIG["paf_over_samp_rate"]
        self._paf_nsamp_per_df = SYSTEM_CONFIG["paf_nsamp_per_df"]
        self._paf_nchan_per_chunk = SYSTEM_CONFIG["paf_nchan_per_chunk"]
        self._paf_nchunk_per_beam = SYSTEM_CONFIG["paf_nchunk_per_beam"]
        self._paf_ndf_per_chunk_per_period = SYSTEM_CONFIG[
            "paf_ndf_per_chunk_per_period"]
        self._pacifix_ncpu_per_numa_node = SYSTEM_CONFIG[
            "pacifix_ncpu_per_numa_node"]
        self._pacifix_memory_limit_per_numa_node = SYSTEM_CONFIG[
            "pacifix_memory_limit_per_numa_node"]

        self._execution = PIPELINE_CONFIG["execution"]
        self._root_software = PIPELINE_CONFIG["root_software"]
        self._root_runtime = PIPELINE_CONFIG["root_runtime"]
        self._rbuf_ndf_per_chunk_per_block = PIPELINE_CONFIG[
            "rbuf_ndf_per_chunk_per_block"]
        self._tbuf_ndf_per_chunk_per_block = PIPELINE_CONFIG[
            "tbuf_ndf_per_chunk_per_block"]
        self._rbuf_blk_res = self._paf_df_res * self._rbuf_ndf_per_chunk_per_block
        self._rbuf_nsamp_per_chan_per_block = self._rbuf_ndf_per_chunk_per_block * \
            self._paf_nsamp_per_df

        self._input_dada_hdr_fname = PIPELINE_CONFIG["input_dada_hdr_fname"]
        self._input_dada_hdr_fname = "{}/config/{}".format(
            self._root_software, self._input_dada_hdr_fname)
        self._input_source_default = PIPELINE_CONFIG["input_source_default"]
        self._input_keys = PIPELINE_CONFIG["input_keys"]
        self._input_nblk = PIPELINE_CONFIG["input_nblk"]
        self._input_nreader = PIPELINE_CONFIG["input_nreader"]
        self._input_nbyte = PIPELINE_CONFIG["input_nbyte"]
        self._input_npol = PIPELINE_CONFIG["input_npol"]
        self._input_ndim = PIPELINE_CONFIG["input_ndim"]
        self._gpu_ndf_per_chunk_per_stream = PIPELINE_CONFIG[
            "gpu_ndf_per_chunk_per_stream"]
        self._gpu_nstream = PIPELINE_CONFIG["gpu_nstream"]
        self._input_check_ndf_per_chunk = PIPELINE_CONFIG[
            "input_check_ndf_per_chunk"]

        self._input_software_name = PIPELINE_CONFIG["input_software_name"]
        self._input_main = "{}/src/{}".format(
            self._root_software, self._input_software_name)
        self._input_cpu_bind = PIPELINE_CONFIG["input_cpu_bind"]
        self._input_pad = PIPELINE_CONFIG["input_pad"]

        self._monitor_keys = PIPELINE_CONFIG["monitor_keys"]
        self._monitor_ip = PIPELINE_CONFIG["monitor_ip"]
        self._monitor_port = PIPELINE_CONFIG["monitor_port"]
        self._monitor_ptype = PIPELINE_CONFIG["monitor_ptype"]

        self._spectrometer_keys = PIPELINE_CONFIG["spectrometer_keys"]
        self._spectrometer_nblk = PIPELINE_CONFIG["spectrometer_nblk"]
        self._spectrometer_nreader = PIPELINE_CONFIG["spectrometer_nreader"]
        self._spectrometer_cufft_nx = PIPELINE_CONFIG["spectrometer_cufft_nx"]
        self._spectrometer_nbyte = PIPELINE_CONFIG["spectrometer_nbyte"]
        self._spectrometer_ptype = PIPELINE_CONFIG["spectrometer_ptype"]
        self._spectrometer_ndata_per_samp = PIPELINE_CONFIG[
            "spectrometer_ndata_per_samp"]
        self._spectrometer_ip = PIPELINE_CONFIG["spectrometer_ip"]
        self._spectrometer_port = PIPELINE_CONFIG["spectrometer_port"]
        self._spectrometer_dbdisk = PIPELINE_CONFIG["spectrometer_dbdisk"]
        self._spectrometer_monitor = PIPELINE_CONFIG["spectrometer_monitor"]
        self._spectrometer_accumulate_nblk = PIPELINE_CONFIG[
            "spectrometer_accumulate_nblk"]
        self._spectrometer_nchan_keep_per_chan = self._spectrometer_cufft_nx / \
            self._paf_over_samp_rate
        if self._spectrometer_dbdisk:
            self._spectrometer_sod = 1

        self._spectrometer_software_name = PIPELINE_CONFIG[
            "spectrometer_software_name"]
        self._spectrometer_main = "{}/src/{}".format(
            self._root_software, self._spectrometer_software_name)

        self._simultaneous_spectrometer_start_chunk = PIPELINE_CONFIG[
            "simultaneous_spectrometer_start_chunk"]
        self._simultaneous_spectrometer_nchunk = PIPELINE_CONFIG[
            "simultaneous_spectrometer_nchunk"]
        self._simultaneous_spectrometer_nchan        = self._simultaneous_spectrometer_nchunk *\
            self._paf_nchan_per_chunk

        self._search_keys = PIPELINE_CONFIG["search_keys"]
        self._search_nblk = PIPELINE_CONFIG["search_nblk"]
        self._search_nchan = PIPELINE_CONFIG["search_nchan"]
        self._search_cufft_nx = PIPELINE_CONFIG["search_cufft_nx"]
        self._search_nbyte = PIPELINE_CONFIG["search_nbyte"]
        self._search_npol = PIPELINE_CONFIG["search_npol"]
        self._search_ndim = PIPELINE_CONFIG["search_ndim"]
        self._search_heimdall = PIPELINE_CONFIG["search_heimdall"]
        self._search_dbdisk = PIPELINE_CONFIG["search_dbdisk"]
        self._search_monitor = PIPELINE_CONFIG["search_monitor"]
        self._search_spectrometer = PIPELINE_CONFIG["search_spectrometer"]
        self._search_sod = self._search_heimdall or self._search_dbdisk
        self._search_nreader       = (self._search_heimdall + self._search_dbdisk) \
            if (self._search_heimdall + self._search_dbdisk) else 1
        self._search_detect_thresh = PIPELINE_CONFIG["search_detect_thresh"]
        self._search_dm = PIPELINE_CONFIG["search_dm"]
        self._search_zap_chans = PIPELINE_CONFIG["search_zap_chans"]
        self._search_nchan_keep_per_chan = self._search_cufft_nx / self._paf_over_samp_rate

        self._search_software_name = PIPELINE_CONFIG["search_software_name"]
        self._search_main = "{}/src/{}".format(
            self._root_software, self._search_software_name)

        self._tel_lon = PIPELINE_CONFIG["tel_lon"]
        self._tel_lat = PIPELINE_CONFIG["tel_lat"]
        self._tel_alt = PIPELINE_CONFIG["tel_alt"]

        self._fold_keys = PIPELINE_CONFIG["fold_keys"]
        self._fold_nblk = PIPELINE_CONFIG["fold_nblk"]
        self._fold_cufft_nx = PIPELINE_CONFIG["fold_cufft_nx"]
        self._fold_nbyte = PIPELINE_CONFIG["fold_nbyte"]
        self._fold_npol = PIPELINE_CONFIG["fold_npol"]
        self._fold_ndim = PIPELINE_CONFIG["fold_ndim"]
        self._fold_dspsr = PIPELINE_CONFIG["fold_dspsr"]
        self._fold_dbdisk = PIPELINE_CONFIG["fold_dbdisk"]
        self._fold_monitor = PIPELINE_CONFIG["fold_monitor"]
        self._fold_spectrometer = PIPELINE_CONFIG["fold_spectrometer"]
        self._fold_subint = PIPELINE_CONFIG["fold_subint"]
        self._fold_nchan_keep_per_chan = self._fold_cufft_nx / self._paf_over_samp_rate
        self._fold_sod = self._fold_dspsr or self._fold_dbdisk
        self._fold_nreader   = (self._fold_dspsr + self._fold_dbdisk) \
            if (self._fold_dspsr + self._fold_dbdisk) else 1
        self._fold_software_name = PIPELINE_CONFIG["fold_software_name"]
        self._fold_main = "{}/src/{}".format(
            self._root_software, self._fold_software_name)

        # To see if we can process input data with integer repeats
        if self._rbuf_ndf_per_chunk_per_block % \
           (self._gpu_ndf_per_chunk_per_stream * self._gpu_nstream):
            log.error("data in input ring buffer block can only "
                      "be processed by baseband2baseband with integer repeats")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("data in input ring buffer block can only "
                                "be processed by baseband2baseband with integer repeats")

        # To see if the dada header template file is there
        if not os.path.isfile(self._input_dada_hdr_fname):
            log.error("{} is not there".format(self._input_dada_hdr_fname))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("{} is not there".format(
                self._input_dada_hdr_fname))

        self._cleanup_commands_config = ["pkill -9 -f capture",
                                         "pkill -9 -f dspsr",
                                         "pkill -9 -f dada_db",
                                         "pkill -9 -f heimdall",
                                         "pkill -9 -f dada_diskdb",
                                         "pkill -9 -f dada_dbdisk",
                                         # process name, maximum 16 bytes
                                         # (15 bytes visiable)
                                         "pkill -9 -f baseband2filter",
                                         # process name, maximum 16 bytes
                                         # (15 bytes visiable)
                                         "pkill -9 -f baseband2spectr",
                                         # process name, maximum 16 bytes
                                         # (15 bytes visiable)
                                         "pkill -9 -f baseband2baseba",
                                         "ipcrm -a"]

        self._cleanup_commands_start = ["pkill -9 -f dspsr",
                                        "pkill -9 -f dada_db",
                                        "pkill -9 -f heimdall",
                                        "pkill -9 -f dada_diskdb",
                                        "pkill -9 -f dada_dbdisk",
                                        # process name, maximum 16 bytes (15
                                        # bytes visiable)
                                        "pkill -9 -f baseband2filter",
                                        # process name, maximum 16 bytes (15
                                        # bytes visiable)
                                        "pkill -9 -f baseband2spectr",
                                        "pkill -9 -f baseband2baseba"]

        # Cleanup at very beginning
        self._cleanup(self._cleanup_commands_config)

    def __del__(self):
        class_name = self.__class__.__name__

    def notify(self):
        for callback in self.callbacks:
            callback(self._state, self)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        self._state = value
        self.notify()

    def configure(self):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def deconfigure(self):
        raise NotImplementedError

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        self._beam_sensor0 = Sensor.float(
            "beam0.id",
            description="The ID of current beam",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._beam_sensor0)

        self._instant_sensor0 = Sensor.float(
            "beam0.inst-packet-loss-fraction",
            description="The instanteous packet loss fraction",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._instant_sensor0)

        self._time_sensor0 = Sensor.float(
            "beam0.time-elapsed",
            description="The time so far in seconds",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._time_sensor0)

        self._average_sensor0 = Sensor.float(
            "beam0.total-packet-loss-fraction",
            description="Fraction of packets lost",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._average_sensor0)

        self._beam_sensor1 = Sensor.float(
            "beam1.id",
            description="The ID of current beam",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._beam_sensor1)

        self._instant_sensor1 = Sensor.float(
            "beam1.inst-packet-loss-fraction",
            description="The instanteous packet loss fraction",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._instant_sensor1)

        self._time_sensor1 = Sensor.float(
            "beam1.time-elapsed",
            description="The time so far in seconds",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._time_sensor1)

        self._average_sensor1 = Sensor.float(
            "beam1.total-packet-loss-fraction",
            description="Fraction of packets lost",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._average_sensor1)

    @property
    def sensors(self):
        return self._sensors

    def _terminate_execution_instances(self):
        for execution_instance in self._execution_instances:
            execution_instance.terminate()

    def _refresh_iers(self):
        '''
        The IERS data are managed via a instances of the IERS_Auto class.
        These instances are created internally within the relevant time and coordinate objects during transformations.
        If the astropy data cache does not have the required IERS data file then astropy will request the file from the IERS service.
        This will occur the first time such a transform is done for a new setup or on a new machine.
        '''
        t = Time('2016:001')
        t.ut1

    def _coord_convertion_thread(self):
        # This takes couple fo seconds, put it here while we are wait for the
        # "ready_counter"
        for i in range(self._input_nbeam):
            # Beam coordinate
            beam_ra, beam_dec = self._coord_convertion(self._utc_start_process, self._source_ra, self._source_dec,
                                                       self._beam_alt_d, self._beam_az_d, self._input_beam_index[i])
            self._beam_ra.append(beam_ra)
            self._beam_dec.append(beam_dec)

    def _rotation_matrix(self, angle, d):
        directions = {
            "x": [1, 0, 0],
            "y": [0, 1, 0],
            "z": [0, 0, 1]
        }
        direction = np.array(directions[d])
        sina = sym.sin(angle)
        cosa = sym.cos(angle)
        R = sym.Matrix([[cosa, 0, 0], [0, cosa, 0], [0, 0, cosa]])
        R += sym.Matrix(np.outer(direction, direction)) * (1 - cosa)
        direction = sym.Matrix(direction)
        direction *= sina
        R += sym.Matrix([[0,           -direction[2],  direction[1]],
                         [direction[2], 0,             -direction[0]],
                         [-direction[1], direction[0],   0]])
        return R

    def _coord_convertion(self, time, ra, dec, beam_alt_d, beam_az_d, beam_id):
        site = EarthLocation(lat=self._tel_lat * units.deg,
                             lon=self._tel_lon * units.deg,
                             height=self._tel_alt * units.m)
        sc = SkyCoord(np.array(float(ra)), np.array(float(dec)),
                      unit='deg', frame='icrs', equinox="J2000")
        aa_frame = AltAz(obstime=time, location=site)
        beamzero_altaz = sc.transform_to(aa_frame)

        daz, delv = sym.symbols('Azd Elvd')
        azel_vec = self._position_vector(daz, delv)
        az, elv = sym.symbols('Az Elv')
        R = self._rotation_matrix(az, "z") * self._rotation_matrix(elv, "y")
        dp = R * azel_vec

        final_azelv = dp.subs({az: beamzero_altaz.az.radian,
                               elv: beamzero_altaz.alt.radian,
                               daz: (beam_az_d[beam_id] / 180.0) * np.pi,
                               delv: (beam_alt_d[beam_id] / 180.0) * np.pi})
        a, b, c = np.array(final_azelv).astype("float64")
        final_elv = np.arcsin(c)
        final_az = (np.arctan2(b, a))
        beam_pos = SkyCoord(final_az,
                            np.abs(final_elv),
                            unit='radian',
                            frame='altaz',
                            location=site,
                            obstime=time)
        beam_ra, beam_dec = beam_pos.icrs.ra.to_string(unit=units.hourangle, sep=":"), \
            beam_pos.icrs.dec.to_string(unit=units.degree, sep=":")

        return beam_ra[0], beam_dec[0]

    def _position_vector(self, a, b):
        return sym.Matrix([[sym.cos(b) * sym.cos(a)],
                           [sym.cos(b) * sym.sin(a)],
                           [sym.sin(b)]])

    def _acquire_beam_index(self, ip, port, ndf_check_chk):
        """
        To get the beam ID
        """
        data = bytearray(self._paf_df_pktsz)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Force to timeout after one data frame period
        sock.settimeout(1)
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            beam_index = []
            for i in range(ndf_check_chk):
                nbyte, address = sock.recvfrom_into(data, self._paf_df_pktsz)
                data_uint64 = np.fromstring(str(data), 'uint64')
                hdr_uint64 = np.uint64(struct.unpack(
                    "<Q", struct.pack(">Q", data_uint64[2]))[0])
                beam_index.append(hdr_uint64 & np.uint64(0x000000000000ffff))

            if (len(list(set(beam_index))) > 1):
                log.error("Beams are mixed up, please check the routing table")
                self._terminate_execution_instances()
                self._cleanup(self._cleanup_commands_config)
                self.state = "error"
                raise PipelineError(
                    "Beams are mixed up, please check the routing table")
            sock.close()
            return beam_index[0]
        except Exception as error:
            log.exception(error)
            sock.close()
            log.error("{} fail".format(inspect.stack()[0][3]))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _synced_startbuf(self, utc_start_process, utc_start_capture):
        if(utc_start_process < utc_start_capture):
            log.error("utc_start_process should be later than utc_start_capture")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "utc_start_process should be later than utc_start_capture")

        delta_time = utc_start_process.unix - utc_start_capture.unix
        start_buf = int(floor(delta_time / self._rbuf_blk_res)
                        ) - 1   # The start buf, to be safe -1;

        sleep_time = utc_start_process.unix - Time.now().unix
        log.debug(
            "SLEEP TIME to wait for START BUF block is {} seconds".format(sleep_time))

        if(sleep_time < 0):
            log.error("Too late to start process")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Too late to start process")

        time.sleep(sleep_time)  # Sleep until we are ready to go
        return start_buf

    def _synced_refinfo(self, utc_start_capture, ip, port):
        # Capture one packet to see what is current epoch, seconds and idf
        # We need to do that because the seconds is not always matched with
        # estimated value
        epoch_ref, sec_ref, idf_ref = self._refinfo(ip, port)

        while utc_start_capture.unix > (epoch_ref * 86400.0 + sec_ref + self._paf_period):
            sec_ref = sec_ref + self._paf_period
        while utc_start_capture.unix < (epoch_ref * 86400.0 + sec_ref):
            sec_ref = sec_ref - self._paf_period

        idf_ref = (utc_start_capture.unix - epoch_ref *
                   86400.0 - sec_ref) / self._paf_df_res

        return epoch_ref, sec_ref, int(idf_ref)

    def _refinfo(self, ip, port):
        """
        To get reference information for capture
        """
        data = bytearray(self._paf_df_pktsz)
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Force to timeout after one data frame period
        sock.settimeout(1)
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            nbyte, address = sock.recvfrom_into(data, self._paf_df_pktsz)
            data = np.fromstring(str(data), 'uint64')

            hdr_part = np.uint64(struct.unpack(
                "<Q", struct.pack(">Q", data[0]))[0])
            sec_ref = (hdr_part & np.uint64(
                0x3fffffff00000000)) >> np.uint64(32)
            idf_ref = hdr_part & np.uint64(0x00000000ffffffff)

            hdr_part = np.uint64(struct.unpack(
                "<Q", struct.pack(">Q", data[1]))[0])
            epoch_idx = (hdr_part & np.uint64(
                0x00000000fc000000)) >> np.uint64(26)

            for epoch in EPOCHS:
                if epoch[1] == epoch_idx:
                    break
            epoch_ref = int(epoch[0].unix / 86400.0)

            sock.close()

            return epoch_ref, int(sec_ref), int(idf_ref)
        except Exception as error:
            log.exception(error)
            sock.close()
            log.error("{} fail".format(inspect.stack()[0][3]))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _check_beam_connection(self, destination, ndf_check_chk):
        """
        To check the connection of one beam with given ip and port numbers
        """
        nport = len(destination)
        alive = np.zeros(nport, dtype=int)
        nchk_alive = np.zeros(nport, dtype=int)

        destination_dead = []   # The destination where we can not receive data
        destination_alive = []   # The destination where we can receive data
        for i in range(nport):
            ip = destination[i].split("_")[0]
            port = int(destination[i].split("_")[1])
            alive, nchk_alive = self._check_port_connection(
                ip, port, ndf_check_chk)

            if alive == 1:
                destination_alive.append(
                    destination[i] + "_{}".format(nchk_alive))
            else:
                destination_dead.append(destination[i])

        if (len(destination_alive) == 0):  # No alive ports, error
            log.error("The stream is not alive")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("The stream is not alive")

        return destination_alive, destination_dead

    def _check_port_connection(self, ip, port, ndf_check_chk):
        """
        To check the connection of single port
        """
        alive = 1
        nchk_alive = 0
        data = bytearray(self._paf_df_pktsz)
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Force to timeout after one data frame period
        sock.settimeout(1)
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            nbyte, address = sock.recvfrom_into(data, self._paf_df_pktsz)
            if (nbyte != self._paf_df_pktsz):
                alive = 0
            else:
                source = []
                alive = 1
                for i in range(ndf_check_chk):
                    buf, address = sock.recvfrom(self._paf_df_pktsz)

                    data_uint64 = np.fromstring(str(buf), 'uint64')
                    hdr_uint64 = np.uint64(struct.unpack(
                        "<Q", struct.pack(">Q", data_uint64[2]))[0])

                    source.append(address)
                nchk_alive = len(set(source))
            sock.close()
        except Exception as error:
            log.exception(error)
            log.error("{} fail".format(inspect.stack()[0][3]))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))
        return alive, nchk_alive

    def _capture_control(self, ctrl_socket, command, socket_address):
        if self._execution:
            try:
                ctrl_socket.sendto(command, socket_address)
            except Exception as error:
                log.exception(error)
                log.error("{} fail".format(inspect.stack()[0][3]))
                self._terminate_execution_instances()
                self._cleanup(self._cleanup_commands_config)
                self.state = "error"
                raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _handle_execution_returncode(self, returncode, callback):
        if self._execution:
            if returncode:
                self.state = "error"
                log.error(returncode)
                self._terminate_execution_instances()
                self._cleanup(self._cleanup_commands_config)
                raise PipelineError(returncode)

    def _handle_execution_stdout(self, stdout, callback):
        if self._execution:
            pass

    def _ready_counter_callback(self, stdout, callback):
        if self._execution:
            log.debug(stdout)
            if stdout.find("READY") != -1:
                self._ready_lock.acquire()
                self._ready_counter += 1
                self._ready_lock.release()

    def _process_status_callback(self, stdout, callback):
        if self._execution:
            if stdout.find("finished") != -1:
                log.debug(stdout)
                log.info(stdout)

    def _capture_status_callback(self, stdout, callback):
        if self._execution:
            log.debug(stdout)
            if stdout.find("CAPTURE_STATUS") != -1:
                process_index = int(stdout.split(" ")[-1])
                capture_status = stdout.split(";")[0].split(" ")
                if process_index == 0:
                    self._beam_sensor0.set_value(
                        float(self._input_beam_index[0]))
                    self._time_sensor0.set_value(float(capture_status[1]))
                    self._average_sensor0.set_value(float(capture_status[2]))
                    self._instant_sensor0.set_value(float(capture_status[3]))
                if process_index == 1:
                    self._beam_sensor1.set_value(
                        float(self._input_beam_index[1]))
                    self._time_sensor1.set_value(float(capture_status[1]))
                    self._average_sensor1.set_value(float(capture_status[2]))
                    self._instant_sensor1.set_value(float(capture_status[3]))

    def _cleanup(self, cleanup_commands):
        execution_instances = []
        for command in cleanup_commands:
            execution_instances.append(
                ExecuteCommand(command, self._execution))
        for execution_instance in execution_instances:         # Wait until the cleanup is done
            execution_instance.finish()


@register_pipeline("Fold")
class Fold(Pipeline):

    def __init__(self):
        super(Fold, self).__init__()
        self.state = "idle"

    def configure(self, config_json, input_config):
        log.info("Received 'CONFIGURE' command")
        self._execution_instances = []
        self._input_execution_instances = []

        self._fold_commands = []
        self._fold_create_rbuf_commands = []
        self._fold_delete_rbuf_commands = []
        self._fold_dbdisk_commands = []

        self._spectrometer_create_rbuf_commands = []
        self._spectrometer_delete_rbuf_commands = []
        self._spectrometer_dbdisk_commands = []

        self._input_beam_index = []
        self._input_socket_address = []
        self._input_control_socket = []
        self._pipeline_runtime_directory = []
        self._input_commands = []
        self._input_create_rbuf_commands = []
        self._input_delete_rbuf_commands = []

        if (self.state != "idle"):
            log.error("Can only configure pipeline in idle state")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Can only configure pipeline in idle state")
        log.info("Configuring")

        # Refresh IERS database to save the time on "start" for coordinate
        # conversion
        self._refresh_iers()

        # Setup parameters of the pipeline
        self.state = "configuring"
        self._input_config = input_config
        self._config_info = json.loads(config_json)
        self._config_nchunk = self._config_info['nbands']
        self._config_ip = self._config_info["ip_address"]
        self._config_nchunk_offset = self._config_info["band_offset"]
        self._config_freq = self._config_info["frequency"]
        self._fold_spectrometer = self._config_info["fold_spectrometer"]
        if self._fold_spectrometer == 1:
            self._simultaneous_spectrometer_nchunk = self._config_info[
                "zoomnbands"]
            self._simultaneous_spectrometer_start_chunk = self._config_info[
                "zoomband0"]

        beam_alt_d = np.array(self._config_info["beam_alt_d"])
        beam_az_d = np.array(self._config_info["beam_az_d"])
        self._beam_alt_d = -1 * (beam_alt_d != 0) * beam_alt_d
        self._beam_az_d = -1 * (beam_az_d != 0) * beam_az_d

        self._pacifix_numa = int(self._config_ip.split(".")[3]) - 1
        self._utc_start_capture = Time(
            self._config_info["utc_start_capture"], format='isot', scale='utc')

        self._input_nbeam = self._input_config["input_nbeam"]
        self._input_nchunk_per_port = self._input_config[
            "input_nchunk_per_port"]
        self._input_ports = self._input_config["input_ports"]
        self._input_nport = len(self._input_ports[0])
        self._input_nchunk = self._input_nport * self._input_nchunk_per_port
        self._input_nchan = self._input_nchunk * self._paf_nchan_per_chunk

        # Check the frequency information in the configuration
        if self._config_freq not in self._paf_freq:
            log.error("The config frequency should be {}, but it is {}".format(
                self._paf_freq, self._config_freq))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("The config frequency should be {}, but it is {}".format(
                self._paf_freq, self._config_freq))
        if self._input_nchunk != self._config_nchunk:
            log.error("The config band number is not right, it should be {}, but it is {}".format(
                self._input_nchunk, self._config_nchunk))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("The config band number is not right, it should be {}, but it is {}".format(
                self._input_nchunk, self._config_nchunk))
        self._start_chunk = int(math.floor(
            (self._paf_nchunk_per_beam - self._input_nchunk) / 2.0))
        self._first_chunk = self._start_chunk + self._config_nchunk_offset
        self._last_chunk = self._start_chunk + \
            self._input_nchunk + self._config_nchunk_offset
        if ((self._first_chunk < 0) or (self._last_chunk) > self._paf_nchunk_per_beam):
            log.error("Required frequency chunks are out of range")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Required frequency chunks are out of range")

        self._freq         = self._config_freq +  \
            0.5 * (self._last_chunk + self._first_chunk - self._paf_nchunk_per_beam) *\
            self._paf_nchan_per_chunk

        # Buffer size
        self._input_blksz = self._input_nchunk * \
            self._paf_df_dtsz * \
            self._rbuf_ndf_per_chunk_per_block
        self._fold_blksz = int(self._input_blksz * self._fold_nbyte * self._fold_npol * self._fold_ndim /
                               (self._paf_over_samp_rate * self._input_nbyte * self._input_npol * self._input_ndim))

        # To see if we have enough memory
        self._simultaneous_spectrometer_blksz = self._simultaneous_spectrometer_nchan * \
            self._spectrometer_nchan_keep_per_chan * \
            self._spectrometer_ndata_per_samp * \
            self._spectrometer_nbyte * \
            (self._spectrometer_dbdisk and self._fold_spectrometer)

        if self._input_nbeam * (self._input_blksz * self._input_nblk +
                                self._input_nchunk *
                                self._paf_df_dtsz *
                                self._tbuf_ndf_per_chunk_per_block +
                                self._fold_blksz * self._fold_nblk +
                                self._simultaneous_spectrometer_blksz * self._spectrometer_nblk) >\
                self._pacifix_memory_limit_per_numa_node:
            log.error("We do not have enough shared memory for the setup "
                      "Try to reduce the ring buffer block number, or  "
                      "reduce the number of packets in each ring buffer block, or "
                      "reduce the number of frequency chunks for spectral (if there is any)")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("We do not have enough shared memory for the setup "
                                "Try to reduce the ring buffer block number, or  "
                                "reduce the number of packets in each ring buffer block, or "
                                "reduce the number of frequency chunks for spectral (if there is any)")

        # To see if we can fit FFT into input samples
        if self._rbuf_nsamp_per_chan_per_block % self._fold_cufft_nx:
            log.error(
                "self._rbuf_nsamp_per_chan_per_block should be multiple times of self._fold_cufft_nx")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "self._rbuf_nsamp_per_chan_per_block should be multiple times of self._fold_cufft_nx")
        if self._fold_spectrometer and (self._rbuf_nsamp_per_chan_per_block % self._spectrometer_cufft_nx):
            log.error(
                "self._rbuf_nsamp_per_chan_per_block should be multiple times of self._spectrometer_cufft_nx")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "self._rbuf_nsamp_per_chan_per_block should be multiple times of self._spectrometer_cufft_nx")

        # To check pol type
        if self._fold_monitor and (self._monitor_ptype not in [1, 2, 4]):
            log.error("Monitor pol type should be 1, 2 or 4, but it is {}".format(
                self._monitor_ptype))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "Monnitor pol type should be 1, 2 or 4, but it is {}".format(self._monitor_ptype))
        if self._fold_spectrometer and (self._spectrometer_ptype not in [1, 2, 4]):
            log.error("Spectrometer pol type should be 1, 2 or 4, but it is {}".format(
                self._spectrometer_ptype))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Spectrometer pol type should be 1, 2 or 4, but it is {}".format(
                self._spectrometer_ptype))

        # To check existing of files
        if not os.path.isfile(self._fold_main):
            log.error("{} is not exist".format(self._fold_main))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("{} is not exist".format(self._fold_main))

        # To setup commands for each process
        for i in range(self._input_nbeam):
            if self._execution:
                # To setup address
                destination = []
                for port in self._input_ports[i]:
                    destination.append("{}_{}_{}".format(
                        self._config_ip, port, self._input_nchunk_per_port))

                destination_alive, dead_info = self._check_beam_connection(
                    destination, self._input_check_ndf_per_chunk)
                first_alive_ip = destination_alive[0].split("_")[0]
                first_alive_port = int(destination_alive[0].split("_")[1])

                beam_index = self._acquire_beam_index(
                    first_alive_ip, first_alive_port, self._input_check_ndf_per_chunk)
                refinfo = self._synced_refinfo(
                    self._utc_start_capture, first_alive_ip, first_alive_port)

                # To get directory for data and socket for control
                pipeline_runtime_directory = "{}/beam{:02}".format(
                    self._root_runtime, beam_index)
                if not os.path.isdir(pipeline_runtime_directory):
                    try:
                        os.makedirs(pipeline_runtime_directory)
                    except Exception as error:
                        log.exception(error)
                        log.error("Fail to create {}".format(
                            pipeline_runtime_directory))
                        self._terminate_execution_instances()
                        self._cleanup(self._cleanup_commands_config)
                        self.state = "error"
                        raise PipelineError(
                            "Fail to create {}".format(pipeline_runtime_directory))

                socket_address = "{}/capture.socket".format(
                    pipeline_runtime_directory)
                # If the socket is there, remove it to be safe
                if os.path.isfile(socket_address):
                    try:
                        os.remove(socket_address)
                    except Exception as error:
                        log.exception(error)
                        log.error(
                            "Fail to remove {} before create a new one".format(socket_address))
                        self._terminate_execution_instances()
                        self._cleanup(self._cleanup_commands_config)
                        self.state = "error"
                        raise PipelineError(
                            "Fail to remove {} before create a new one".format(socket_address))
                control_socket = socket.socket(
                    socket.AF_UNIX, socket.SOCK_DGRAM)
                control_socket.setsockopt(
                    socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            else:
                destination_alive = []
                dead_info = []

                beam_index = i
                refinfo = [0, 0, 0]

                socket_address = None
                control_socket = None
                runtime_directory = None

            self._input_beam_index.append(beam_index)
            self._pipeline_runtime_directory.append(pipeline_runtime_directory)
            self._input_socket_address.append(socket_address)
            self._input_control_socket.append(control_socket)

            # To setup CPU bind information and dead_info
            self._pacifix_ncpu_per_instance = self._pacifix_ncpu_per_numa_node / self._input_nbeam
            cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node + \
                i * self._pacifix_ncpu_per_instance
            alive_info = []
            for info in destination_alive:
                alive_info.append("{}_{}".format(info, cpu))
                cpu += 1

            buf_control_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node + \
                i * self._pacifix_ncpu_per_instance + self._input_nport
            capture_control_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node + \
                i * self._pacifix_ncpu_per_instance + self._input_nport
            capture_control = "1_{}".format(capture_control_cpu)
            refinfo = "{}_{}_{}".format(refinfo[0], refinfo[1], refinfo[2])

            # capture command
            command = ("{} -a {} -b {} -c {} -e {} -f {} -g {} -i {} -j {} "
                       "-k {} -l {} -m {} -n {} -o {} -p {} -q {} ").format(
                           self._input_main, self._input_keys[
                               i], self._paf_df_hdrsz, " -c ".join(alive_info),
                           self._freq, refinfo, pipeline_runtime_directory, buf_control_cpu, capture_control,
                           self._input_cpu_bind, self._rbuf_ndf_per_chunk_per_block, self._tbuf_ndf_per_chunk_per_block,
                           self._input_dada_hdr_fname, self._input_source_default, self._input_pad, beam_index)
            self._input_commands.append(command)

            # Command to create input ring buffer
            dadadb_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                (i + 1) * self._pacifix_ncpu_per_instance - 1
            self._input_create_rbuf_commands.append(("taskset -c {} dada_db -l -p -k {:} "
                                                     "-b {:} -n {:} -r {:}").format(dadadb_cpu, self._input_keys[i],
                                                                                    self._input_blksz,
                                                                                    self._input_nblk,
                                                                                    self._input_nreader))

            # command to create fold ring buffer
            self._fold_create_rbuf_commands.append(("taskset -c {} dada_db -l -p -k {:} "
                                                    "-b {:} -n {:} -r {:}").format(dadadb_cpu, self._fold_keys[i],
                                                                                   self._fold_blksz,
                                                                                   self._fold_nblk,
                                                                                   self._fold_nreader))

            # Command to create spectrometer ring buffer as required
            if self._fold_spectrometer and self._spectrometer_dbdisk:
                command = ("taskset -c {} dada_db -l -p -k {:} "
                           "-b {:} -n {:} -r {:}").format(dadadb_cpu, self._spectrometer_keys[i],
                                                          self._simultaneous_spectrometer_blksz,
                                                          self._spectrometer_nblk,
                                                          self._spectrometer_nreader)
                self._spectrometer_create_rbuf_commands.append(command)

            # Setup baseband2baseband
            fold_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                (i + 1) * self._pacifix_ncpu_per_instance - 1
            command = ("taskset -c {} {} -a {} -b {} -c {} -d {} "
                       "-e {} -f {} -g {} -i {} ").format(fold_cpu, self._fold_main, self._input_keys[i],
                                                          self._fold_keys[
                                                              i], self._rbuf_ndf_per_chunk_per_block,
                                                          self._gpu_nstream, self._gpu_ndf_per_chunk_per_stream,
                                                          self._pipeline_runtime_directory[
                                                              i],
                                                          self._input_nchunk, self._fold_cufft_nx)
            if self._fold_sod:
                command += "-j 1 "
            else:
                command += "-j 0 "
            if self._fold_monitor:
                command += "-k Y_{}_{}_{} ".format(self._monitor_ip,
                                                   self._monitor_port,
                                                   self._monitor_ptype)
            else:
                command += "-k N "
            self._fold_commands.append(command)

            # Command to run dbdisk for fold output as required
            if self._fold_dbdisk:
                dbdisk_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                    (i + 1) * self._pacifix_ncpu_per_instance - 1
                self._fold_dbdisk_commands.append("taskset -c {} dada_dbdisk -W -k {} -D "
                                                  "{} -o -s -z".format(dbdisk_cpu, self._fold_keys[i],
                                                                       self._pipeline_runtime_directory[i]))

            # Command to run dbdisk for spectrometer output
            if self._fold_spectrometer and self._spectrometer_dbdisk:
                dbdisk_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                    (i + 1) * self._pacifix_ncpu_per_instance - 1
                command = ("taskset -c {} dada_dbdisk -W -k {} "
                           "-D {} -o -s -z").format(dbdisk_cpu, self._spectrometer_keys[i],
                                                    self._pipeline_runtime_directory[i])
                self._spectrometer_dbdisk_commands.append(command)

            # command to delete input ring buffer
            self._input_delete_rbuf_commands.append(
                "taskset -c {} dada_db -d -k {:}".format(dadadb_cpu, self._input_keys[i]))

            # command to delete fold ring buffer
            self._fold_delete_rbuf_commands.append(
                "taskset -c {} dada_db -d -k {:}".format(dadadb_cpu, self._fold_keys[i]))

            # command to delete spectrometer ring buffer
            if self._fold_spectrometer and self._spectrometer_dbdisk:
                command = "taskset -c {} dada_db -d -k {:}".format(
                    dadadb_cpu, self._spectrometer_keys[i])
                self._spectrometer_delete_rbuf_commands.append(command)

        # Create baseband ring buffer
        process_index = 0
        execution_instances = []
        for command in self._input_create_rbuf_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1
        for execution_instance in execution_instances:
            execution_instance.finish()

        # Execute the capture
        process_index = 0
        self._ready_counter = 0
        for command in self._input_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._input_execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1

        if self._execution:  # Ready when all capture threads and the capture control thread of all capture instances are ready
            while self.state != "error":
                if self._ready_counter == (self._input_nport + 1) * self._input_nbeam:
                    break

        # Remove ready_counter_callback and add capture_status_callback
        for execution_instance in self._input_execution_instances:
            execution_instance.stdout_callbacks.remove(
                self._ready_counter_callback)
            execution_instance.stdout_callbacks.add(
                self._capture_status_callback)

        self.state = "ready"
        log.info("Ready")

    def start(self, status_json):
        log.info("Received 'START' command")
        if self.state != "ready":
            log.error("Pipeline can only be started from ready state")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "Pipeline can only be started from ready state")

        self.state = "starting"
        log.info("Starting")

        self._beam_ra = []
        self._beam_dec = []
        self._fold_dspsr_commands = []

        self._cleanup(self._cleanup_commands_start)
        self._status_info = json.loads(status_json)
        self._scan_num = self._status_info["scannum"]
        self._sub_scan_num = self._status_info["subscannum"]
        self._utc_start_process = Time(
            self._status_info["utc_start_process"], format='isot', scale='utc')
        self._source_name = self._status_info["source-name"]
        self._source_ra, self._source_dec = float(
            self._status_info['ra']), float(self._status_info['dec'])

        # To start the coord conversion in a thread to save the wait time
        self._fold_coord_convertion = threading.Thread(
            target=self._coord_convertion_thread)
        self._fold_coord_convertion.start()

        for i in range(self._input_nbeam):
            # Command to run dspsr, has to setup in start because we need
            # pulsar name
            if self._fold_dspsr:
                fold_dspsr_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                    (i + 1) * self._pacifix_ncpu_per_instance - 1
                kfname = "{}/dspsr.key".format(
                    self._pipeline_runtime_directory[i])
                kfile = open(kfname, "w")
                kfile.writelines("DADA INFO:\n")
                kfile.writelines("key {:s}\n".format(self._fold_keys[i]))
                kfile.close()
                if not os.path.isfile(kfname):
                    log.error("{} is not exist".format(kfname))
                    self._terminate_execution_instances()
                    self._cleanup(self._cleanup_commands_config)
                    self.state = "error"
                    raise PipelineError("{} is not exist".format(kfname))
                pfname = "/phased-array-feed/config/{}.par".format(
                    self._source_name)
                if not os.path.isfile(pfname):
                    log.error("{} is not exist".format(pfname))
                    self._terminate_execution_instances()
                    self._cleanup(self._cleanup_commands_config)
                    self.state = "error"
                    raise PipelineError("{} is not exist".format(pfname))
                self._fold_dspsr_commands.append(("taskset -c {} dspsr -b 1024 -L {} -A "
                                                  "-E {} -cuda 0,0 {}").format(fold_dspsr_cpu, self._fold_subint,
                                                                               pfname,
                                                                               kfname))

        # Create ring buffer for baseband data
        process_index = 0
        execution_instances = []
        for command in self._fold_create_rbuf_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1
        for execution_instance in execution_instances:         # Wait until the buffer creation is done
            execution_instance.finish()

        # Create ring buffer for simultaneous spectrometer output
        if self._fold_spectrometer and self._spectrometer_dbdisk:
            process_index = 0
            execution_instances = []
            for command in self._spectrometer_create_rbuf_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks(
                    self._handle_execution_returncode)
                execution_instances.append(execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            for execution_instance in execution_instances:
                execution_instance.finish()

        # Run baseband2baseband
        process_index = 0
        self._ready_counter = 0
        self._fold_execution_instances = []
        for command in self._fold_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._execution_instances.append(execution_instance)
            self._fold_execution_instances.append(execution_instance)
            process_index += 1

        # run dbdisk for spectrometer as required
        if self._fold_spectrometer and self._spectrometer_dbdisk:
            process_index = 0
            self._spectrometer_dbdisk_execution_instances = []
            for command in self._spectrometer_dbdisk_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                self._spectrometer_dbdisk_execution_instances.append(
                    execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1

        # Run dspsr
        if self._fold_dspsr:
            process_index = 0
            self._fold_dspsr_execution_instances = []
            for command in self._fold_dspsr_commands:
                if self._input_beam_index[process_index] == 0:
                    execution_instance = ExecuteCommand(
                        command, self._execution, process_index)
                    execution_instance.stdout_callbacks.add(
                        self._handle_execution_stdout)
                    execution_instance.returncode_callbacks.add(
                        self._handle_execution_returncode)
                    self._fold_dspsr_execution_instances.append(
                        execution_instance)
                    self._execution_instances.append(execution_instance)

                process_index += 1

        if self._fold_dbdisk:   # Run dbdisk if required
            process_index = 0
            self._fold_dbdisk_execution_instances = []
            for command in self._fold_dbdisk_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                self._fold_dbdisk_execution_instances.append(
                    execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1

        # Has to get coord at this point
        self._fold_coord_convertion.join()

        # Enable the SOD of baseband ring buffer with given time and then
        # "running"
        if self._execution:
            while self.state != "error":
                if self._ready_counter == self._input_nbeam:
                    break
            process_index = 0
            start_buf = self._synced_startbuf(
                self._utc_start_process, self._utc_start_capture)
            log.debug("START BUF index is {}".format(start_buf))
            for control_socket in self._input_control_socket:
                self._capture_control(control_socket,
                                      "START-OF-DATA_{}_{}_{}_{}-{}_{}".format(
                                          self._source_name,
                                          self._beam_ra[process_index],
                                          self._beam_dec[process_index],
                                          self._scan_num,
                                          self._sub_scan_num,
                                          start_buf),
                                      self._input_socket_address[process_index])
                process_index += 1

        # Remove ready_counter_callback
        for execution_instance in self._fold_execution_instances:
            execution_instance.stdout_callbacks.remove(
                self._ready_counter_callback)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)

        self.state = "running"
        log.info("Running")

    def stop(self):
        log.info("Received 'STOP' command")
        if self.state != "running":
            log.error("Can only stop a running pipeline")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"
        log.info("Stopping")

        if self._execution:
            process_index = 0
            for control_socket in self._input_control_socket:  # Stop data
                self._capture_control(control_socket,
                                      "END-OF-DATA",
                                      self._input_socket_address[process_index])
                process_index += 1
        if self._fold_dspsr:
            process_index = 0
            for execution_instance in self._fold_dspsr_execution_instances:
                if self._input_beam_index[process_index] == 0:
                    execution_instance.finish()
                    process_index += 1

        if self._fold_dbdisk:
            for execution_instance in self._fold_dbdisk_execution_instances:
                execution_instance.finish()

        for execution_instance in self._fold_execution_instances:
            execution_instance.finish()

        # To delete simultaneous spectral output buffer
        if self._fold_spectrometer and self._spectrometer_dbdisk:
            process_index = 0
            execution_instances = []
            for command in self._spectrometer_delete_rbuf_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                execution_instances.append(execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            for execution_instance in execution_instances:
                execution_instance.finish()

        # To delete baseband ring buffer
        process_index = 0
        execution_instances = []
        for command in self._fold_delete_rbuf_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1
        for execution_instance in execution_instances:
            execution_instance.finish()

        self.state = "ready"
        log.info("Ready")

    def deconfigure(self):
        log.info("Receive 'DECONFIGURE' command")
        if self.state not in ["ready", "error"]:
            log.error(
                "Pipeline can only be deconfigured from ready or error state")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "Pipeline can only be deconfigured from ready or error state")
        log.info("Deconfiguring")

        if self.state == "ready":  # Normal deconfigure
            self.state = "deconfiguring"

            # To stop the capture
            if self._execution:
                process_index = 0
                for control_socket in self._input_control_socket:
                    self._capture_control(
                        control_socket, "END-OF-CAPTURE", self._input_socket_address[process_index])
                    process_index += 1
            for execution_instance in self._input_execution_instances:
                execution_instance.finish()

            # To delete baseband ring buffer
            process_index = 0
            execution_instances = []
            for command in self._input_delete_rbuf_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                execution_instances.append(execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            for execution_instance in execution_instances:
                execution_instance.finish()

        else:  # Force deconfigure
            self.state = "deconfiguring"
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)

        self.state = "idle"
        log.info("Idle")


@register_pipeline("Search")
class Search(Pipeline):

    def __init__(self):
        super(Search, self).__init__()
        self.state = "idle"

    def configure(self, config_json, input_config):
        log.info("Received 'CONFIGURE' command")
        log.debug("Config json: {}".format(config_json))
        log.debug("Input config: {}".format(input_config))
        self._execution_instances = []
        self._input_execution_instances = []

        self._input_beam_index = []
        self._input_socket_address = []
        self._input_control_socket = []
        self._pipeline_runtime_directory = []
        self._input_commands = []
        self._input_create_rbuf_commands = []
        self._input_delete_rbuf_commands = []

        self._search_commands = []
        self._search_create_rbuf_commands = []
        self._search_delete_rbuf_commands = []
        self._search_dbdisk_commands = []
        self._search_heimdall_commands = []

        self._spectrometer_create_rbuf_commands = []
        self._spectrometer_delete_rbuf_commands = []
        self._spectrometer_dbdisk_commands = []

        if (self.state != "idle"):
            log.error("Can only configure pipeline in idle state")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Can only configure pipeline in idle state")
        log.info("Configuring")
        log.info("Setup and verify parameters for the pipeline")
        self.state = "configuring"
        # Refresh IERS database to save the time on "start" for coordinate
        # conversion
        self._refresh_iers()

        # Setup parameters of the pipeline
        self._input_config = input_config
        self._config_info = json.loads(config_json)
        self._config_freq = self._config_info["frequency"]
        self._config_ip = self._config_info['ip_address']
        self._config_nchunk = self._config_info['nbands']
        self._config_nchunk_offset = self._config_info["band_offset"]
        self._search_spectrometer = self._config_info["search_spectrometer"]
        if self._search_spectrometer:
            self._simultaneous_spectrometer_nchunk = self._config_info[
                "zoomnbands"]
            self._simultaneous_spectrometer_start_chunk = self._config_info[
                "zoomband0"]

        beam_alt_d = np.array(self._config_info["beam_alt_d"])
        beam_az_d = np.array(self._config_info["beam_az_d"])
        self._beam_alt_d = -1 * (beam_alt_d != 0) * beam_alt_d
        self._beam_az_d = -1 * (beam_az_d != 0) * beam_az_d

        self._pacifix_numa = int(self._config_ip.split(".")[3]) - 1
        self._utc_start_capture = Time(
            self._config_info["utc_start_capture"], format='isot', scale='utc')

        self._input_nbeam = self._input_config["input_nbeam"]
        self._input_nchunk_per_port = self._input_config[
            "input_nchunk_per_port"]
        self._input_ports = self._input_config["input_ports"]
        self._input_nport = len(self._input_ports[0])
        self._input_nchunk = self._input_nport * self._input_nchunk_per_port
        self._input_nchan = self._input_nchunk * self._paf_nchan_per_chunk

        self._search_heimdall = self._input_config["search_heimdall"]
        self._search_dbdisk = self._input_config["search_dbdisk"]
        self._search_sod = self._input_config["search_sod"]
        self._search_nreader = self._input_config["search_nreader"]

        # Check the frequency information in the configuration
        if self._config_freq not in self._paf_freq:
            log.error("The config frequency should be {}, but it is {}".format(
                self._paf_freq, self._config_freq))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("The config frequency should be {}, but it is {}".format(
                self._paf_freq, self._config_freq))
        if self._input_nchunk != self._config_nchunk:
            log.error("The config band number is not right, it should be {}, but it is {}".format(
                self._input_nchunk, self._config_nchunk))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("The config band number is not right, it should be {}, but it is {}".format(
                self._input_nchunk, self._config_nchunk))
        self._start_chunk = int(math.floor(
            (self._paf_nchunk_per_beam - self._input_nchunk) / 2.0))
        self._first_chunk = self._start_chunk + self._config_nchunk_offset
        self._last_chunk = self._start_chunk + \
            self._input_nchunk + self._config_nchunk_offset
        if ((self._first_chunk < 0) or (self._last_chunk) > self._paf_nchunk_per_beam):
            log.error("Required frequency chunks are out of range")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Required frequency chunks are out of range")

        self._freq         = self._config_freq +  \
            0.5 * (self._last_chunk + self._first_chunk - self._paf_nchunk_per_beam) *\
            self._paf_nchan_per_chunk

        # Buffer size
        self._input_blksz = self._input_nchunk * \
            self._paf_df_dtsz * \
            self._rbuf_ndf_per_chunk_per_block
        self._search_blksz = int(self._input_blksz * self._search_nchan *
                                 self._search_nbyte * self._search_npol *
                                 self._search_ndim / float(self._input_nbyte *
                                                           self._input_npol *
                                                           self._input_ndim *
                                                           self._input_nchan *
                                                           self._search_cufft_nx))
        # To see if we have enough memory
        self._simultaneous_spectrometer_blksz = self._simultaneous_spectrometer_nchan * \
            self._spectrometer_nchan_keep_per_chan * \
            self._spectrometer_ndata_per_samp * \
            self._spectrometer_nbyte * \
            (self._spectrometer_dbdisk and self._search_spectrometer)

        if self._input_nbeam * (self._input_blksz * self._input_nblk +
                                self._input_nchunk *
                                self._paf_df_dtsz *
                                self._tbuf_ndf_per_chunk_per_block +
                                self._search_blksz * self._search_nblk +
                                self._simultaneous_spectrometer_blksz * self._spectrometer_nblk) >\
                self._pacifix_memory_limit_per_numa_node:
            log.error("We do not have enough shared memory for the setup "
                      "Try to reduce the ring buffer block number, or  "
                      "reduce the number of packets in each ring buffer block, or "
                      "reduce the number of frequency chunks for spectral (if there is any)")
            self._terminate_execution_instances()
            self._clean(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("We do not have enough shared memory for the setup "
                                "Try to reduce the ring buffer block number, or  "
                                "reduce the number of packets in each ring buffer block, or "
                                "reduce the number of frequency chunks for spectral (if there is any)")

        # To see if we can fit FFT into input samples
        if self._rbuf_nsamp_per_chan_per_block % self._search_cufft_nx:
            log.error(
                "self._rbuf_nsamp_per_chan_per_block should be multiple times of self._search_cufft_nx")
            self._terminate_execution_instances()
            self._clean(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "self._rbuf_nsamp_per_chan_per_block should be multiple times of self._search_cufft_nx")
        if self._search_spectrometer and (self._rbuf_nsamp_per_chan_per_block % self._spectrometer_cufft_nx):
            log.error(
                "self._rbuf_nsamp_per_chan_per_block should be multiple times of self._spectrometer_cufft_nx")
            self._terminate_execution_instances()
            self._clean(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "self._rbuf_nsamp_per_chan_per_block should be multiple times of self._spectrometer_cufft_nx")

        # To check pol type
        if self._search_monitor and (self._monitor_ptype not in [1, 2, 4]):
            log.error("Monitor pol type should be 1, 2 or 4, but it is {}".format(
                self._monitor_ptype))
            self._terminate_execution_instances()
            self._clean(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "Monnitor pol type should be 1, 2 or 4, but it is {}".format(self._monitor_ptype))
        if self._search_spectrometer and (self._spectrometer_ptype not in [1, 2, 4]):
            log.error("Spectrometer pol type should be 1, 2 or 4, but it is {}".format(
                self._spectrometer_ptype))
            self._terminate_execution_instances()
            self._clean(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Spectrometer pol type should be 1, 2 or 4, but it is {}".format(
                self._spectrometer_ptype))

        # To check existing of files
        if not os.path.isfile(self._search_main):
            log.error("{} is not exist".format(self._search_main))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("{} is not exist".format(self._search_main))
        log.info("Setup and verify parameters for the pipeline, DONE")

        log.info("Setup command lines for the pipeline")
        # To setup commands for each process
        for i in range(self._input_nbeam):
            if self._execution:
                # To setup address
                destination = []
                for port in self._input_ports[i]:
                    destination.append("{}_{}_{}".format(
                        self._config_ip, port, self._input_nchunk_per_port))
                log.debug("checking beam connection")
                destination_alive, dead_info = self._check_beam_connection(
                    destination, self._input_check_ndf_per_chunk)
                first_alive_ip = destination_alive[0].split("_")[0]
                first_alive_port = int(destination_alive[0].split("_")[1])
                log.debug("acquire beam connection")
                beam_index = self._acquire_beam_index(
                    first_alive_ip, first_alive_port, self._input_check_ndf_per_chunk)
                refinfo = self._synced_refinfo(
                    self._utc_start_capture, first_alive_ip, first_alive_port)
                log.debug("beam index is {}".format(beam_index))
                # To get directory for data and socket for control
                pipeline_runtime_directory = "{}/beam{:02}".format(
                    self._root_runtime, beam_index)
                if not os.path.isdir(pipeline_runtime_directory):
                    try:
                        os.makedirs(pipeline_runtime_directory)
                    except Exception as error:
                        log.exception(error)
                        log.error("Fail to create {}".format(
                            pipeline_runtime_directory))
                        self._terminate_execution_instances()
                        self._cleanup(self._cleanup_commands_config)
                        self.state = "error"
                        raise PipelineError(
                            "Fail to create {}".format(pipeline_runtime_directory))

                socket_address = "{}/capture.socket".format(
                    pipeline_runtime_directory)
                # If the socket is there, remove it to be safe
                if os.path.isfile(socket_address):
                    try:
                        os.remove(socket_address)
                    except Exception as error:
                        log.exception(error)
                        log.error(
                            "Fail to remove {} before create a new one".format(socket_address))
                        self._terminate_execution_instances()
                        self._cleanup(self._cleanup_commands_config)
                        self.state = "error"
                        raise PipelineError(
                            "Fail to remove {} before create a new one".format(socket_address))
                control_socket = socket.socket(
                    socket.AF_UNIX, socket.SOCK_DGRAM)
                control_socket.setsockopt(
                    socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            else:
                destination_alive = []
                dead_info = []

                beam_index = i
                refinfo = [0, 0, 0]

                socket_address = None
                control_socket = None
                runtime_directory = None
            self._input_beam_index.append(beam_index)
            self._pipeline_runtime_directory.append(pipeline_runtime_directory)
            self._input_socket_address.append(socket_address)
            self._input_control_socket.append(control_socket)

            # To setup CPU bind information and dead_info
            self._pacifix_ncpu_per_instance = self._pacifix_ncpu_per_numa_node / self._input_nbeam
            cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node + \
                i * self._pacifix_ncpu_per_instance
            alive_info = []
            for info in destination_alive:
                alive_info.append("{}_{}".format(info, cpu))
                cpu += 1

            buf_control_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node + \
                i * self._pacifix_ncpu_per_instance + self._input_nport
            capture_control_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node + \
                i * self._pacifix_ncpu_per_instance + self._input_nport
            capture_control = "1_{}".format(capture_control_cpu)
            refinfo = "{}_{}_{}".format(refinfo[0], refinfo[1], refinfo[2])

            # capture command
            command = ("{} -a {} -b {} -c {} -e {} -f {} -g {} -i {} -j {} "
                       "-k {} -l {} -m {} -n {} -o {} -p {} -q {} ").format(
                           self._input_main, self._input_keys[
                               i], self._paf_df_hdrsz, " -c ".join(alive_info),
                           self._freq, refinfo, pipeline_runtime_directory, buf_control_cpu, capture_control,
                           self._input_cpu_bind, self._rbuf_ndf_per_chunk_per_block, self._tbuf_ndf_per_chunk_per_block,
                           self._input_dada_hdr_fname, self._input_source_default, self._input_pad, beam_index)
            self._input_commands.append(command)

            # search command
            search_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                (i + 1) * self._pacifix_ncpu_per_instance - 1
            command = ("taskset -c {} {} -a {} -b {} -c {} -d {} -e {} "
                       "-f {} -i {} -j {} -k {} ").format(
                           search_cpu, self._search_main, self._input_keys[i],
                           self._search_keys[
                               i], self._rbuf_ndf_per_chunk_per_block,
                           self._gpu_nstream, self._gpu_ndf_per_chunk_per_stream, self._pipeline_runtime_directory[
                               i],
                           self._input_nchunk, self._search_cufft_nx, self._search_nchan)

            if self._search_spectrometer:
                if self._spectrometer_dbdisk:
                    command += "-m k_{}_{}_{}_{}_{}_{}_{} ".format(self._spectrometer_keys[i],
                                                                   self._spectrometer_sod,
                                                                   self._spectrometer_ptype,
                                                                   self._simultaneous_spectrometer_start_chunk,
                                                                   self._simultaneous_spectrometer_nchunk,
                                                                   self._spectrometer_accumulate_nblk,
                                                                   self._spectrometer_cufft_nx)
                else:
                    command += "-m n_{}_{}_{}_{}_{}_{}_{} ".format(self._spectrometer_ip,
                                                                   self._spectrometer_port,
                                                                   self._spectrometer_ptype,
                                                                   self._simultaneous_spectrometer_start_chunk,
                                                                   self._simultaneous_spectrometer_nchunk,
                                                                   self._spectrometer_accumulate_nblk,
                                                                   self._spectrometer_cufft_nx)
            else:
                command += "-m N "

            if self._search_sod:
                command += "-g 1 "
            else:
                command += "-g 0 "

            if self._search_monitor:
                command += "-l Y_{}_{}_{} ".format(
                    self._monitor_ip, self._monitor_port, self._monitor_ptype)
            else:
                command += "-l N"
            self._search_commands.append(command)

            # Command to create search ring buffer
            dadadb_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                (i + 1) * self._pacifix_ncpu_per_instance - 1
            command = ("taskset -c {} dada_db -p -l -k {} "
                       "-b {} -n {} -r {}").format(
                           dadadb_cpu, self._search_keys[i],
                           self._search_blksz,
                           self._search_nblk,
                           self._search_nreader)
            self._search_create_rbuf_commands.append(command)

            # Command to create spectrometer ring buffer as required
            if self._search_spectrometer and self._spectrometer_dbdisk:
                command = ("taskset -c {} dada_db -l -p -k {:} "
                           "-b {:} -n {:} -r {:}").format(dadadb_cpu, self._spectrometer_keys[i],
                                                          self._simultaneous_spectrometer_blksz,
                                                          self._spectrometer_nblk,
                                                          self._spectrometer_nreader)
                self._spectrometer_create_rbuf_commands.append(command)

            # command to create input ring buffer
            command = ("taskset -c {} dada_db -p -l -k {} "
                       "-b {} -n {} -r {}").format(
                           dadadb_cpu, self._input_keys[i],
                           self._input_blksz,
                           self._input_nblk,
                           self._input_nreader)
            self._input_create_rbuf_commands.append(command)

            # command to delete filterbank ring buffer
            command = "taskset -c {} dada_db -d -k {}".format(
                dadadb_cpu, self._search_keys[i])
            self._search_delete_rbuf_commands.append(command)

            # command to delete baseband ring buffer
            command = "taskset -c {} dada_db -d -k {}".format(
                dadadb_cpu, self._input_keys[i])
            self._input_delete_rbuf_commands.append(command)

            # command to delete spectrometer ring buffer
            if self._search_spectrometer and self._spectrometer_dbdisk:
                command = "taskset -c {} dada_db -d -k {:}".format(
                    dadadb_cpu, self._spectrometer_keys[i])
                self._spectrometer_delete_rbuf_commands.append(command)

            # Command to run heimdall
            if self._search_heimdall:
                heimdall_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                    (i + 1) * self._pacifix_ncpu_per_instance - 1
                command = ("taskset -c {} heimdall -k {} "
                           "-detect_thresh {} -output_dir {} ").format(
                               heimdall_cpu, self._search_keys[i],
                               self._search_detect_thresh, pipeline_runtime_directory)
                if self._search_zap_chans:
                    zap = ""
                    for search_zap_chan in self._search_zap_chans:
                        zap += " -zap_chans {} {}".format(
                            self._search_zap_chan[0], self._search_zap_chan[1])
                    command += zap
                if self._search_dm:
                    command += "-dm {} {}".format(
                        self._search_dm[0], self._search_dm[1])
                self._search_heimdall_commands.append(command)

            # Command to run dbdisk
            if self._search_dbdisk:
                dbdisk_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                    (i + 1) * self._pacifix_ncpu_per_instance - 1
                command = ("dada_dbdisk -b {} -k {} "
                           "-D {} -o -s -z").format(
                               dbdisk_cpu,
                               self._search_keys[i],
                               pipeline_runtime_directory)
                self._search_dbdisk_commands.append(command)

            # Command to run dbdisk for spectrometer output
            if self._search_spectrometer and self._spectrometer_dbdisk:
                command = ("dada_dbdisk -W -k {} "
                           "-D {} -o -s -z").format(self._spectrometer_keys[i],
                                                    self._pipeline_runtime_directory[i])
                self._spectrometer_dbdisk_commands.append(command)

        log.info("Setup command lines for the pipeline, DONE")

        # Create baseband ring buffer
        log.info("Create baseband ring buffer")
        process_index = 0
        execution_instances = []
        for command in self._input_create_rbuf_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1
        for execution_instance in execution_instances:
            execution_instance.finish()
        log.info("Create baseband ring buffer, DONE")

        # Execute the capture
        log.info("Start capture")
        process_index = 0
        self._ready_counter = 0
        for command in self._input_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._input_execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1

        if self._execution:  # Ready when all capture threads and the capture control thread of all capture instances are ready
            while self.state != "error":
                if self._ready_counter == (self._input_nport + 1) * self._input_nbeam:
                    break
        log.info("Capturing")

        # Remove ready_counter_callback and add capture_status_callback
        for execution_instance in self._input_execution_instances:
            execution_instance.stdout_callbacks.remove(
                self._ready_counter_callback)
            execution_instance.stdout_callbacks.add(
                self._capture_status_callback)

        self.state = "ready"
        log.info("Ready")

    def start(self, status_json):
        log.info("Received 'START' command")
        if self.state != "ready":
            log.error("Pipeline can only be started from ready state")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "Pipeline can only be started from ready state")

        self.state = "starting"
        log.info("Starting")

        self._cleanup(self._cleanup_commands_start)
        self._status_info = json.loads(status_json)
        self._scan_num = self._status_info["scannum"]
        self._sub_scan_num = self._status_info["subscannum"]
        self._utc_start_process = Time(
            self._status_info["utc_start_process"], format='isot', scale='utc')
        self._source_name = self._status_info["source-name"]
        self._source_ra, self._source_dec = float(
            self._status_info['ra']), float(self._status_info['dec'])

        # To start the coord conversion in a thread to save the wait time
        self._beam_ra = []
        self._beam_dec = []
        self._search_coord_convertion = threading.Thread(
            target=self._coord_convertion_thread)
        self._search_coord_convertion.start()

        # Create ring buffer for filterbank data
        log.info("Create filterbank ring buffer")
        process_index = 0
        execution_instances = []
        for command in self._search_create_rbuf_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1
        for execution_instance in execution_instances:         # Wait until the buffer creation is done
            execution_instance.finish()
        log.info("Create filterbank ring buffer, DONE")

        # Create ring buffer for simultaneous spectrometer output
        if self._search_spectrometer and self._spectrometer_dbdisk:
            log.info("Create spectrometer ring buffer")
            process_index = 0
            execution_instances = []
            for command in self._spectrometer_create_rbuf_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                execution_instances.append(execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            for execution_instance in execution_instances:
                execution_instance.finish()
            log.info("Create spectrometer ring buffer, DONE")

        # Run baseband2filterbank
        log.info("Start baseband2filterbank")
        process_index = 0
        self._ready_counter = 0
        self._search_execution_instances = []
        for command in self._search_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._search_execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1

        # run dbdisk for spectrometer as required
        if self._search_spectrometer and self._spectrometer_dbdisk:
            log.info("Start to run dbdisk for spectrometer output")
            process_index = 0
            self._spectrometer_dbdisk_execution_instances = []
            for command in self._spectrometer_dbdisk_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                self._spectrometer_dbdisk_execution_instances.append(
                    execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            log.info("Dbdisk for spectrometer output is running")

        if self._search_heimdall:  # run heimdall if required
            log.info("Start to run heimdall")
            process_index = 0
            self._search_heimdall_execution_instances = []
            for command in self._search_heimdall_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                self._search_heimdall_execution_instances.append(
                    execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            log.info("heimdall is running")

        if self._search_dbdisk:   # Run dbdisk if required
            log.info("Start to run dbdisk for filterbank data")
            process_index = 0
            self._search_dbdisk_execution_instances = []
            for command in self._search_dbdisk_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                self._search_dbdisk_execution_instances.append(
                    execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            log.info("Dbdisk for filterbank is running")

        # Have to get right coord here
        self._search_coord_convertion.join()

        # Enable the SOD of baseband ring buffer with given time and then
        # "running"
        if self._execution:
            while self.state != "error":
                if self._ready_counter == self._input_nbeam:
                    break
            process_index = 0
            start_buf = self._synced_startbuf(
                self._utc_start_process, self._utc_start_capture)
            log.debug("START BUF index is {}".format(start_buf))
            for control_socket in self._input_control_socket:
                self._capture_control(control_socket,
                                      "START-OF-DATA_{}_{}_{}_{}-{}_{}".format(
                                          self._source_name,
                                          self._beam_ra[process_index],
                                          self._beam_dec[process_index],
                                          self._scan_num,
                                          self._sub_scan_num,
                                          start_buf),
                                      self._input_socket_address[process_index])
                process_index += 1
        log.info("Baseband2filterbank is running")

        # Remove ready_counter_callback
        for execution_instance in self._search_execution_instances:
            execution_instance.stdout_callbacks.remove(
                self._ready_counter_callback)
            # execution_instance.stdout_callbacks.add(self._process_status_callback)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)

        self.state = "running"
        log.info("Running")

    def stop(self):
        log.info("Received 'STOP' command")
        if self.state != "running":
            log.error("Can only stop a running pipeline")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"
        log.info("Stopping")

        if self._execution:
            log.info("Send 'END-OF-DATA' command to capture software")
            process_index = 0
            for control_socket in self._input_control_socket:  # Stop data
                self._capture_control(control_socket,
                                      "END-OF-DATA",
                                      self._input_socket_address[process_index])
                process_index += 1
            log.info("Send 'END-OF-DATA' command to capture software, DONE")
            
        for execution_instance in self._search_execution_instances:
            execution_instance.finish()
            log.info("Finish the baseband2filterbank execution")

        if self._search_dbdisk:
            for execution_instance in self._search_dbdisk_execution_instances:
                execution_instance.finish()
            log.info("Finish the search_dbdisk execution")
            
        if self._search_spectrometer and self._spectrometer_dbdisk:
            for execution_instance in self._spectrometer_dbdisk_execution_instances:
                execution_instance.terminate()
            log.info("Finish the spectrometer_dbdisk execution")
            
            
        if self._search_heimdall:
            for execution_instance in self._search_heimdall_execution_instances:
                execution_instance.terminate()
            log.info("Finish the heimdall execution")

        #if self._search_dbdisk:
        #    for execution_instance in self._search_dbdisk_execution_instances:
        #        execution_instance.finish()
        #    log.info("Finish the search_dbdisk execution")
        #
        #if self._search_heimdall:
        #    for execution_instance in self._search_heimdall_execution_instances:
        #        execution_instance.finish()
        #    log.info("Finish the heimdall execution")
        #if self._search_spectrometer and self._spectrometer_dbdisk:
        #    for execution_instance in self._spectrometer_dbdisk_execution_instances:
        #        execution_instance.finish()
        #    log.info("Finish the spectrometer_dbdisk execution")
        #for execution_instance in self._search_execution_instances:
        #    execution_instance.finish()
        #    log.info("Finish the baseband2filterbank execution")

        # To delete simultaneous spectral output buffer
        if self._search_spectrometer and self._spectrometer_dbdisk:
            log.info("Delete spectrometer ring buffer")
            process_index = 0
            execution_instances = []
            for command in self._spectrometer_delete_rbuf_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                execution_instances.append(execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            for execution_instance in execution_instances:
                execution_instance.finish()
            log.info("Delete spectrometer ring buffer, DONE")

        # To delete filterbank ring buffer
        process_index = 0
        execution_instances = []
        log.info("Delete filterbank ring buffer")
        for command in self._search_delete_rbuf_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1
        for execution_instance in execution_instances:
            execution_instance.finish()
        log.info("Delete filterbank ring buffer, DONE")

        self.state = "ready"
        log.info("Ready")

    def deconfigure(self):
        log.info("Receive 'DECONFIGURE' command")
        if self.state not in ["ready", "error"]:
            log.error(
                "Pipeline can only be deconfigured from ready or error state")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "Pipeline can only be deconfigured from ready or error state")

        if self.state == "ready":  # Normal deconfigure
            self.state = "deconfiguring"
            log.info("Deconfigure with normal status")

            # To stop the capture
            if self._execution:
                log.info("Send 'END-OF-CAPTURE' command to capture software")
                process_index = 0
                for control_socket in self._input_control_socket:
                    self._capture_control(
                        control_socket, "END-OF-CAPTURE", self._input_socket_address[process_index])
                    process_index += 1
            for execution_instance in self._input_execution_instances:
                execution_instance.finish()
            log.info("Send 'END-OF-CAPTURE' command to capture software, DONE")

            # To delete baseband ring buffer
            process_index = 0
            execution_instances = []
            log.info("Delete baseband ring buffer")
            for command in self._input_delete_rbuf_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                execution_instances.append(execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            for execution_instance in execution_instances:
                execution_instance.finish()
            log.info("Delete baseband ring buffer, DONE")

        else:  # Force deconfigure
            self.state = "deconfiguring"
            log.info("Deconfigure with wrong status")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)

        self.state = "idle"
        log.info("Idle")


@register_pipeline("Spectrometer")
class Spectrometer(Pipeline):

    def __init__(self):
        super(Spectrometer, self).__init__()
        self.state = "idle"

    def configure(self, config_json, input_config):
        log.info("Received 'CONFIGURE' command")
        self._execution_instances = []
        self._input_execution_instances = []

        self._spectrometer_commands = []
        self._spectrometer_create_rbuf_commands = []
        self._spectrometer_delete_rbuf_commands = []
        self._spectrometer_dbdisk_commands = []

        self._input_beam_index = []
        self._input_socket_address = []
        self._input_control_socket = []
        self._pipeline_runtime_directory = []
        self._input_commands = []
        self._input_create_rbuf_commands = []
        self._input_delete_rbuf_commands = []

        if (self.state != "idle"):
            log.error("Can only configure pipeline in idle state")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Can only configure pipeline in idle state")
        log.info("Configuring")

        log.info("Setup and verify parameters for the pipeline")
        # Refresh IERS database to save the time on "start" for coordinate
        # conversion
        self._refresh_iers()

        # Setup parameters of the pipeline
        self.state = "configuring"
        self._input_config = input_config
        self._config_info = json.loads(config_json)
        self._config_freq = self._config_info["frequency"]
        self._config_ip = self._config_info['ip_address']
        self._config_nchunk = self._config_info['nbands']
        self._config_nchunk_offset = self._config_info["band_offset"]

        beam_alt_d = np.array(self._config_info["beam_alt_d"])
        beam_az_d = np.array(self._config_info["beam_az_d"])
        self._beam_alt_d = -1 * (beam_alt_d != 0) * beam_alt_d
        self._beam_az_d = -1 * (beam_az_d != 0) * beam_az_d

        self._pacifix_numa = int(self._config_ip.split(".")[3]) - 1
        self._utc_start_capture = Time(
            self._config_info["utc_start_capture"], format='isot', scale='utc')

        self._input_nbeam = self._input_config["input_nbeam"]
        self._input_nchunk_per_port = self._input_config[
            "input_nchunk_per_port"]
        self._input_ports = self._input_config["input_ports"]
        self._input_nport = len(self._input_ports[0])
        self._input_nchunk = self._input_nport * self._input_nchunk_per_port
        self._input_nchan = self._input_nchunk * self._paf_nchan_per_chunk

        # Check the frequency information in the configuration
        if self._config_freq not in self._paf_freq:
            log.error("The config frequency should be {}, but it is {}".format(
                self._paf_freq, self._config_freq))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("The config frequency should be {}, but it is {}".format(
                self._paf_freq, self._config_freq))
        if self._input_nchunk != self._config_nchunk:
            log.error("The config band number is not right, it should be {}, but it is {}".format(
                self._input_nchunk, self._config_nchunk))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("The config band number is not right, it should be {}, but it is {}".format(
                self._input_nchunk, self._config_nchunk))
        self._start_chunk = int(math.floor(
            (self._paf_nchunk_per_beam - self._input_nchunk) / 2.0))
        self._first_chunk = self._start_chunk + self._config_nchunk_offset
        self._last_chunk = self._start_chunk + \
            self._input_nchunk + self._config_nchunk_offset
        if ((self._first_chunk < 0) or (self._last_chunk) > self._paf_nchunk_per_beam):
            log.error("Required frequency chunks are out of range")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Required frequency chunks are out of range")
        self._freq         = self._config_freq +  \
            0.5 * (self._last_chunk + self._first_chunk - self._paf_nchunk_per_beam) *\
            self._paf_nchan_per_chunk

        # Buffer size
        self._input_blksz = self._input_nchunk * \
            self._paf_df_dtsz * \
            self._rbuf_ndf_per_chunk_per_block
        self._spectrometer_blksz = int(self._spectrometer_ndata_per_samp * self._input_nchan *
                                       self._spectrometer_cufft_nx /
                                       self._paf_over_samp_rate *
                                       self._spectrometer_nbyte *
                                       self._spectrometer_dbdisk)

        # To check pol type
        # We can only have three possibilities
        if self._spectrometer_ptype not in [1, 2, 4]:
            log.error("Spectrometer pol type should be 1, 2 or 4, but it is {}".format(
                self._spectrometer_ptype))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Spectrometer pol type should be 1, 2 or 4, but it is {}".format(
                self._spectrometer_ptype))
        if self._spectrometer_monitor and (self._monitor_ptype not in [1, 2, 4]):
            log.error("Monitor pol type should be 1, 2 or 4, but it is {}".format(
                self._monitor_ptype))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "monitor pol type should be 1, 2 or 4, but it is {}".format(self._monitor_ptype))

        # To see if we have enough memory
        if self._input_nbeam * (self._input_blksz * self._input_nblk +
                                self._spectrometer_blksz * self._spectrometer_nblk) > \
                self._pacifix_memory_limit_per_numa_node:
            log.error("We do not have enough shared memory for the setup "
                      "Try to reduce the ring buffer block number "
                      "or reduce the number of packets in each ring buffer block")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("We do not have enough shared memory for the setup "
                                "Try to reduce the ring buffer block number "
                                "or reduce the number of packets in each ring buffer block")

        # To check the existing of file
        if not os.path.isfile(self._spectrometer_main):
            log.error("{} is not exist".format(self._spectrometer_main))
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "{} is not exist".format(self._spectrometer_main))
        log.info("Setup and verify parameters for the pipeline")

        log.info("Setup command lines for the pipeline")
        # To setup commands for each process
        for i in range(self._input_nbeam):
            if self._execution:
                # To setup address
                destination = []
                for port in self._input_ports[i]:
                    destination.append("{}_{}_{}".format(
                        self._config_ip, port, self._input_nchunk_per_port))

                destination_alive, dead_info = self._check_beam_connection(
                    destination, self._input_check_ndf_per_chunk)
                first_alive_ip = destination_alive[0].split("_")[0]
                first_alive_port = int(destination_alive[0].split("_")[1])

                beam_index = self._acquire_beam_index(
                    first_alive_ip, first_alive_port, self._input_check_ndf_per_chunk)
                refinfo = self._synced_refinfo(
                    self._utc_start_capture, first_alive_ip, first_alive_port)

                # To get directory for data and socket for control
                pipeline_runtime_directory = "{}/beam{:02}".format(
                    self._root_runtime, beam_index)
                if not os.path.isdir(pipeline_runtime_directory):
                    try:
                        os.makedirs(pipeline_runtime_directory)
                    except Exception as error:
                        log.exception(error)
                        log.error("Fail to create {}".format(
                            pipeline_runtime_directory))
                        self._terminate_execution_instances()
                        self._cleanup(self._cleanup_commands_config)
                        self.state = "error"
                        raise PipelineError(
                            "Fail to create {}".format(pipeline_runtime_directory))

                socket_address = "{}/capture.socket".format(
                    pipeline_runtime_directory)
                control_socket = socket.socket(
                    socket.AF_UNIX, socket.SOCK_DGRAM)
                control_socket.setsockopt(
                    socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            else:
                destination_alive = []
                dead_info = []

                beam_index = i
                refinfo = [0, 0, 0]

                socket_address = None
                control_socket = None
                runtime_directory = None

            self._input_beam_index.append(beam_index)
            self._pipeline_runtime_directory.append(pipeline_runtime_directory)
            self._input_socket_address.append(socket_address)
            self._input_control_socket.append(control_socket)

            # To setup CPU bind information and dead_info
            self._pacifix_ncpu_per_instance = self._pacifix_ncpu_per_numa_node / self._input_nbeam
            cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node + \
                i * self._pacifix_ncpu_per_instance
            alive_info = []
            for info in destination_alive:
                alive_info.append("{}_{}".format(info, cpu))
                cpu += 1

            buf_control_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node + \
                i * self._pacifix_ncpu_per_instance + self._input_nport
            capture_control_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node + \
                i * self._pacifix_ncpu_per_instance + self._input_nport
            #capture_control = "1_{}".format(capture_control_cpu)
            capture_control = "0_{}".format(capture_control_cpu)
            refinfo = "{}_{}_{}".format(refinfo[0], refinfo[1], refinfo[2])

            # capture command
            command = ("{} -a {} -b {} -c {} -e {} -f {} -g {} -i {} -j {} "
                       "-k {} -l {} -m {} -n {} -o {} -p {} -q {} ").format(
                           self._input_main, self._input_keys[
                               i], self._paf_df_hdrsz, " -c ".join(alive_info),
                           self._freq, refinfo, pipeline_runtime_directory, buf_control_cpu, capture_control,
                           self._input_cpu_bind, self._rbuf_ndf_per_chunk_per_block, self._tbuf_ndf_per_chunk_per_block,
                           self._input_dada_hdr_fname, self._input_source_default, self._input_pad, beam_index)
            self._input_commands.append(command)

            # spectrometer command
            spectrometer_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                (i + 1) * self._pacifix_ncpu_per_instance - 1
            command = "taskset -c {} {} -a {} -c {} -d {} -e {} -f {} -g {} -i {} -j {} -k {} ".format(
                spectrometer_cpu,
                self._spectrometer_main,
                self._input_keys[i], self._rbuf_ndf_per_chunk_per_block,
                self._gpu_nstream, self._gpu_ndf_per_chunk_per_stream,
                self._pipeline_runtime_directory[i], self._input_nchunk,
                self._spectrometer_cufft_nx, self._spectrometer_ptype, self._spectrometer_accumulate_nblk)
            if self._spectrometer_dbdisk:
                command += "-b k_{}_{} ".format(
                    self._spectrometer_keys[i], self._spectrometer_sod)
            else:
                command += "-b n_{}_{} ".format(self._spectrometer_ip,
                                                self._spectrometer_port)
            if self._spectrometer_monitor:
                command += "-l Y_{}_{}_{} ".format(self._monitor_ip,
                                                   self._monitor_port,
                                                   self._monitor_ptype)
            else:
                command += "-l N "
            self._spectrometer_commands.append(command)

            # Command to create spectrometer ring buffer
            dadadb_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                (i + 1) * self._pacifix_ncpu_per_instance - 1
            if self._spectrometer_dbdisk:
                command = ("taskset -c {} dada_db -l -p -k {} "
                           "-b {} -n {} -r {}").format(
                               dadadb_cpu, self._spectrometer_keys[i],
                               self._spectrometer_blksz,
                               self._spectrometer_nblk,
                               self._spectrometer_nreader)
                self._spectrometer_create_rbuf_commands.append(command)

            # command to create input ring buffer
            command = ("taskset -c {} dada_db -l -p -k {} "
                       "-b {} -n {} -r {}").format(
                           dadadb_cpu, self._input_keys[i],
                           self._input_blksz,
                           self._input_nblk,
                           self._input_nreader)
            self._input_create_rbuf_commands.append(command)

            # command to delete spectrometer ring buffer
            if self._spectrometer_dbdisk:
                command = "taskset -c {} dada_db -d -k {}".format(
                    dadadb_cpu, self._spectrometer_keys[i])
                self._spectrometer_delete_rbuf_commands.append(command)

            # command to delete input ring buffer
            command = "taskset -c {} dada_db -d -k {}".format(
                dadadb_cpu, self._input_keys[i])
            self._input_delete_rbuf_commands.append(command)

            # Command to run dbdisk
            if self._spectrometer_dbdisk:
                dbdisk_cpu = self._pacifix_numa * self._pacifix_ncpu_per_numa_node +\
                    (i + 1) * self._pacifix_ncpu_per_instance - 1
                command = ("dada_dbdisk -b {} -k {} "
                           "-D {} -o -s -z").format(
                               dbdisk_cpu,
                               self._spectrometer_keys[i],
                               self._pipeline_runtime_directory[i])
                self._spectrometer_dbdisk_commands.append(command)
        log.info("Setup command lines for the pipeline, DONE")

        # Create baseband ring buffer
        log.info("Create baseband ring buffer")
        process_index = 0
        execution_instances = []
        for command in self._input_create_rbuf_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1
        for execution_instance in execution_instances:
            execution_instance.finish()
        log.info("Create baseband ring buffer, DONE")

        # Run spectrometer
        log.info("Start to run baseband2spectral")
        process_index = 0
        self._ready_counter = 0
        self._spectrometer_execution_instances = []
        for command in self._spectrometer_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._spectrometer_execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1

        if self._execution:
            while self.state != "error":
                if self._ready_counter == self._input_nbeam:
                    break
        
        for execution_instance in self._spectrometer_execution_instances:
            execution_instance.stdout_callbacks.remove(
                self._ready_counter_callback)
            # execution_instance.stdout_callbacks.add(self._process_status_callback)
            execution_instance.stdout_callbacks.add(
                self._handle_execution_stdout)
        
        # Execute the capture
        log.info("Start capture")
        process_index = 0
        self._ready_counter = 0
        for command in self._input_commands:
            execution_instance = ExecuteCommand(
                command, self._execution, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._input_execution_instances.append(execution_instance)
            self._execution_instances.append(execution_instance)
            process_index += 1

        if self._execution:  # Ready when all capture threads and the capture control thread of all capture instances are ready
            while self.state != "error":
                if self._ready_counter == (self._input_nport) * self._input_nbeam:
                    break
        log.info("Capturing")

        for execution_instance in self._input_execution_instances:
            execution_instance.stdout_callbacks.remove(
                self._ready_counter_callback)
            execution_instance.stdout_callbacks.add(
                self._capture_status_callback)
        
        self.state = "ready"
        log.info("Ready")

    def start(self, status_json):
        log.info("Received 'START' command")
        if self.state != "ready":
            self._terminate_execution_instances()
            log.error("Pipeline can only be started from ready state")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "Pipeline can only be started from ready state")

        self.state = "starting"
        log.info("Starting")
        self.state = "running"
        log.info("Running")

    def stop(self):
        log.info("Received 'STOP' command")
        if self.state != "running":
            log.error("Can only stop a running pipeline")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"
        log.info("Stopping")
        self.state = "ready"
        log.info("Ready")

    def deconfigure(self):
        log.info("Receive 'DECONFIGURE' command")
        if self.state not in ["ready", "error"]:
            log.error(
                "Pipeline can only be deconfigured from ready or error state")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)
            self.state = "error"
            raise PipelineError(
                "Pipeline can only be deconfigured from ready or error state")
        log.info("Deconfiguring")

        if self.state == "ready":  # Normal deconfigure
            self.state = "deconfiguring"
            log.info("Deconfigure with normal status")

            ## To stop the capture
            #if self._execution:
            #    process_index = 0
            #    log.info("Send 'END-OF-CAPTURE' command to capture software")
            #    for control_socket in self._input_control_socket:
            #        self._capture_control(
            #            control_socket, "END-OF-CAPTURE", self._input_socket_address[process_index])
            #        process_index += 1
            #    log.info("Send 'END-OF-CAPTURE' command to capture software, DONE")
            #for execution_instance in self._input_execution_instances:
            #    execution_instance.finish()
            #log.info("Finish capture")

            # Terminate processing
            for execution_instance in self._spectrometer_execution_instances:
                execution_instance.terminate()
                log.info("Finish baseband2spectral")

            # Terminate capture
            for execution_instance in self._input_execution_instances:
                execution_instance.terminate()
                log.info("Finish capture")
        
            # To delete input ring buffer
            log.info("Delete baseband ring buffer")
            process_index = 0
            execution_instances = []
            for command in self._input_delete_rbuf_commands:
                execution_instance = ExecuteCommand(
                    command, self._execution, process_index)
                execution_instance.stdout_callbacks.add(
                    self._handle_execution_stdout)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                execution_instances.append(execution_instance)
                self._execution_instances.append(execution_instance)
                process_index += 1
            for execution_instance in execution_instances:
                execution_instance.finish()
            log.info("Delete spectrometer ring buffer, DONE")

        else:  # Force deconfigure
            self.state = "deconfiguring"
            log.info("Deconfigure with wrong status")
            self._terminate_execution_instances()
            self._cleanup(self._cleanup_commands_config)

        self.state = "idle"
        log.info("Idle")


@register_pipeline("Spectrometer2Beam")
class Spectrometer2Beam(Spectrometer):

    def __init__(self):
        super(Spectrometer2Beam, self).__init__()

    def configure(self, config_json):
        config_dictionary = {
            "input_nbeam":                  2,
            "input_nchunk_per_port":       11,
            "input_ports":                 [[17100, 17101, 17102], [17103, 17104, 17105]]
        }

        super(Spectrometer2Beam, self).configure(
            config_json, config_dictionary)


@register_pipeline("Search1BeamHigh")
class Search1BeamHigh(Search):

    def __init__(self):
        super(Search1BeamHigh, self).__init__()

    def configure(self, config_json):
        config_dictionary = {
            "input_nbeam":                  1,
            "input_nchunk_per_port":       16,
            "input_ports":                 [[17100, 17101, 17102]],
            "search_heimdall":     1,
            "search_dbdisk":       1,
            "search_spectrometer": 1,
            "search_sod":          1,
            "search_nreader":      2,
        }
        super(Search1BeamHigh, self).configure(config_json, config_dictionary)


@register_pipeline("Search2BeamLow")
class Search2BeamLow(Search):

    def __init__(self):
        super(Search2BeamLow, self).__init__()

    #def configure(self, config_json):
        #config_dictionary = {
        #    "input_nbeam":                  2,
        #    "input_nchunk_per_port":       11,
        #    "input_ports":                 [[17100, 17101, 17102], [17103, 17104, 17105]],
        #    "search_heimdall":     0,
        #    "search_dbdisk":       0,
        #    "search_spectrometer": 0,
        #    "search_sod":          0,
        #    "search_nreader":      1,
        #}
    def configure(self, config_json):
        config_dictionary = {
            "input_nbeam":                  2,
            "input_nchunk_per_port":       11,
            "input_ports":                 [[17100, 17101, 17102], [17103, 17104, 17105]],
            "search_heimdall":     1,
            "search_dbdisk":       1,
            "search_spectrometer": 0,
            "search_sod":          1,
            "search_nreader":      2,
        }
        super(Search2BeamLow, self).configure(config_json, config_dictionary)        

@register_pipeline("Search2BeamHigh")
class Search2BeamHigh(Search):

    def __init__(self):
        super(Search2BeamHigh, self).__init__()

    def configure(self, config_json):
        config_dictionary = {
            "input_nbeam":                  2,
            "input_nchunk_per_port":       11,
            "input_ports":                 [[17100, 17101, 17102], [17103, 17104, 17105]],
            #"search_heimdall":     0,
            "search_heimdall":     1,
            "search_dbdisk":       1,
            "search_spectrometer": 0,
            "search_sod":          1,
            #"search_nreader":      1,
            "search_nreader":      2,
        }
        super(Search2BeamHigh, self).configure(config_json, config_dictionary)

# ./pipeline.py -a 0 -b search2beamlow    -c 1 -d 1 -e 100
# ./pipeline.py -a 0 -b search2beamhigh   -c 1 -d 1 -e 100
# ./pipeline.py -a 1 -b search1beamhigh   -c 1 -d 1 -e 100
# ./pipeline.py -a 1 -b spectrometer2beam -c 1 -d 1 -e 100

if __name__ == "__main__":
    logging.getLogger().addHandler(logging.NullHandler())
    log = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level='DEBUG',
        # level='INFO',
        logger=log)

    parser = argparse.ArgumentParser(
        description='To run the pipeline for my test')
    parser.add_argument('-a', '--numa', type=int, nargs='+',
                        help='The ID of numa node')
    parser.add_argument('-b', '--pipeline', type=str, nargs='+',
                        help='The pipeline to run')
    parser.add_argument('-c', '--nconfigure', type=int, nargs='+',
                        help='How many times to repeat the configure')
    parser.add_argument('-d', '--nstart', type=int, nargs='+',
                        help='How many times to repeat the start')
    parser.add_argument('-e', '--length', type=int, nargs='+',
                        help='Length in seconds of observations')

    args = parser.parse_args()
    numa = args.numa[0]
    pipeline = args.pipeline[0]
    nconfigure = args.nconfigure[0]
    nstart = args.nstart[0]
    length = args.length[0]

    host_id = check_output("hostname").strip()[-1]
    ip = "10.17.{}.{}".format(host_id, numa + 1)

    config_info = {"utc_start_capture": Time(Time.now(), format='isot', scale='utc').value,
                   "frequency":         1340.5,
                   "band_offset":        0,
                   "ip_address":        ip,
                   "beam_alt_d":         [0, -0.1, -0.2, -0.3, -0.1, -0.2, -0.3,  0.1,  0.2,  0.3, -0.11, -0.21, -0.31, -0.11, -0.21, -0.31,  0.12,  0.22,  0.32, -0.12, -0.22, -0.32, -0.12, -0.22, -0.32,  0.12,  0.22,  0.32, 0.13, 0.23,  0.33, -0.13, -0.23, -0.33, -0.13, -0.23, -0.33,  0.13,  0.23],
                   # First column in the file, opposite value
                   "beam_az_d":        [0, -0.1, -0.2, -0.3,  0.1,  0.2,  0.3, -0.1, -0.2, -0.3, -0.11, -0.21, -0.31,  0.11,  0.21,  0.31, -0.12, -0.22, -0.32, -0.12, -0.22, -0.32,  0.12,  0.22,  0.32, -0.12, -0.22, -0.32, -0.13, -0.23, -0.33, -0.13, -0.23, -0.33,  0.13,  0.23,  0.33, -0.13, -0.23],
                   # Second column in the file, opposite value
                   }
    status_info = {"utc_start_process":  Time(Time.now(), format='isot', scale='utc').value,
                   "source-name": "J0332+5434",
                   "ra":   190.3,
                   "dec":  80.10,
                   "scannum":    1001,
                   "subscannum": 1002,
                   }

    for i in range(nconfigure):
        log.info("Create pipeline ...")
        if pipeline == "search2beamlow":
            config_info["nbands"] = 33
            config_info["search_spectrometer"] = 0
            mode = Search2BeamLow()
        if pipeline == "search2beamhigh":
            config_info["nbands"] = 33
            config_info["zoomnbands"] = 5
            config_info["zoomband0"] = 26
            config_info["search_spectrometer"] = 1
            mode = Search2BeamHigh()
        if pipeline == "search1beamhigh":
            config_info["nbands"] = 48
            config_info["zoomnbands"] = 5
            config_info["zoomband0"] = 33
            config_info["search_spectrometer"] = 1
            mode = Search1BeamHigh()
        if pipeline == "spectrometer2beam":
            config_info["nbands"] = 33
            mode = Spectrometer2Beam()

        log.info("Configure it ...")
        config_info["utc_start_capture"] = Time(
            Time.now() + 27 * units.second, format='isot', scale='utc').value
        config_json = json.dumps(config_info)
        mode.configure(config_json)

        for j in range(nstart):
            log.info("Start it ...")
            status_info["utc_start_process"] = Time(
                Time.now() + 15 * units.second, format='isot', scale='utc').value
            status_json = json.dumps(status_info)
            mode.start(status_json)

            time.sleep(length)
            log.info("Stop it ...")
            mode.stop()

        log.info("Deconfigure it ...")
        mode.deconfigure()
