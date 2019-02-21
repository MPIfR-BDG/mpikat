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
import astropy.units as units
import logging
from katcp import Sensor
import argparse
import threading
import inspect
import os
from math import floor

# Updates:
# 1. Check the directory exist or not, create it if not;
# 2. Check the header template exist or not, raise error if not;
# 3. The cleanup works as expected now;
# 4. Add callbacks for stderr and returncode for class ExecuteCommand, done;
# 5. Check the memory size before execute, done;
# 6. Cleanup the log, almost done;
# 7. Remove print, done
# 8. add reuseaddr in pipeline and also capture, done
# 9. utc_start_process, to put the dada_dbregister into capture does not help, now synced, more test;
# 10. add lock to counter to protect it
# 11. Remove NVPROF, SOD
log = logging.getLogger('mpikat.effelsberg.paf.pipeline')
log.setLevel('DEBUG')
EXECUTE = True
#EXECUTE        = False

HEIMDALL = False   # To run heimdall on filterbank file or not
# HEIMDALL       = True   # To run heimdall on filterbank file or not

DBDISK = True   # To run dbdisk on filterbank file or not
#DBDISK         = False   # To run dbdisk on filterbank file or not

PAF_ROOT = "/home/pulsar/xinping/phased-array-feed/"
DATA_ROOT = "/beegfs/DENG/"
DADA_ROOT = "{}/AUG/baseband/".format(DATA_ROOT)
SOURCE_DEFAULT = "UNKNOW_00:00:00.00_00:00:00.00"

PAF_CONFIG = {"instrument_name":    "PAF-BMF",
              "nchan_chk":    	     7,        # MHz
              "over_samp_rate":      (32.0 / 27.0),
              "prd":                 27,       # Seconds
              "df_res":              1.08E-4,  # Seconds
              "ndf_prd":             250000,

              "df_dtsz":      	     7168,
              "df_pktsz":     	     7232,
              "df_hdrsz":     	     64,

              "nbyte_baseband":      2,
              "npol_samp_baseband":  2,
              "ndim_pol_baseband":   2,

              "ncpu_numa":           10,
              "mem_node":            60791751475,  # has 10% spare
              "first_port":          17100,
}

SEARCH_CONFIG_GENERAL = {"rbuf_baseband_ndf_chk":   16384,
                         "rbuf_baseband_nblk":      5,
                         "rbuf_baseband_nread":     1,
                         "tbuf_baseband_ndf_chk":   128,

                         "rbuf_filterbank_ndf_chk": 16384,
                         "rbuf_filterbank_nblk":    2,
                         "rbuf_filterbank_nread":   (HEIMDALL + DBDISK) if (HEIMDALL + DBDISK) else 1,

                         "nchan_filterbank":        512,
                         "cufft_nx":                128,

                         "nbyte_filterbank":        1,
                         "npol_samp_filterbank":    1,
                         "ndim_pol_filterbank":     1,

                         "ndf_stream":      	    1024,
                         "nstream":                 2,

                         "bind":                    1,

                         "pad":                     0,
                         "ndf_check_chk":           1024,

                         "detect_thresh":           10,
                         "dm":                      [1, 1000],
                         "zap_chans":               [],
}

SEARCH_CONFIG_1BEAM = {"dada_hdr_fname":         "{}/config/header_1beam.txt".format(PAF_ROOT),
                       "rbuf_baseband_key":      ["dada"],
                       "rbuf_filterbank_key":    ["dade"],
                       "nchan_keep_band":        32768,
                       "nbeam":                  1,
                       "nport_beam":             3,
                       "nchk_port":              16,
}

SEARCH_CONFIG_2BEAMS = {"dada_hdr_fname":         "{}/config/header_2beams.txt".format(PAF_ROOT),
                        "rbuf_baseband_key":       ["dada", "dadc"],
                        "rbuf_filterbank_key":     ["dade", "dadg"],
                        "nchan_keep_band":         24576,
                        "nbeam":                   2,
                        "nport_beam":              3,
                        "nchk_port":               11,
}


SPECTRAL_CONFIG_GENERAL = {"rbuf_baseband_ndf_chk":   16384,                 
                           "rbuf_baseband_nblk":      5,
                           "rbuf_baseband_nread":     1,                 
                           "tbuf_baseband_ndf_chk":   128,
                           
                           "rbuf_spectral_ndf_chk":   16384,
                           "rbuf_spectral_nblk":      2,
                           "rbuf_spectral_nread":     1,
                           
                           "cufft_nx":                1024,
                           "nbyte_spectral":          4,
                           "ndf_stream":      	      1024,
                           "nstream":                 2,
                           
                           "bind":                    1,
                           "pad":                     0,
                           "ndf_check_chk":           1024,
}

SPECTRAL_CONFIG_1BEAM = {"dada_hdr_fname":         "{}/config/header_1beam.txt".format(PAF_ROOT),
                         "rbuf_baseband_key":      ["dada"],
                         "rbuf_spectral_key":      ["dade"],
                         "nbeam":                  1,
                         "nport_beam":             3,
                         "nchk_port":              16,
}

SPECTRAL_CONFIG_2BEAMS = {"dada_hdr_fname":         "{}/config/header_1beam.txt".format(PAF_ROOT),
                          "rbuf_baseband_key":       ["dada", "dadc"],
                          "rbuf_spectral_key":       ["dade", "dadg"],
                          "nbeam":                   2,
                          "nport_beam":              3,
                          "nchk_port":               11,
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
        PIPELINES[name] = cls
        return cls
    return _register


class ExecuteCommand(object):

    def __init__(self, command, process_index = None):
        self._command = command
        self._process_index = process_index
        self.stdout_callbacks = set()
        self.stderr_callbacks = set()
        self.returncode_callbacks = set()
        self._monitor_threads = []
        self._process = None
        self._executable_command = None
        self._stdout = None
        self._stderr = None
        self._returncode = None

        log.debug(self._command)
        self._executable_command = shlex.split(self._command)

        if EXECUTE:
            try:
                self._process = Popen(self._executable_command,
                                      stdout=PIPE,
                                      stderr=PIPE,
                                      bufsize=1,
                                      universal_newlines=True)
            except Exception as error:
                log.exception("Error while launching command: {} with error {}".format(
                    self._command, error))
                self.returncode = self._command + "; RETURNCODE is: ' 1'"
            if self._process == None:
                self.returncode = self._command + "; RETURNCODE is: ' 1'"

            # Start monitors
            self._monitor_threads.append(
                threading.Thread(target=self._stdout_monitor))
            self._monitor_threads.append(
                threading.Thread(target=self._stderr_monitor))

            for thread in self._monitor_threads:
                thread.daemon = True
                thread.start()

    def __del__(self):
        class_name = self.__class__.__name__

    def finish(self):
        if EXECUTE:
            for thread in self._monitor_threads:
                thread.join()

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

    def _stdout_monitor(self):
        if EXECUTE:
            while self._process.poll() == None:
                stdout = self._process.stdout.readline().rstrip("\n\r")
                if stdout != b"":
                    if self._process_index != None:
                        self.stdout = stdout + "; PROCESS_INDEX is " + str(self._process_index)
                    else:
                        self.stdout = stdout
                   
            if self._process.returncode:
                self.returncode = self._command + \
                    "; RETURNCODE is: " + str(self._process.returncode)

    def _stderr_monitor(self):
        if EXECUTE:
            while self._process.poll() == None:
                stderr = self._process.stderr.readline().rstrip("\n\r")
                if stderr != b"":
                    if self._process_index != None:
                        self.stderr = self._command + "; STDERR is: " + stderr + "; PROCESS_INDEX is " + str(self._process_index)
                    else:
                        self.stderr = self._command + "; STDERR is: " + stderr
                                        
            if self._process.returncode:
                self.returncode = self._command + \
                    "; RETURNCODE is: " + str(self._process.returncode)


class Pipeline(object):

    def __init__(self):
        self._sensors = []
        self.callbacks = set()
        self._ready_counter = 0
        self.setup_sensors()

        self._prd = PAF_CONFIG["prd"]
        self._first_port = PAF_CONFIG["first_port"]
        self._df_res = PAF_CONFIG["df_res"]
        self._df_dtsz = PAF_CONFIG["df_dtsz"]
        self._df_pktsz = PAF_CONFIG["df_pktsz"]
        self._df_hdrsz = PAF_CONFIG["df_hdrsz"]
        self._ncpu_numa = PAF_CONFIG["ncpu_numa"]
        self._nchan_chk = PAF_CONFIG["nchan_chk"]
        self._nbyte_baseband = PAF_CONFIG["nbyte_baseband"]
        self._ndim_pol_baseband = PAF_CONFIG["ndim_pol_baseband"]
        self._npol_samp_baseband = PAF_CONFIG["npol_samp_baseband"]
        self._mem_node = PAF_CONFIG["mem_node"]
        self._over_samp_rate     = PAF_CONFIG["over_samp_rate"]
        self._cleanup_commands = []
        self._ready_counter_lock = threading.Lock()
        
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

    def _acquire_beam_index(self, ip, port, ndf_check_chk):
        """
        To get the beam ID
        """
        data = bytearray(self._df_pktsz)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Force to timeout after one data frame period
        socket.setdefaulttimeout(self._prd)
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            beam_index = []
            for i in range(ndf_check_chk):
                nbyte, address = sock.recvfrom_into(data, self._df_pktsz)
                data_uint64 = np.fromstring(str(data), 'uint64')
                hdr_uint64 = np.uint64(struct.unpack(
                    "<Q", struct.pack(">Q", data_uint64[2]))[0])
                beam_index.append(hdr_uint64 & np.uint64(0x000000000000ffff))

            if (len(list(set(beam_index))) > 1):
                self.state = "error"
                log.error("Beams are mixed up, please check the routing table")
                raise PipelineError(
                    "Beams are mixed up, please check the routing table")
            sock.close()
            return beam_index[0]
        except Exception as error:
            log.exception(error)
            sock.close()
            self.state = "error"
            log.error("{} fail".format(inspect.stack()[0][3]))
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _synced_startbuf(self, utc_start_process, utc_start_capture):
        if(utc_start_process < utc_start_capture):
            self.state = "error"
            log.error("utc_start_process should be later than utc_start_capture")
            raise PipelineError("utc_start_process should be later than utc_start_capture")
            
        delta_time = utc_start_process.unix - utc_start_capture.unix
        start_buf = int(floor(delta_time / self._blk_res)) - 1   # The start buf, to be safe -1; 

        sleep_time = utc_start_process.unix - Time.now().unix
        log.debug("SLEEP TIME to wait for START BUF block is {} seconds".format(sleep_time))
        
        if(sleep_time<0):
            self.state = "error"
            log.error("Too late to start process")
            raise PipelineError("Too late to start process")
            
        time.sleep(sleep_time)  # Sleep until we are ready to go
        return start_buf
        
    def _synced_refinfo(self, utc_start_capture, ip, port):
        # Capture one packet to see what is current epoch, seconds and idf
        # We need to do that because the seconds is not always matched with
        # estimated value
        epoch_ref, sec_ref, idf_ref = self._refinfo(ip, port)
        
        while utc_start_capture.unix > (epoch_ref * 86400.0 + sec_ref + PAF_CONFIG["prd"]):
            sec_ref = sec_ref + PAF_CONFIG["prd"]
        while utc_start_capture.unix < (epoch_ref * 86400.0 + sec_ref):
            sec_ref = sec_ref - PAF_CONFIG["prd"]

        idf_ref = (utc_start_capture.unix - epoch_ref *
                   86400.0 - sec_ref) / self._df_res

        return epoch_ref, sec_ref, int(idf_ref)

    def _refinfo(self, ip, port):
        """
        To get reference information for capture
        """
        data = bytearray(self._df_pktsz)
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Force to timeout after one data frame period
        socket.setdefaulttimeout(self._prd)
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            nbyte, address = sock.recvfrom_into(data, self._df_pktsz)
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
            self.state = "error"
            log.error("{} fail".format(inspect.stack()[0][3]))
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _check_connection_beam(self, destination, ndf_check_chk):
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
            alive, nchk_alive = self._check_connection_port(
                ip, port, ndf_check_chk)

            if alive == 1:
                destination_alive.append(
                    destination[i] + "_{}".format(nchk_alive))
            else:
                destination_dead.append(destination[i])

        if (len(destination_alive) == 0):  # No alive ports, error
            self.state = "error"
            log.error("The stream is not alive")
            raise PipelineError("The stream is not alive")

        return destination_alive, destination_dead

    def _check_connection_port(self, ip, port, ndf_check_chk):
        """
        To check the connection of single port
        """
        alive = 1
        nchk_alive = 0
        data = bytearray(self._df_pktsz)
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Force to timeout after one data frame period
        socket.setdefaulttimeout(self._prd)
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            nbyte, address = sock.recvfrom_into(data, self._df_pktsz)
            if (nbyte != self._df_pktsz):
                alive = 0
            else:
                source = []
                alive = 1
                for i in range(ndf_check_chk):
                    buf, address = sock.recvfrom(self._df_pktsz)

                    data_uint64 = np.fromstring(str(buf), 'uint64')
                    hdr_uint64 = np.uint64(struct.unpack(
                        "<Q", struct.pack(">Q", data_uint64[2]))[0])

                    source.append(address)
                nchk_alive = len(set(source))
            sock.close()
        except Exception as error:
            log.exception(error)
            self.state = "error"
            log.error("{} fail".format(inspect.stack()[0][3]))
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))
        return alive, nchk_alive

    def _capture_control(self, ctrl_socket, command, socket_address):
        if EXECUTE:
            try:
                ctrl_socket.sendto(command, socket_address)
            except Exception as error:
                log.exception(error)
                self.state = "error"
                log.error("{} fail".format(inspect.stack()[0][3]))
                raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _handle_execution_returncode(self, returncode, callback):
        if EXECUTE:
            log.debug(returncode)
            if returncode:
                self.state = "error"
                log.error(returncode)
                raise PipelineError(returncode)
        
    def _handle_execution_stderr(self, stderr, callback):
        if EXECUTE:
            log.error(stderr)
            self.state = "error"
            log.error(stderr)
            raise PipelineError(stderr)

    def _ready_counter_callback(self, stdout, callback):
        if EXECUTE:
            log.debug(stdout)
            if stdout.find("READY") != -1:
                self._ready_counter_lock.acquire()
                self._ready_counter += 1
                self._ready_counter_lock.release()
                
    def _capture_status_callback(self, stdout, callback):
        if EXECUTE:
            log.debug(stdout)
            if stdout.find("CAPTURE_STATUS") != -1:
                process_index = int(stdout.split(" ")[-1])
                capture_status = stdout.split(";")[0].split(" ")
                if process_index == 0:
                    self._beam_sensor0.set_value(float(self._beam_index[0]))
                    self._time_sensor0.set_value(float(capture_status[1]))
                    self._average_sensor0.set_value(float(capture_status[2]))
                    self._instant_sensor0.set_value(float(capture_status[3]))
                if process_index == 1:
                    self._beam_sensor1.set_value(float(self._beam_index[1]))
                    self._time_sensor1.set_value(float(capture_status[1]))
                    self._average_sensor1.set_value(float(capture_status[2]))
                    self._instant_sensor1.set_value(float(capture_status[3]))
                    
    def _cleanup(self):
        if EXECUTE:
            execution_instances = []
            for command in self._cleanup_commands:
                execution_instances.append(ExecuteCommand(command))
            for execution_instance in execution_instances:         # Wait until the cleanup is done
                execution_instance.finish()

@register_pipeline("Search")
class Search(Pipeline):

    def __init__(self):
        super(Search, self).__init__()
        self.state = "idle"

        self._beam_index = []

        self._socket_address = []
        self._control_socket = []
        self._runtime_directory = []

        self._dbdisk_commands = []
        self._capture_commands = []
        self._heimdall_commands = []
        self._baseband2filterbank_commands = []
        self._baseband_create_buffer_commands = []
        self._baseband_delete_buffer_commands = []
        self._filterbank_create_buffer_commands = []
        self._filterbank_delete_buffer_commands = []

        self._dbdisk_execution_instances = []
        self._capture_execution_instances = []
        self._baseband2filterbank_execution_instances = []
        self._heimdall_execution_instances = []

        self._dm = SEARCH_CONFIG_GENERAL["dm"],
        self._pad = SEARCH_CONFIG_GENERAL["pad"]
        self._bind = SEARCH_CONFIG_GENERAL["bind"]
        self._nstream = SEARCH_CONFIG_GENERAL["nstream"]
        self._cufft_nx = SEARCH_CONFIG_GENERAL["cufft_nx"]
        self._zap_chans = SEARCH_CONFIG_GENERAL["zap_chans"]
        self._ndf_stream = SEARCH_CONFIG_GENERAL["ndf_stream"]
        self._detect_thresh = SEARCH_CONFIG_GENERAL["detect_thresh"]
        self._ndf_check_chk = SEARCH_CONFIG_GENERAL["ndf_check_chk"]
        self._nchan_filterbank = SEARCH_CONFIG_GENERAL["nchan_filterbank"]
        self._nbyte_filterbank = SEARCH_CONFIG_GENERAL["nbyte_filterbank"]
        self._rbuf_baseband_nblk = SEARCH_CONFIG_GENERAL["rbuf_baseband_nblk"]
        self._ndim_pol_filterbank = SEARCH_CONFIG_GENERAL[
            "ndim_pol_filterbank"]
        self._rbuf_baseband_nread = SEARCH_CONFIG_GENERAL[
            "rbuf_baseband_nread"]
        self._npol_samp_filterbank = SEARCH_CONFIG_GENERAL[
            "npol_samp_filterbank"]
        self._rbuf_filterbank_nblk = SEARCH_CONFIG_GENERAL[
            "rbuf_filterbank_nblk"]
        self._rbuf_baseband_ndf_chk = SEARCH_CONFIG_GENERAL[
            "rbuf_baseband_ndf_chk"]
        self._rbuf_filterbank_nread = SEARCH_CONFIG_GENERAL[
            "rbuf_filterbank_nread"]
        self._tbuf_baseband_ndf_chk = SEARCH_CONFIG_GENERAL[
            "tbuf_baseband_ndf_chk"]
        self._rbuf_filterbank_ndf_chk = SEARCH_CONFIG_GENERAL[
            "rbuf_filterbank_ndf_chk"]

        self._cleanup_commands = ["pkill -9 -f capture_main",
                                  "pkill -9 -f dada_db",
                                  # process name, maximum 16 bytes (15 bytes
                                  # visiable)
                                  "pkill -9 -f baseband2filter",
                                  "pkill -9 -f heimdall",
                                  "pkill -9 -f dada_dbdisk",
                                  "ipcrm -a"]

    def configure(self, utc_start_capture, freq, ip, pipeline_config):
        log.info("Received 'configure' command")
        if (self.state != "idle"):
            self.state = "error"
            log.error("Can only configure pipeline in idle state")
            raise PipelineError("Can only configure pipeline in idle state")
        log.info("Configuring")
        
        # Setup parameters of the pipeline
        self.state = "configuring"
        self._pipeline_config = pipeline_config
        self._ip = ip
        self._numa = int(ip.split(".")[3]) - 1
        self._freq = freq
        self._utc_start_capture = Time(utc_start_capture, format='isot', scale='utc')
        
        self._nbeam = self._pipeline_config["nbeam"]
        self._nchk_port = self._pipeline_config["nchk_port"]
        self._nport_beam = self._pipeline_config["nport_beam"]
        self._nchan_keep_band = self._pipeline_config["nchan_keep_band"]
        self._rbuf_baseband_key = self._pipeline_config["rbuf_baseband_key"]
        self._rbuf_filterbank_key = self._pipeline_config[
            "rbuf_filterbank_key"]
        self._dada_hdr_fname = self._pipeline_config["dada_hdr_fname"]
        self._blk_res = self._df_res * self._rbuf_baseband_ndf_chk
        self._nchk_beam = self._nchk_port * self._nport_beam
        self._nchan_baseband = self._nchan_chk * self._nchk_beam
        self._ncpu_pipeline = self._ncpu_numa / self._nbeam
        self._rbuf_baseband_blksz = self._nchk_port * \
            self._nport_beam * self._df_dtsz * self._rbuf_baseband_ndf_chk
        self._rbuf_filterbank_blksz = int(self._nchan_filterbank * self._rbuf_baseband_blksz *
                                          self._nbyte_filterbank * self._npol_samp_filterbank *
                                          self._ndim_pol_filterbank / float(self._nbyte_baseband *
                                                                            self._npol_samp_baseband *
                                                                            self._ndim_pol_baseband *
                                                                            self._nchan_baseband *
                                                                            self._cufft_nx))

        # To see if we can process baseband data with integer repeats
        if self._rbuf_baseband_ndf_chk % (self._ndf_stream * self._nstream):
            self.state = "error"
            log.error("data in baseband ring buffer block can only "
                      "be processed by baseband2filterbank with integer repeats")
            raise PipelineError("data in baseband ring buffer block can only "
                                "be processed by baseband2filterbank with integer repeats")

        # To see if we have enough memory
        if self._nbeam * (self._rbuf_filterbank_blksz + self._rbuf_baseband_blksz) > self._mem_node:
            self.state = "error"
            log.error("We do not have enough shared memory for the setup "
                      "Try to reduce the ring buffer block number "
                      "or reduce the number of packets in each ring buffer block")
            raise PipelineError("We do not have enough shared memory for the setup "
                                "Try to reduce the ring buffer block number "
                                "or reduce the number of packets in each ring buffer block")

        # To be safe, kill all related softwares and free shared memory if there is any
        self._cleanup()
        
        # To setup commands for each process
        capture = "{}/src/capture_main".format(PAF_ROOT)
        baseband2filterbank = "{}/src/baseband2filterbank_main".format(
            PAF_ROOT)
        if not os.path.isfile(capture):
            self.state = "error"
            log.error("{} is not exist".format(capture))
            raise PipelineError("{} is not exist".format(capture))
        if not os.path.isfile(baseband2filterbank):
            self.state = "error"
            log.error("{} is not exist".format(baseband2filterbank))
            raise PipelineError("{} is not exist".format(baseband2filterbank))
        if not os.path.isfile(self._dada_hdr_fname):
            self.state = "error"
            log.error("{} is not exist".format(self._dada_hdr_fname))
            raise PipelineError("{} is not exist".format(self._dada_hdr_fname))

        for i in range(self._nbeam):
            if EXECUTE:
                # To setup address
                destination = []
                for j in range(self._nport_beam):
                    port = self._first_port + i * self._nport_beam + j
                    destination.append("{}_{}_{}".format(
                        self._ip, port, self._nchk_port))

                destination_alive, dead_info = self._check_connection_beam(
                    destination, self._ndf_check_chk)
                first_alive_ip = destination_alive[0].split("_")[0]
                first_alive_port = int(destination_alive[0].split("_")[1])

                beam_index = self._acquire_beam_index(
                    first_alive_ip, first_alive_port, self._ndf_check_chk)
                refinfo = self._synced_refinfo(
                    self._utc_start_capture, first_alive_ip, first_alive_port)
                beam_index = self._acquire_beam_index(
                    first_alive_ip, first_alive_port, self._ndf_check_chk)
                refinfo = self._synced_refinfo(
                    self._utc_start_capture, first_alive_ip, first_alive_port)

                # To get directory for data and socket for control
                runtime_directory = "{}/beam{:02}".format(
                    DATA_ROOT, beam_index)
                if not os.path.isdir(runtime_directory):
                    try:
                        os.makedirs(directory)
                    except Exception as error:
                        log.exception(error)
                        self.state = "error"
                        log.error("Fail to create {}".format(runtime_directory))
                        raise PipelineError(
                            "Fail to create {}".format(runtime_directory))

                socket_address = "{}/capture.socket".format(runtime_directory)
                control_socket = socket.socket(
                    socket.AF_UNIX, socket.SOCK_DGRAM)
                control_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            else:
                destination_alive = []
                dead_info = []

                beam_index = i
                refinfo = [0, 0, 0]

                socket_address = None
                control_socket = None
                runtime_directory = None

            self._beam_index.append(beam_index)
            self._runtime_directory.append(runtime_directory)
            self._socket_address.append(socket_address)
            self._control_socket.append(control_socket)

            # To setup CPU bind information and dead_info
            cpu = self._numa * self._ncpu_numa + i * self._ncpu_pipeline
            alive_info = []
            for info in destination_alive:
                alive_info.append("{}_{}".format(info, cpu))
                cpu += 1

            buf_control_cpu = self._numa * self._ncpu_numa + \
                i * self._ncpu_pipeline + self._nport_beam
            capture_control_cpu = self._numa * self._ncpu_numa + \
                i * self._ncpu_pipeline + self._nport_beam
            capture_control = "1_{}".format(capture_control_cpu)
            refinfo = "{}_{}_{}".format(refinfo[0], refinfo[1], refinfo[2])

            # capture command
            command = ("{} -a {} -b {} -c {} -e {} -f {} -g {} -i {} -j {} "
                       "-k {} -l {} -m {} -n {} -o {} -p {} -q {} -r {}").format(
                           capture, self._rbuf_baseband_key[i], self._df_hdrsz, " -c ".join(alive_info),
                           self._freq, refinfo, runtime_directory, buf_control_cpu, capture_control, 
                           self._bind, self._rbuf_baseband_ndf_chk, self._tbuf_baseband_ndf_chk,
                           self._dada_hdr_fname, PAF_CONFIG["instrument_name"], SOURCE_DEFAULT, self._pad, beam_index)
            self._capture_commands.append(command)

            # baseband2filterbank command
            baseband2filterbank_cpu = self._numa * self._ncpu_numa +\
                                      (i + 1) * self._ncpu_pipeline - 1
            command = ("taskset -c {} {} -a {} -b {} -c {} -d {} -e {} "
                       "-f {} -i {} -j {} -k {} -l {} -g 1").format(
                           baseband2filterbank_cpu, baseband2filterbank, self._rbuf_baseband_key[i],
                           self._rbuf_filterbank_key[i], self._rbuf_filterbank_ndf_chk,
                           self._nstream, self._ndf_stream, self._runtime_directory[i],
                           self._nchk_beam, self._cufft_nx,
                           self._nchan_filterbank, self._nchan_keep_band)
            self._baseband2filterbank_commands.append(command)

            # Command to create filterbank ring buffer
            dadadb_cpu = self._numa * self._ncpu_numa +\
                (i + 1) * self._ncpu_pipeline - 1
            command = ("taskset -c {} dada_db -p -l -k {} "
                       "-b {} -n {} -r {}").format(
                           dadadb_cpu, self._rbuf_filterbank_key[i],
                           self._rbuf_filterbank_blksz,
                           self._rbuf_filterbank_nblk,
                           self._rbuf_filterbank_nread)
            self._filterbank_create_buffer_commands.append(command)

            # command to create baseband ring buffer
            command = ("taskset -c {} dada_db -p -l -k {} "
                       "-b {} -n {} -r {}").format(
                           dadadb_cpu, self._rbuf_baseband_key[i],
                           self._rbuf_baseband_blksz,
                           self._rbuf_baseband_nblk,
                           self._rbuf_baseband_nread)
            self._baseband_create_buffer_commands.append(command)

            # command to delete filterbank ring buffer
            command = "taskset -c {} dada_db -d -k {}".format(dadadb_cpu, self._rbuf_filterbank_key[i])
            self._filterbank_delete_buffer_commands.append(command)

            # command to delete baseband ring buffer
            command = "taskset -c {} dada_db -d -k {}".format(dadadb_cpu, self._rbuf_baseband_key[i])
            self._baseband_delete_buffer_commands.append(command)

            # Command to run heimdall
            heimdall_cpu = self._numa * self._ncpu_numa +\
                (i + 1) * self._ncpu_pipeline - 1
            command = ("taskset -c {} heimdall -k {} "
                       "-detect_thresh {} -output_dir {} ").format(
                           heimdall_cpu, self._rbuf_filterbank_key[i],
                           self._detect_thresh, runtime_directory)
            if self._zap_chans:
                zap = ""
                for zap_chan in self._zap_chans:
                    zap += " -zap_chans {} {}".format(
                        self._zap_chan[0], self._zap_chan[1])
                command += zap
                if self._dm:
                    command += "-dm {} {}".format(self._dm[0], self._dm[1])
            self._heimdall_commands.append(command)

            # Command to run dbdisk
            dbdisk_cpu = self._numa * self._ncpu_numa +\
                (i + 1) * self._ncpu_pipeline - 1
            command = ("dada_dbdisk -b {} -k {} "
                       "-D {} -o -s -z").format(
                           dbdisk_cpu,
                           self._rbuf_filterbank_key[i],
                           self._runtime_directory[i])
            self._dbdisk_commands.append(command)

        # Create baseband ring buffer
        process_index = 0
        execution_instances = []
        for command in self._baseband_create_buffer_commands:
            execution_instances.append(ExecuteCommand(command, process_index))
            process_index += 1
        for execution_instance in execution_instances:
            execution_instance.finish()

        # Execute the capture
        process_index = 0
        self._capture_execution_instances = []
        for command in self._capture_commands:
            execution_instance = ExecuteCommand(command, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.stderr_callbacks.add(
                self._handle_execution_stderr)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._capture_execution_instances.append(execution_instance)
            process_index += 1
            
        if EXECUTE:  # Ready when all capture threads and the capture control thread of all capture instances are ready
            self._ready_counter = 0
            while True:
                if self._ready_counter == (self._nport_beam + 1) * self._nbeam:
                    break

        # Remove ready_counter_callback and add capture_status_callback
        for execution_instance in self._capture_execution_instances:
            execution_instance.stdout_callbacks.remove(
                self._ready_counter_callback)            
            execution_instance.stdout_callbacks.add(
                self._capture_status_callback)

        self.state = "ready"
        log.info("Ready")
        
    def start(self, utc_start_process, source_name, ra, dec):
        log.info("Received 'start' command")
        if self.state != "ready":
            self.state = "error"
            log.error("Pipeline can only be started from ready state")
            raise PipelineError(
                "Pipeline can only be started from ready state")
        self.state = "starting"
        log.info("Starting")
        
        utc_start_process = Time(utc_start_process, format='isot', scale='utc')

        # Create ring buffer for filterbank data
        process_index = 0
        execution_instances = []
        for command in self._filterbank_create_buffer_commands:
            execution_instances.append(ExecuteCommand(command, process_index))
            process_index += 1
        for execution_instance in execution_instances:         # Wait until the buffer creation is done
            execution_instance.finish()
                
        # Run baseband2filterbank
        process_index = 0
        self._baseband2filterbank_execution_instances = []
        for command in self._baseband2filterbank_commands:
            execution_instance = ExecuteCommand(command, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.stderr_callbacks.add(
                self._handle_execution_stderr)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._baseband2filterbank_execution_instances.append(
                execution_instance)
            process_index += 1

        if HEIMDALL:  # run heimdall if required
            process_index = 0
            self._heimdall_execution_instances = []
            for command in self._heimdall_commands:
                execution_instance = ExecuteCommand(command, process_index)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                self._heimdall_execution_instances.append(
                    execution_instance)
                process_index += 1
                
        if DBDISK:   # Run dbdisk if required
            process_index = 0
            self._dbdisk_execution_instances = []
            for command in self._dbdisk_commands:
                execution_instance = ExecuteCommand(command, process_index)
                execution_instance.returncode_callbacks.add(
                    self._handle_execution_returncode)
                self._dbdisk_execution_instances.append(
                    execution_instance)
                process_index += 1

        # Enable the SOD of baseband ring buffer with given time and then
        # "running"
        if EXECUTE:
            self._ready_counter = 0
            while True:
                if self._ready_counter == self._nbeam:
                    break
            process_index = 0
            start_buf = self._synced_startbuf(utc_start_process, self._utc_start_capture)
            log.debug("START BUF index is {}".format(start_buf))
            for control_socket in self._control_socket:
                self._capture_control(control_socket,
                                      "START-OF-DATA_{}_{}_{}_{}".format(
                                          source_name, ra, dec, start_buf),
                                      self._socket_address[process_index])
                process_index += 1

        # Remove ready_counter_callback 
        for execution_instance in self._baseband2filterbank_execution_instances:
            execution_instance.stdout_callbacks.remove(self._ready_counter_callback)
            
        self.state = "running"
        log.info("Running")
        
    def stop(self):
        log.info("Received 'stop' command")
        if self.state != "running":
            self.state = "error"
            log.error("Can only stop a running pipeline")
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"
        log.info("Stopping")
        
        if EXECUTE:
            process_index = 0
            for control_socket in self._control_socket:  # Stop data
                self._capture_control(control_socket,
                                      "END-OF-DATA",
                                      self._socket_address[process_index])
                process_index += 1
        if DBDISK:
            for execution_instance in self._dbdisk_execution_instances:
                execution_instance.finish()
        if HEIMDALL:
            for execution_instance in self._heimdall_execution_instances:
                execution_instance.finish()
        for execution_instance in self._baseband2filterbank_execution_instances:
            execution_instance.finish()

        # To delete filterbank ring buffer
        process_index = 0
        execution_instances = []
        for command in self._filterbank_delete_buffer_commands:
            execution_instances.append(ExecuteCommand(command, process_index))
            process_index += 1
        for execution_instance in execution_instances:
            execution_instance.finish()

        self.state = "ready"
        log.info("Ready")

    def deconfigure(self):
        log.info("Receive 'deconfigure' command")
        if self.state not in ["ready", "error"]:
            self.state = "error"
            log.error("Pipeline can only be deconfigured from ready or error state")
            raise PipelineError(
                "Pipeline can only be deconfigured from ready or error state")
        log.info("Deconfiguring")
        
        if self.state == "ready":  # Normal deconfigure
            self.state = "deconfiguring"

            # To stop the capture
            if EXECUTE:
                process_index = 0
                for control_socket in self._control_socket:
                    self._capture_control(
                        control_socket, "END-OF-CAPTURE", self._socket_address[process_index])
                    process_index += 1
            for execution_instance in self._capture_execution_instances:
                execution_instance.finish()
            
            # To delete baseband ring buffer
            process_index = 0
            execution_instances = []
            for command in self._baseband_delete_buffer_commands:
                execution_instances.append(ExecuteCommand(command, process_index))
                process_index += 1
            for execution_instance in execution_instances:
                execution_instance.finish()

        else:  # Force deconfigure
            self.state = "deconfiguring"
            
        self.state = "idle"
        log.info("Idle")

@register_pipeline("Search2Beams")
class Search2Beams(Search):

    def configure(self, utc_start_capture, freq, ip):
        super(Search2Beams, self).configure(
            utc_start_capture, freq, ip, SEARCH_CONFIG_2BEAMS)


@register_pipeline("Search1Beam")
class Search1Beam(Search):

    def configure(self, utc_start_capture, freq, ip):
        super(Search1Beam, self).configure(
            utc_start_capture, freq, ip, SEARCH_CONFIG_1BEAM)

@register_pipeline("Spectral")
class Spectral(Pipeline):

    def __init__(self):
        super(Spectral, self).__init__()
        self.state = "idle"

        self._beam_index = []

        self._socket_address = []
        self._control_socket = []
        self._runtime_directory = []

        self._dbdisk_commands = []
        self._capture_commands = []
        self._baseband2spectral_commands = []
        self._baseband_create_buffer_commands = []
        self._baseband_delete_buffer_commands = []
        self._spectral_create_buffer_commands = []
        self._spectral_delete_buffer_commands = []

        self._dbdisk_execution_instances = []
        self._capture_execution_instances = []
        self._baseband2spectral_execution_instances = []
        self._heimdall_execution_instances = []

        self._pad = SPECTRAL_CONFIG_GENERAL["pad"]
        self._bind = SPECTRAL_CONFIG_GENERAL["bind"]
        self._nstream = SPECTRAL_CONFIG_GENERAL["nstream"]
        self._cufft_nx = SPECTRAL_CONFIG_GENERAL["cufft_nx"]
        self._ndf_stream = SPECTRAL_CONFIG_GENERAL["ndf_stream"]
        self._ndf_check_chk = SPECTRAL_CONFIG_GENERAL["ndf_check_chk"]
        self._nbyte_spectral = SPECTRAL_CONFIG_GENERAL["nbyte_spectral"]
        self._rbuf_baseband_nblk = SPECTRAL_CONFIG_GENERAL["rbuf_baseband_nblk"]
        self._rbuf_baseband_nread = SPECTRAL_CONFIG_GENERAL[
            "rbuf_baseband_nread"]
        self._rbuf_spectral_nblk = SPECTRAL_CONFIG_GENERAL[
            "rbuf_spectral_nblk"]
        self._rbuf_baseband_ndf_chk = SPECTRAL_CONFIG_GENERAL[
            "rbuf_baseband_ndf_chk"]
        self._rbuf_spectral_nread = SPECTRAL_CONFIG_GENERAL[
            "rbuf_spectral_nread"]
        self._tbuf_baseband_ndf_chk = SPECTRAL_CONFIG_GENERAL[
            "tbuf_baseband_ndf_chk"]
        self._rbuf_spectral_ndf_chk = SPECTRAL_CONFIG_GENERAL[
            "rbuf_spectral_ndf_chk"]

        self._cleanup_commands = ["pkill -9 -f capture_main",
                                  "pkill -9 -f dada_db",
                                  # process name, maximum 16 bytes (15 bytes
                                  # visiable)
                                  "pkill -9 -f baseband2spectr",
                                  "pkill -9 -f dada_dbdisk",
                                  "ipcrm -a"]

    def configure(self, utc_start_capture, freq, ip, ptype, pipeline_config):
        log.info("Received 'configure' command")
        if (self.state != "idle"):
            self.state = "error"
            log.error("Can only configure pipeline in idle state")
            raise PipelineError("Can only configure pipeline in idle state")
        log.info("Configuring")
        
        # Setup parameters of the pipeline
        self.state = "configuring"
        self._pipeline_config = pipeline_config
        self._ip = ip
        self._numa = int(ip.split(".")[3]) - 1
        self._freq = freq
        self._utc_start_capture = Time(utc_start_capture, format='isot', scale='utc')
        self._ptype = ptype
        
        self._nbeam = self._pipeline_config["nbeam"]
        self._nchk_port = self._pipeline_config["nchk_port"]
        self._nport_beam = self._pipeline_config["nport_beam"]
        self._rbuf_baseband_key = self._pipeline_config["rbuf_baseband_key"]
        self._rbuf_spectral_key = self._pipeline_config["rbuf_spectral_key"]
        self._dada_hdr_fname = self._pipeline_config["dada_hdr_fname"]
        self._blk_res = self._df_res * self._rbuf_baseband_ndf_chk
        self._nchk_beam = self._nchk_port * self._nport_beam
        self._nchan_baseband = self._nchan_chk * self._nchk_beam
        self._ncpu_pipeline = self._ncpu_numa / self._nbeam
        self._rbuf_baseband_blksz = self._nchk_port * \
            self._nport_beam * self._df_dtsz * self._rbuf_baseband_ndf_chk
        self._rbuf_spectral_blksz = int(4 * self._nchan_baseband * # Replace 4 with true pol numbers if we do not pad 0
                                        self._cufft_nx /
                                        self._over_samp_rate *
                                        self._nbyte_spectral)
        
        # To see if we can process baseband data with integer repeats
        if self._rbuf_baseband_ndf_chk % (self._ndf_stream * self._nstream):
            self.state = "error"
            log.error("data in baseband ring buffer block can only "
                      "be processed by baseband2spectral with integer repeats")
            raise PipelineError("data in baseband ring buffer block can only "
                                "be processed by baseband2spectral with integer repeats")

        # To see if we have enough memory
        if self._nbeam * (self._rbuf_spectral_blksz + self._rbuf_baseband_blksz) > self._mem_node:
            self.state = "error"
            log.error("We do not have enough shared memory for the setup "
                       "Try to reduce the ring buffer block number "
                       "or reduce the number of packets in each ring buffer block")
            raise PipelineError("We do not have enough shared memory for the setup "
                                "Try to reduce the ring buffer block number "
                                "or reduce the number of packets in each ring buffer block")

        # To be safe, kill all related softwares and free shared memory if there is any
        self._cleanup()
        
        # To setup commands for each process
        capture = "{}/src/capture_main".format(PAF_ROOT)
        baseband2spectral = "{}/src/baseband2spectral_main".format(
            PAF_ROOT)
        if not os.path.isfile(capture):
            self.state = "error"
            log.error("{} is not exist".format(capture))
            raise PipelineError("{} is not exist".format(capture))
        if not os.path.isfile(baseband2spectral):
            self.state = "error"
            log.error("{} is not exist".format(baseband2spectral))
            raise PipelineError("{} is not exist".format(baseband2spectral))
        if not os.path.isfile(self._dada_hdr_fname):
            self.state = "error"
            log.error("{} is not exist".format(self._dada_hdr_fname))
            raise PipelineError("{} is not exist".format(self._dada_hdr_fname))

        for i in range(self._nbeam):
            if EXECUTE:
                # To setup address
                destination = []
                for j in range(self._nport_beam):
                    port = self._first_port + i * self._nport_beam + j
                    destination.append("{}_{}_{}".format(
                        self._ip, port, self._nchk_port))

                destination_alive, dead_info = self._check_connection_beam(
                    destination, self._ndf_check_chk)
                first_alive_ip = destination_alive[0].split("_")[0]
                first_alive_port = int(destination_alive[0].split("_")[1])

                beam_index = self._acquire_beam_index(
                    first_alive_ip, first_alive_port, self._ndf_check_chk)
                refinfo = self._synced_refinfo(
                    self._utc_start_capture, first_alive_ip, first_alive_port)
                beam_index = self._acquire_beam_index(
                    first_alive_ip, first_alive_port, self._ndf_check_chk)
                refinfo = self._synced_refinfo(
                    self._utc_start_capture, first_alive_ip, first_alive_port)

                # To get directory for data and socket for control
                runtime_directory = "{}/beam{:02}".format(
                    DATA_ROOT, beam_index)
                if not os.path.isdir(runtime_directory):
                    try:
                        os.makedirs(directory)
                    except Exception as error:
                        log.exception(error)
                        self.state = "error"
                        log.error("Fail to create {}".format(runtime_directory))
                        raise PipelineError(
                            "Fail to create {}".format(runtime_directory))

                socket_address = "{}/capture.socket".format(runtime_directory)
                control_socket = socket.socket(
                    socket.AF_UNIX, socket.SOCK_DGRAM)
                control_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            else:
                destination_alive = []
                dead_info = []

                beam_index = i
                refinfo = [0, 0, 0]

                socket_address = None
                control_socket = None
                runtime_directory = None

            self._beam_index.append(beam_index)
            self._runtime_directory.append(runtime_directory)
            self._socket_address.append(socket_address)
            self._control_socket.append(control_socket)

            # To setup CPU bind information and dead_info
            cpu = self._numa * self._ncpu_numa + i * self._ncpu_pipeline
            alive_info = []
            for info in destination_alive:
                alive_info.append("{}_{}".format(info, cpu))
                cpu += 1

            buf_control_cpu = self._numa * self._ncpu_numa + \
                i * self._ncpu_pipeline + self._nport_beam
            capture_control_cpu = self._numa * self._ncpu_numa + \
                i * self._ncpu_pipeline + self._nport_beam
            capture_control = "1_{}".format(capture_control_cpu)
            refinfo = "{}_{}_{}".format(refinfo[0], refinfo[1], refinfo[2])

            # capture command
            command = ("{} -a {} -b {} -c {} -e {} -f {} -g {} -i {} -j {} "
                       "-k {} -l {} -m {} -n {} -o {} -p {} -q {} -r {}").format(
                           capture, self._rbuf_baseband_key[i], self._df_hdrsz, " -c ".join(alive_info),
                           self._freq, refinfo, runtime_directory, buf_control_cpu, capture_control, self._bind,
                           self._rbuf_baseband_ndf_chk, self._tbuf_baseband_ndf_chk,
                           self._dada_hdr_fname, PAF_CONFIG["instrument_name"], SOURCE_DEFAULT, self._pad, beam_index)
            self._capture_commands.append(command)

            # baseband2spectral command
            baseband2spectral_cpu = self._numa * self._ncpu_numa + i * self._ncpu_pipeline + 1
            command = ("taskset -c {} {} -a {} -b k_{}_1 -c {} "
                       "-d {} -e {} -f {} -g {} -i {} -j {} ").format(
                           baseband2spectral_cpu, baseband2spectral,
                           self._rbuf_baseband_key[i], self._rbuf_spectral_key[i],
                           self._rbuf_spectral_ndf_chk, self._nstream, self._ndf_stream,
                           self._runtime_directory[i], self._nchk_beam, self._cufft_nx, self._ptype)
            self._baseband2spectral_commands.append(command)

            # Command to create spectral ring buffer
            dadadb_cpu = self._numa * self._ncpu_numa +\
                (i + 1) * self._ncpu_pipeline - 1
            command = ("taskset -c {} dada_db -l -p -k {} "
                       "-b {} -n {} -r {}").format(
                           dadadb_cpu, self._rbuf_spectral_key[i],
                           self._rbuf_spectral_blksz,
                           self._rbuf_spectral_nblk,
                           self._rbuf_spectral_nread)
            self._spectral_create_buffer_commands.append(command)

            # command to create baseband ring buffer
            command = ("taskset -c {} dada_db -l -p -k {} "
                       "-b {} -n {} -r {}").format(
                           dadadb_cpu, self._rbuf_baseband_key[i],
                           self._rbuf_baseband_blksz,
                           self._rbuf_baseband_nblk,
                           self._rbuf_baseband_nread)
            self._baseband_create_buffer_commands.append(command)

            # command to delete spectral ring buffer
            command = "taskset -c {} dada_db -d -k {}".format(dadadb_cpu, self._rbuf_spectral_key[i])
            self._spectral_delete_buffer_commands.append(command)

            # command to delete baseband ring buffer
            command = "taskset -c {} dada_db -d -k {}".format(dadadb_cpu, self._rbuf_baseband_key[i])
            self._baseband_delete_buffer_commands.append(command)

            # Command to run dbdisk
            dbdisk_cpu = self._numa * self._ncpu_numa +\
                (i + 1) * self._ncpu_pipeline - 1
            command = ("dada_dbdisk -b {} -k {} "
                       "-D {} -o -s -z").format(
                           dbdisk_cpu,
                           self._rbuf_spectral_key[i],
                           self._runtime_directory[i])
            self._dbdisk_commands.append(command)

        # Create baseband ring buffer
        process_index = 0
        execution_instances = []
        for command in self._baseband_create_buffer_commands:
            execution_instances.append(ExecuteCommand(command, process_index))
            process_index += 1
        for execution_instance in execution_instances:
            execution_instance.finish()

        # Execute the capture
        process_index = 0
        self._capture_execution_instances = []
        for command in self._capture_commands:
            execution_instance = ExecuteCommand(command, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.stderr_callbacks.add(
                self._handle_execution_stderr)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._capture_execution_instances.append(execution_instance)
            process_index += 1
            
        if EXECUTE:  # Ready when all capture threads and the capture control thread of all capture instances are ready
            self._ready_counter = 0
            while True:
                if self._ready_counter == (self._nport_beam + 1) * self._nbeam:
                    break

        for execution_instance in self._capture_execution_instances:            
            execution_instance.stdout_callbacks.remove(
                self._ready_counter_callback)
            execution_instance.stdout_callbacks.add(
                self._capture_status_callback)
            
        self.state = "ready"
        log.info("Ready")
        
    def start(self, utc_start_process, source_name, ra, dec):
        log.info("Received 'start' command")
        if self.state != "ready":
            self.state = "error"
            log.error("Pipeline can only be started from ready state")
            raise PipelineError(
                "Pipeline can only be started from ready state")
        self.state = "starting"
        log.info("Starting")
        
        utc_start_process = Time(utc_start_process, format='isot', scale='utc')

        # Create ring buffer for spectral data
        process_index = 0
        execution_instances = []
        for command in self._spectral_create_buffer_commands:
            execution_instances.append(ExecuteCommand(command, process_index))
            process_index += 1
        for execution_instance in execution_instances:         # Wait until the buffer creation is done
            execution_instance.finish()
                
        # Run baseband2spectral
        process_index = 0
        self._baseband2spectral_execution_instances = []
        for command in self._baseband2spectral_commands:
            execution_instance = ExecuteCommand(command, process_index)
            execution_instance.stdout_callbacks.add(
                self._ready_counter_callback)
            execution_instance.stderr_callbacks.add(
                self._handle_execution_stderr)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._baseband2spectral_execution_instances.append(
                execution_instance)
            process_index += 1
                
        # Run dbdisk 
        process_index = 0
        self._dbdisk_execution_instances = []
        for command in self._dbdisk_commands:
            execution_instance = ExecuteCommand(command, process_index)
            execution_instance.returncode_callbacks.add(
                self._handle_execution_returncode)
            self._dbdisk_execution_instances.append(
                execution_instance)
            process_index += 1

        # Enable the SOD of baseband ring buffer with given time and then
        # "running"
        if EXECUTE:
            self._ready_counter = 0
            while True:
                if self._ready_counter == self._nbeam:
                    break
            process_index = 0
            start_buf = self._synced_startbuf(utc_start_process, self._utc_start_capture)
            log.debug("START BUF index is {}".format(start_buf))
            for control_socket in self._control_socket:
                self._capture_control(control_socket,
                                      "START-OF-DATA_{}_{}_{}_{}".format(
                                          source_name, ra, dec, start_buf),
                                      self._socket_address[process_index])
                process_index += 1

        # We do not need to monitor the stdout anymore
        for execution_instance in self._baseband2spectral_execution_instances:
            execution_instance.stdout_callbacks.remove(self._ready_counter_callback)
            
        self.state = "running"
        log.info("Running")
        
    def stop(self):
        log.info("Received 'stop' command")
        if self.state != "running":
            self.state = "error"
            log.error("Can only stop a running pipeline")
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"
        log.info("Stopping")
        
        if EXECUTE:
            process_index = 0
            for control_socket in self._control_socket:  # Stop data
                self._capture_control(control_socket,
                                      "END-OF-DATA",
                                      self._socket_address[process_index])
                process_index += 1
        for execution_instance in self._dbdisk_execution_instances:
            execution_instance.finish()
        for execution_instance in self._baseband2spectral_execution_instances:
            execution_instance.finish()

        # To delete spectral ring buffer
        process_index = 0
        execution_instances = []
        for command in self._spectral_delete_buffer_commands:
            execution_instances.append(ExecuteCommand(command, process_index))
            process_index += 1
        for execution_instance in execution_instances:
            execution_instance.finish()

        self.state = "ready"
        log.info("Ready")

    def deconfigure(self):
        log.info("Receive 'deconfigure' command")
        if self.state not in ["ready", "error"]:
            self.state = "error"
            log.error("Pipeline can only be deconfigured from ready or error state")
            raise PipelineError(
                "Pipeline can only be deconfigured from ready or error state")
        log.info("Deconfiguring")
        
        if self.state == "ready":  # Normal deconfigure
            self.state = "deconfiguring"

            # To stop the capture
            if EXECUTE:
                process_index = 0
                for control_socket in self._control_socket:
                    self._capture_control(
                        control_socket, "END-OF-CAPTURE", self._socket_address[process_index])
                    process_index += 1
            for execution_instance in self._capture_execution_instances:
                execution_instance.finish()
            
            # To delete baseband ring buffer
            process_index = 0
            execution_instances = []
            for command in self._baseband_delete_buffer_commands:
                execution_instances.append(ExecuteCommand(command, process_index))
                process_index += 1
            for execution_instance in execution_instances:
                execution_instance.finish()

        else:  # Force deconfigure
            self.state = "deconfiguring"
            
        self.state = "idle"
        log.info("Idle")

@register_pipeline("Spectral2Beams1Pol")
class Spectral2Beams1Pol(Spectral):

    def configure(self, utc_start_capture, freq, ip):
        super(Spectral2Beams1Pol, self).configure(
            utc_start_capture, freq, ip, 1, SPECTRAL_CONFIG_2BEAMS)


@register_pipeline("Spectral1Beam1Pol")
class Spectral1Beam1Pol(Spectral):

    def configure(self, utc_start_capture, freq, ip):
        super(Spectral1Beam1Pol, self).configure(
            utc_start_capture, freq, ip, 1, SPECTRAL_CONFIG_1BEAM)

@register_pipeline("Spectral2Beams2Pols")
class Spectral2Beams2Pols(Spectral):

    def configure(self, utc_start_capture, freq, ip):
        super(Spectral2Beams2Pols, self).configure(
            utc_start_capture, freq, ip, 2, SPECTRAL_CONFIG_2BEAMS)


@register_pipeline("Spectral1Beam2Pols")
class Spectral1Beam2Pols(Spectral):

    def configure(self, utc_start_capture, freq, ip):
        super(Spectral1Beam2Pols, self).configure(
            utc_start_capture, freq, ip, 2, SPECTRAL_CONFIG_1BEAM)

@register_pipeline("Spectral2Beams4Pols")
class Spectral2Beams4Pols(Spectral):

    def configure(self, utc_start_capture, freq, ip):
        super(Spectral2Beams4Pols, self).configure(
            utc_start_capture, freq, ip, 4, SPECTRAL_CONFIG_2BEAMS)


@register_pipeline("Spectral1Beam4Pols")
class Spectral1Beam4Pols(Spectral):

    def configure(self, utc_start_capture, freq, ip):
        super(Spectral1Beam4Pols, self).configure(
            utc_start_capture, freq, ip, 4, SPECTRAL_CONFIG_1BEAM)

if __name__ == "__main__":
    logging.getLogger().addHandler(logging.NullHandler())
    log = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level='DEBUG',
        logger=log)
    
    source_name = "DEBUG"
    ra = "00:00:00.00"   # "HH:MM:SS.SS"
    dec = "00:00:00.00"   # "DD:MM:SS.SS"
    host_id = check_output("hostname").strip()[-1]

    parser = argparse.ArgumentParser(
        description='To run the pipeline for my test')
    parser.add_argument('-a', '--numa', type=int, nargs='+',
                        help='The ID of numa node')
    parser.add_argument('-b', '--beam', type=int, nargs='+',
                        help='The number of beams')

    args = parser.parse_args()
    numa = args.numa[0]
    beam = args.beam[0]
    ip = "10.17.{}.{}".format(host_id, numa + 1)
        
    for i in range(1):
        log.info("Create pipeline ...")
        if beam == 1:
            freq = 1340.5
            search_mode = Search1Beam()
        if beam == 2:
            freq = 1337.0
            search_mode = Search2Beams()
    
        log.info("Configure it ...")
        utc_start_capture = Time.now()  
        search_mode.configure(utc_start_capture, freq, ip)
    
        for j in range(1):
            log.info("Start it ...")
            utc_start_process = Time.now() + 10 * units.second
            search_mode.start(utc_start_process, source_name, ra, dec)
            time.sleep(10)
            log.info("Stop it ...")
            search_mode.stop()
    
        log.info("Deconfigure it ...")
        search_mode.deconfigure()
    
    #for i in range(1):
    #    log.info("Create pipeline ...")
    #    if beam == 1:
    #        freq = 1340.5
    #        spectral_mode = Spectral1Beam1Pol()
    #        spectral_mode = Spectral1Beam2Pols()
    #        spectral_mode = Spectral1Beam4Pols()
    #    if beam == 2:
    #        freq = 1337.0
    #        spectral_mode = Spectral2Beams1Pol()
    #        spectral_mode = Spectral2Beams2Pols()
    #        spectral_mode = Spectral2Beams4Pols()
    #
    #    log.info("Configure it ...")
    #    utc_start_capture = Time.now()  
    #    spectral_mode.configure(utc_start_capture, freq, ip)
    #
    #    for j in range(1):
    #        log.info("Start it ...")
    #        utc_start_process = Time.now() + 15 * units.second
    #        spectral_mode.start(utc_start_process, source_name, ra, dec)
    #        time.sleep(10)
    #        log.info("Stop it ...")
    #        spectral_mode.stop()
    #
    #    log.info("Deconfigure it ...")
    #    spectral_mode.deconfigure()
