#!/usr/bin/env python

import ConfigParser
import json
import numpy as np
import socket
import struct
import time
import shlex
from subprocess import PIPE, Popen, check_call, check_output
from inspect import currentframe, getframeinfo
from astropy.time import Time
import astropy.units as units
import logging
from katcp import Sensor
import argparse
import threading
import inspect

# 1. Why capture does not return immediately?
# 2. why bad memory access?

log = logging.getLogger("mpikat.paf_pipeline")

EXECUTE = True
#EXECUTE        = False

NVPROF = True
#NVPROF         = False

FILTERBANK_SOD = True   # Start filterbank data
# FILTERBANK_SOD  = False  # Do not start filterbank data

HEIMDALL = False   # To run heimdall on filterbank file or not
# HEIMDALL       = True   # To run heimdall on filterbank file or not

DBDISK = True   # To run dbdisk on filterbank file or not
# DBDISK         = False   # To run dbdisk on filterbank file or not

PAF_ROOT = "/home/pulsar/xinping/phased-array-feed/"
DATA_ROOT = "/beegfs/DENG/"
DADA_ROOT = "{}/AUG/baseband/".format(DATA_ROOT)
SOURCE_DEFAULT = "UNKNOW;00:00:00.00;00:00:00.00"
DADA_HDR_FNAME = "{}/config/header_16bit.txt".format(PAF_ROOT)

PAF_CONFIG = {"instrument_name":    "PAF-BMF",
              "nchan_chk":    	     7,        # MHz
              "samp_rate":    	     0.84375,
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
              "first_port":          17100,
              }

SEARCH_CONFIG_GENERAL = {"rbuf_baseband_ndf_chk":   16384,
                         "rbuf_baseband_nblk":      4,
                         "rbuf_baseband_nread":     1,
                         "tbuf_baseband_ndf_chk":   128,

                         "rbuf_filterbank_ndf_chk": 16384,
                         "rbuf_filterbank_nblk":    20,
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

SEARCH_CONFIG_1BEAM = {"rbuf_baseband_key":      ["dada"],
                       "rbuf_filterbank_key":    ["dade"],
                       "nchan_keep_band":        32768,
                       "nbeam":                  1,
                       "nport_beam":             3,
                       "nchk_port":              16,
                       }

SEARCH_CONFIG_2BEAMS = {"rbuf_baseband_key":       ["dada", "dadc"],
                        "rbuf_filterbank_key":     ["dade", "dadg"],
                        "nchan_keep_band":         24576,
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

    def __init__(self, command, resident=False):
        self._command = command
        self._resident = resident
        self.stdout_callbacks = set()
        self.error_callbacks = set()

        self._process = None
        self._executable_command = None
        self._monitor_thread = None
        self._stdout = None
        self._error = False

        self._finish_event = threading.Event()
        print self._command

        if not self._resident:  # For the command which stops immediately, we need to set the event before hand
            self._finish_event.set()

        log.info(self._command)
        self._executable_command = shlex.split(self._command)
        log.info(self._executable_command)

        if EXECUTE:
            try:
                self._process = Popen(self._executable_command,
                                      stdout=PIPE,
                                      stderr=PIPE,
                                      bufsize=1,
                                      universal_newlines=True)
                print self._process
            except Exception as error:
                log.exception("Error while launching command: {}".format(self._executable_command))
                self.error = True
            else:
                self._monitor_thread = threading.Thread(
                    target=self._execution_monitor)
                self._monitor_thread.start()

    def __del__(self):
        class_name = self.__class__.__name__

    def set_finish_event(self):
        if not self._finish_event.isSet():
            self._finish_event.set()

    def finish(self):
        if EXECUTE:
            self._monitor_thread.join()

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

    def error_notify(self):
        for callback in self.error_callbacks:
            callback(self._error, self)

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, value):
        self._error = value
        self.error_notify()

    def _execution_monitor(self):
        # Monitor the execution and also the stdout for the outside useage
        if EXECUTE:
            while self._process.poll() == None:
                stdout = self._process.stdout.readline().rstrip("\n\r")

                if stdout != b"":
                    self.stdout = stdout
                    print self.stdout, self._command

            if not self._finish_event.isSet():
                # For the command which runs for a while, if it stops before
                # the event is set, the command does not successfully finish
                stderr = self._process.stderr.read()
                log.error(
                    "Process exited unexpectedly with return code: {}".format(self._process.returncode))
                self.error = True


class Pipeline(object):

    def __init__(self):
        self._sensors = []
        self.callbacks = set()
        self._capture_ready_counter = 0
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
                raise PipelineError(
                    "Beams are mixed up, please check the routing table")
            sock.close()
            return beam_index[0]
        except:
            sock.close()
            self.state = "error"
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _synced_refinfo(self, utc_start_capture, ip, port):
        utc_start_capture = Time(utc_start_capture, format='isot', scale='utc')

        # Capture one packet to see what is current epoch, seconds and idf
        # We need to do that because the seconds is not always matched with
        # estimated value
        epoch_ref, sec_ref, idf_ref = self._refinfo(ip, port)
        print epoch_ref, sec_ref, idf_ref

        if(utc_start_capture.unix - epoch_ref * 86400.0 - sec_ref) > PAF_CONFIG["prd"]:
            sec_ref = sec_ref + PAF_CONFIG["prd"]
        idf_ref = int((utc_start_capture.unix - epoch_ref *
                       86400.0 - sec_ref) / self._df_res)
        print epoch_ref, int(sec_ref), idf_ref

        return epoch_ref, int(sec_ref), idf_ref

    def _refinfo(self, ip, port):
        """
        To get reference information for capture
        """
        data = bytearray(self._df_pktsz)
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
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

            return epoch_ref, sec_ref, idf_ref
        except:
            sock.close()
            self.state = "error"
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
            ip = destination[i].split(";")[0]
            port = int(destination[i].split(";")[1])
            alive, nchk_alive = self._check_connection_port(
                ip, port, ndf_check_chk)

            if alive == 1:
                destination_alive.append(
                    destination[i] + ";{}".format(nchk_alive))
            else:
                destination_dead.append(destination[i])

        if (len(destination_alive) == 0):  # No alive ports, error
            self.state = "error"
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
        except:
            self.state = "error"
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))
        return alive, nchk_alive

    def _capture_control(self, ctrl_socket, command, socket_address):
        if EXECUTE:
            try:
                ctrl_socket.sendto(command, socket_address)
            except:
                self.state = "error"
                raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _handle_execution_error(self, state, callback):
        if EXECUTE:
            log.info('New state of ExecuteCommand is {}'.format(str(state)))
            if state:
                self.state = "error"
                raise PipelineError("ExecuteCommand fail")

    def _decode_capture_stdout(self, stdout, callback):
        if EXECUTE:
            log.debug('New stdout of capture is {}'.format(str(stdout)))
            if stdout.find("CAPTURE_READY") != -1:
                self._capture_ready_counter += 1
            if stdout.find("CAPTURE_STATUS") != -1:
                capture_status = stdout.split(" ")
                print "HERE CAPTURE_STATUS", stdout, capture_status
                process_index = int(capture_status[1])
                if process_index == 0:
                    self._beam_sensor0.set_value(float(self._beam_index[0]))
                    self._time_sensor0.set_value(float(capture_status[2]))
                    self._average_sensor0.set_value(float(capture_status[3]))
                    self._instant_sensor0.set_value(float(capture_status[4]))
                if process_index == 1:
                    self._beam_sensor1.set_value(float(self._beam_index[1]))
                    self._time_sensor1.set_value(float(capture_status[2]))
                    self._average_sensor1.set_value(float(capture_status[3]))
                    self._instant_sensor1.set_value(float(capture_status[4]))


@register_pipeline("Search")
class Search(Pipeline):

    def _decode_baseband2filterbank_stdout(self, stdout, callback):
        if EXECUTE:
            log.info('New stdout of baseband2filterbank is {}'.format(str(stdout)))
            if stdout.find("BASEBAND2FILTERBANK_READY") != -1:
                self._baseband2filterbank_ready_counter += 1

    def __init__(self):
        super(Search, self).__init__()
        self.state = "idle"

        self._baseband2filterbank_ready_counter = 0

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

        self._cleanup_commands = ["pkill -f capture_main",
                                  "pkill -f baseband2filterbank_main",
                                  "pkill -f heimdall",
                                  "pkill -f dada_dbdisk",
                                  "ipcrm -a"]

    def configure(self, utc_start_capture, freq, ip, pipeline_config):
        if (self.state != "idle"):
            raise PipelineError("Can only configure pipeline in idle state")

        # Setup parameters of the pipeline
        self.state = "configuring"
        self._pipeline_config = pipeline_config
        self._ip = ip
        self._numa = int(ip.split(".")[3]) - 1
        self._freq = freq

        self._nbeam = self._pipeline_config["nbeam"]
        self._nchk_port = self._pipeline_config["nchk_port"]
        self._nport_beam = self._pipeline_config["nport_beam"]
        self._nchan_keep_band = self._pipeline_config["nchan_keep_band"]
        self._rbuf_baseband_key = self._pipeline_config["rbuf_baseband_key"]
        self._rbuf_filterbank_key = self._pipeline_config[
            "rbuf_filterbank_key"]

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

        if self._rbuf_baseband_ndf_chk % (self._ndf_stream * self._nstream):
            self.state = "error"
            raise PipelineError("data in baseband ring buffer block can only "
                                "be processed by baseband2filterbank with integer repeats")

        # To be safe, kill all related softwares and free shared memory
        execution_instances = []
        for command in self._cleanup_commands:
            execution_instances.append(ExecuteCommand(command))
        for execution_instance in execution_instances:         # Wait until the cleanup is done
            execution_instance.set_finish_event()
            execution_instance.finish()
        print self._state, "HERE\n"

        # To setup commands for each process
        capture = "{}/src/capture_main".format(PAF_ROOT)
        baseband2filterbank = "{}/src/baseband2filterbank_main".format(
            PAF_ROOT)
        for i in range(self._nbeam):
            if EXECUTE:
                # To setup address
                destination = []
                for j in range(self._nport_beam):
                    port = self._first_port + i * self._nport_beam + j
                    destination.append("{};{};{}".format(
                        self._ip, port, self._nchk_port))

                destination_alive, dead_info = self._check_connection_beam(
                    destination, self._ndf_check_chk)
                first_alive_ip = destination_alive[0].split(";")[0]
                first_alive_port = int(destination_alive[0].split(";")[1])

                beam_index = self._acquire_beam_index(
                    first_alive_ip, first_alive_port, self._ndf_check_chk)
                refinfo = self._synced_refinfo(
                    utc_start_capture, first_alive_ip, first_alive_port)

                socket_address = "{}/beam{:02}/capture.socket".format(
                    DATA_ROOT, beam_index)
                control_socket = socket.socket(
                    socket.AF_UNIX, socket.SOCK_DGRAM)
            else:
                destination_alive = []
                dead_info = []

                beam_index = i
                refinfo = [0, 0, 0]

                socket_address = None
                control_socket = None
            self._beam_index.append(beam_index)

            # To get directory for data and socket for control
            runtime_directory = "{}/beam{:02}".format(DATA_ROOT, beam_index)
            self._runtime_directory.append(runtime_directory)
            self._socket_address.append(socket_address)
            self._control_socket.append(control_socket)

            # To setup CPU bind information and dead_info
            cpu = self._numa * self._ncpu_numa + i * self._ncpu_pipeline
            alive_info = []
            for info in destination_alive:
                alive_info.append("{};{}".format(info, cpu))
                cpu += 1
            buf_control_cpu = self._numa * self._ncpu_numa + \
                i * self._ncpu_pipeline + self._nport_beam
            capture_control_cpu = self._numa * self._ncpu_numa + \
                i * self._ncpu_pipeline + self._nport_beam
            capture_control = "1;{}".format(capture_control_cpu)
            refinfo = "{};{};{}".format(refinfo[0], refinfo[1], refinfo[2])

            # capture command
            self._capture_commands.append(
                ("{} -a {} -b {} -c {} -e {} -f {} -g {} -i {} -j {} "
                 "-k {} -l {} -m {} -n {} -o {} -p {} -q {} -r {}").format(
                     capture, self._rbuf_baseband_key[
                         i], self._df_hdrsz, " -c ".join(alive_info),
                     self._freq, refinfo, runtime_directory, buf_control_cpu, capture_control, self._bind,
                     self._rbuf_baseband_ndf_chk, self._tbuf_baseband_ndf_chk,
                     DADA_HDR_FNAME, PAF_CONFIG["instrument_name"], SOURCE_DEFAULT, self._pad, i))

            # baseband2filterbank command
            baseband2filterbank_cpu = self._numa * self._ncpu_numa +\
                (i + 1) * self._ncpu_pipeline - 1
            command = "taskset -c {} ".format(baseband2filterbank_cpu)
            if NVPROF:
                command += "nvprof "
            command += ("{} -a {} -b {} -c {} -d {} -e {} "
                        "-f {} -i {} -j {} -k {} -l {} ").format(baseband2filterbank, self._rbuf_baseband_key[i], self._rbuf_filterbank_key[i],                                                                 self._rbuf_filterbank_ndf_chk, self._nstream, self._ndf_stream,
                                                                 self._runtime_directory[
                                                                     i], self._nchk_beam, self._cufft_nx,
                                                                 self._nchan_filterbank, self._nchan_keep_band)
            if FILTERBANK_SOD:
                command += "-g 1"
            else:
                command += "-g 0"
            self._baseband2filterbank_commands.append(command)

            # Command to create filterbank ring buffer
            self._filterbank_create_buffer_commands.append(("dada_db -l -p  -k {:} "
                                                            "-b {:} -n {:} -r {:}").format(self._rbuf_filterbank_key[i],
                                                                                           self._rbuf_filterbank_blksz,
                                                                                           self._rbuf_filterbank_nblk,
                                                                                           self._rbuf_filterbank_nread))

            # command to create baseband ring buffer
            self._baseband_create_buffer_commands.append(("dada_db -l -p  -k {:} "
                                                          "-b {:} -n {:} -r {:}").format(self._rbuf_baseband_key[i],
                                                                                         self._rbuf_baseband_blksz,
                                                                                         self._rbuf_baseband_nblk,
                                                                                         self._rbuf_baseband_nread))

            # command to delete filterbank ring buffer
            self._filterbank_delete_buffer_commands.append(
                "dada_db -d -k {:}".format(self._rbuf_filterbank_key[i]))

            # command to delete baseband ring buffer
            self._baseband_delete_buffer_commands.append(
                "dada_db -d -k {:}".format(self._rbuf_baseband_key[i]))

            # Command to run heimdall
            heimdall_cpu = self._numa * self._ncpu_numa +\
                (i + 1) * self._ncpu_pipeline - 1
            command = "taskset -c {} ".format(heimdall_cpu)
            if NVPROF:
                command += "nvprof "
            command += ("heimdall -k {} -detect_thresh {} -output_dir {} ").format(self._rbuf_filterbank_key[i],
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
            command = "dada_dbdisk -b {} -k {} -D {} -o -s -z".format(
                dbdisk_cpu, self._rbuf_filterbank_key[i], self._runtime_directory[i])
            self._dbdisk_commands.append(command)

        # Create baseband ring buffer
        execution_instances = []
        for command in self._baseband_create_buffer_commands:
            execution_instances.append(ExecuteCommand(command))
        for execution_instance in execution_instances:
            execution_instance.set_finish_event()
        for execution_instance in execution_instances:
            execution_instance.finish()

        # Execute the capture
        self._capture_execution_instances = []
        for command in self._capture_commands:
            execution_instance = ExecuteCommand(command, resident=True)
            execution_instance.stdout_callbacks.add(
                self._decode_capture_stdout)
            execution_instance.error_callbacks.add(
                self._handle_execution_error)
            self._capture_execution_instances.append(execution_instance)

        if EXECUTE:  # Ready when all capture threads and the capture control thread of all capture instances are ready
            self._capture_ready_counter = 0
            while True:
                if self._capture_ready_counter == (self._nport_beam + 1) * self._nbeam:
                    break

        self.state = "ready"
        print self.state, "HERE\n"

    def start(self, utc_start_process, source_name, ra, dec):
        if self.state != "ready":
            raise PipelineError(
                "Pipeline can only be started from ready state")
        self.state = "starting"
        print self.state, "HERE\n"

        # Create ring buffer for filterbank data
        execution_instances = []
        for command in self._filterbank_create_buffer_commands:
            execution_instances.append(ExecuteCommand(command))
        for execution_instance in execution_instances:         # Wait until the buffer creation is done
            execution_instance.set_finish_event()
        for execution_instance in execution_instances:         # Wait until the buffer creation is done
            execution_instance.finish()

        # Run baseband2filterbank
        self._baseband2filterbank_execution_instances = []
        for command in self._baseband2filterbank_commands:
            baseband2filterbank_execution_instance = ExecuteCommand(
                command, resident=True)
            baseband2filterbank_execution_instance.stdout_callbacks.add(
                self._decode_baseband2filterbank_stdout)
            baseband2filterbank_execution_instance.error_callbacks.add(
                self._handle_execution_error)
            self._baseband2filterbank_execution_instances.append(
                baseband2filterbank_execution_instance)

        if HEIMDALL:  # run heimdall if required
            self._heimdall_execution_instances = []
            for command in self._heimdall_commands:
                heimdall_execution_instance = ExecuteCommand(
                    command, resident=True)
                heimdall_execution_instance.error_callbacks.add(
                    self._handle_execution_error)
                self._heimdall_execution_instances.append(
                    heimdall_execution_instance)
        if DBDISK:   # Run dbdisk if required
            self._dbdisk_execution_instances = []
            for command in self._dbdisk_commands:
                dbdisk_execution_instance = ExecuteCommand(
                    command, resident=True)
                dbdisk_execution_instance.error_callbacks.add(
                    self._handle_execution_error)
                self._dbdisk_execution_instances.append(
                    dbdisk_execution_instance)

        # Enable the SOD of baseband ring buffer with given time and then
        # "running"
        if EXECUTE:
            self._baseband2filterbank_ready_counter = 0
            while True:
                if self._baseband2filterbank_ready_counter == self._nbeam:
                    break
            index = 0
            for control_socket in self._control_socket:
                self._capture_control(control_socket,
                                      "START-OF-DATA;{};{};{};{}".format(
                                          source_name, ra, dec, 0),
                                      self._socket_address[index])
                index += 1

        self.state = "running"
        print self.state, "HERE\n"

    def stop(self):
        if self.state != "running":
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"

        if DBDISK:
            for execution_instance in self._dbdisk_execution_instances:
                execution_instance.set_finish_event()
        if HEIMDALL:
            for execution_instance in self._heimdall_execution_instances:
                execution_instance.set_finish_event()
        for execution_instance in self._baseband2filterbank_execution_instances:
            execution_instance.set_finish_event()

        if EXECUTE:
            index = 0
            for control_socket in self._control_socket:  # Stop data
                self._capture_control(control_socket,
                                      "END-OF-DATA", self._socket_address[index])
                index += 1
        if DBDISK:
            for execution_instance in self._dbdisk_execution_instances:
                execution_instance.finish()
        if HEIMDALL:
            for execution_instance in self._heimdall_execution_instances:
                execution_instance.finish()
        for execution_instance in self._baseband2filterbank_execution_instances:
            execution_instance.finish()

        # To delete filterbank ring buffer
        execution_instances = []
        for command in self._filterbank_delete_buffer_commands:
            execution_instances.append(ExecuteCommand(command))
        for execution_instance in execution_instances:
            execution_instance.set_finish_event()
        for execution_instance in execution_instances:
            execution_instance.finish()

        self.state = "ready"
        print self.state, "HERE\n"

    def deconfigure(self):
        if self.state not in ["ready", "error"]:
            raise PipelineError(
                "Pipeline can only be deconfigured from ready or error state")

        print self.state, "HERE\n"
        if self.state == "ready":  # Normal deconfigure
            self.state = "deconfiguring"

            # To stop the capture
            for execution_instance in self._capture_execution_instances:
                execution_instance.set_finish_event()
            if EXECUTE:
                index = 0
                for control_socket in self._control_socket:
                    self._capture_control(
                        control_socket, "END-OF-CAPTURE", self._socket_address[index])
                    index += 1
            for execution_instance in self._capture_execution_instances:
                execution_instance.finish()
            print self.state, "HERE\n"

            # To delete baseband ring buffer
            execution_instances = []
            for command in self._baseband_delete_buffer_commands:
                execution_instances.append(ExecuteCommand(command))
            for execution_instance in execution_instances:
                execution_instance.set_finish_event()
            for execution_instance in execution_instances:
                execution_instance.finish()

        else:  # Force deconfigure
            self.state = "deconfiguring"
            execution_instances = []
            for command in self._cleanup_commands:
                execution_instances.append(ExecuteCommand(command))

            # Wait until the cleanup is done
            for execution_instance in execution_instances:
                execution_instance.set_finish_event()
            for execution_instance in execution_instances:
                execution_instance.finish()

        self.state = "idle"


@register_pipeline("Search2Beams")
class Search2Beams(Search):

    def __init__(self):
        super(Search2Beams, self).__init__()

    def configure(self, utc_start_capture, freq, ip):
        super(Search2Beams, self).configure(
            utc_start_capture, freq, ip, SEARCH_CONFIG_2BEAMS)

    def start(self, utc_start_process, source_name, ra, dec):
        super(Search2Beams, self).start(
            utc_start_process, source_name, ra, dec)

    def stop(self):
        super(Search2Beams, self).stop()

    def deconfigure(self):
        super(Search2Beams, self).deconfigure()


@register_pipeline("Search1Beam")
class Search1Beam(Search):

    def __init__(self):
        super(Search1Beam, self).__init__()

    def configure(self, utc_start_capture, freq, ip):
        super(Search1Beam, self).configure(
            utc_start_capture, freq, ip, SEARCH_CONFIG_1BEAM)

    def start(self, utc_start_process, source_name, ra, dec):
        super(Search1Beam, self).start(utc_start_process, source_name, ra, dec)

    def stop(self):
        super(Search1Beam, self).stop()

    def deconfigure(self):
        super(Search1Beam, self).deconfigure()

if __name__ == "__main__":
    # Question, why the reference seconds is 21 seconds less than the BMF number
    # The number is random. Everytime reconfigure stream, it will change the reference seconds, sometimes it is multiple times of 27 seconds, but in most case, it is not
    # To do, find a way to sync capture of beams
    # understand why the capture does not works sometimes, or try VMA
    #freq              = 1340.5
    freq = 1337.0
    # "YYYY-MM-DDThh:mm:ss", now + stream period (27 seconds)
    utc_start_capture = Time.now() + PAF_CONFIG["prd"] * units.s
    utc_start_process = utc_start_capture + PAF_CONFIG["prd"] * units.s

    #source_name   = "UNKNOWN"
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

    print "\nCreate pipeline ...\n"
    if beam == 1:
        freq = 1340.5
        search_mode = Search1Beam()
    if beam == 2:
        freq = 1337.0
        search_mode = Search2Beams()

    print "\nConfigure it ...\n"
    search_mode.configure(utc_start_capture, freq, ip)

    for i in range(10):
        print "\nStart it ...\n"
        search_mode.start(utc_start_process, source_name, ra, dec)
        time.sleep(10)
        print "\nStop it ...\n"
        search_mode.stop()

    print "\nDeconfigure it ...\n"
    search_mode.deconfigure()
