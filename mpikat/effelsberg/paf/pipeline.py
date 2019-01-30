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

# To do,
# 2. sensor to report status, DONE;
# 3. Why full bandwidth is worse;
# 4. why thread quit, not thread quit, it only stucks there because of the full stdout buffer;

log = logging.getLogger("mpikat.paf_pipeline")

EXECUTE        = True
#EXECUTE        = False

#SOD            = True   # To start filterbank data or not
SOD            = False   # To start filterbank data or not

HEIMDALL       = False   # To run heimdall on filterbank file or not
#HEIMDALL       = True   # To run heimdall on filterbank file or not

#DBDISK         = True   # To run dbdisk on filterbank file or not
DBDISK         = False   # To run dbdisk on filterbank file or not

PAF_ROOT       = "/home/pulsar/xinping/phased-array-feed/"
DATA_ROOT      = "/beegfs/DENG/"
DADA_ROOT      = "{}/AUG/baseband/".format(DATA_ROOT)
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

SPECTRAL_CONFIG_GENERAL = {}
SPECTRAL_CONFIG_1BEAM = {}
SPECTRAL_CONFIG_2BEAMS = {}

PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "error"]

# Epoch of BMF timing system, it updates every 0.5 year, [UTC datetime, EPOCH]
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

class ExecuteCommandError(Exception):
    pass

class ExecuteCommand(object):
    def __init__(self, command):
        self._event              = threading.Event()
        self._monitor_threads    = []
        self.callbacks           = []
        self._command            = command
        self._returncode         = None
        self._process            = None
        self._stderr             = None
        self._stdout             = None
        self._executable_command = None
        log.info(self._command)
        
    def __del__(self):
        class_name = self.__class__.__name__

    def notify(self):
        for callback in self.callbacks:
            callback(self._stdout, self)

    def execute(self):
        self._executable_command = shlex.split(self._command)
        log.info(self._executable_command)
        
        if EXECUTE:
            try:
                self._process = Popen(self._executable_command, stderr=PIPE, stdout=PIPE)                                     
            except:
                raise ExecuteCommandError("can not execute {}".format(self._command))

            self._monitor_threads.append(threading.Thread(target=self._execution_monitor)) # Monitor the execution of command
            self._monitor_threads.append(threading.Thread(target=self._stdout_monitor))    # Monitor the stdout of command
            self._monitor_threads.append(threading.Thread(target=self._stderr_monitor))    # Monitor the stderr of command

            for thread in self._monitor_threads:
                thread.start()
                
    def finish(self):
        if EXECUTE:
            self._event.set()
            for thread in self._monitor_threads:
                thread.join()

    @property
    def stdout(self):
        return self._stdout

    @stdout.setter
    def stdout(self, value):
        self._stdout = value
        self.notify()

    def _execution_monitor(self):
        if EXECUTE:
            self._process.communicate()
            self._returncode = self._process.returncode
            if self._returncode:
                raise ExecuteCommandError("{} fail".format(command))
        
    def _stdout_monitor(self):
        if EXECUTE:
            while not self._event.isSet():
                if self._process.stdout:
                    self.stdout = self._process.stdout.readline()
                    print self.stdout

    def _stderr_monitor(self):
        if EXECUTE:
            while not self._event.isSet():
                if self._process.stderr:
                    self._stderr = self._process.stderr.readline()
                    print self._stderr
                    raise ExecuteCommandError("{} fail".format(command))

class Pipeline(object):
    def __init__(self):
        self.state               = "idle"   # Idle at the very beginning
        self._sensors             = []
        self.callbacks            = set()
        self.beam                 = []
        self.setup_sensors()
        
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
        self._beam_sensor = Sensor.int(
            "beam",
            description = "The ID of current beam",
            default = 0,
            initial_status = Sensor.NOMINAL)
        self.sensors.append(self._beam_sensor)
        
        self._instant_sensor = Sensor.float(
            "instant",
            description = "The instant packet loss rate",
            default = 0,
            initial_status = Sensor.NOMINAL)
        self.sensors.append(self._instant_sensor)
        
        self._time_sensor = Sensor.float(
            "time",
            description = "The time so far in seconds",
            default = 0,
            initial_status = Sensor.NOMINAL)
        self.sensors.append(self._time_sensor)
        
        self._average_sensor = Sensor.float(
            "average",
            description = "The packet loss rate so far in average",
            default = 0,
            initial_status = Sensor.NOMINAL)
        self.sensors.append(self._average_sensor)
        
    @property
    def sensors(self):
        return self._sensors

    def _acquire_beam_index(self, ip, port):
        """
        To get the beam ID
        """
        data = bytearray(self.df_pktsz)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        socket.setdefaulttimeout(self.prd)  # Force to timeout after one data frame period
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            beam_index = []
            for i in range(self.ndf_check_chk):
                nbyte, address = sock.recvfrom_into(data, self.df_pktsz)
                data_uint64 = np.fromstring(str(data), 'uint64')
                hdr_uint64  = np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[2]))[0])
                beam_index.append(hdr_uint64 & np.uint64(0x000000000000ffff))

            if (len(list(set(beam_index)))>1):
                self.state = "error"
                raise PipelineError("Beams are mixed up, please check the routing table")
            sock.close()
            return beam_index[0]
        except:
            sock.close()
            self.state = "error"
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _synced_refinfo(self, utc_start_capture, ip, port):
        utc_start_capture = Time(utc_start_capture, format='isot', scale='utc')
        
        # Capture one packet to see what is current epoch, seconds and idf
        # We need to do that because the seconds is not always matched with estimated value
        epoch_ref, sec_ref, idf_ref = self.refinfo(ip, port)
        print epoch_ref, sec_ref, idf_ref
        
        if(utc_start_capture.unix - epoch_ref * 86400.0 - sec_ref) > PAF_CONFIG["prd"]:
            sec_ref = sec_ref + PAF_CONFIG["prd"]
        idf_ref = int((utc_start_capture.unix - epoch_ref * 86400.0 - sec_ref)/self.df_res)
        print epoch_ref, int(sec_ref), idf_ref
        
        return epoch_ref, int(sec_ref), idf_ref

    def _start_buf(self, utc_start_process, utc_start_capture):
        """
        To get the start buffer with given start time in UTC
        """
        utc_start_capture = Time(utc_start_capture, format='isot', scale='utc')
        utc_start_process = Time(utc_start_process, format='isot', scale='utc')

        start_buffer_idx = int((utc_start_process.unix - utc_start_capture.unix)/self.blk_res)
        
        return start_buffer_idx
        
    def _refinfo(self, ip, port):
        """
        To get reference information for capture
        """
        data = bytearray(self.df_pktsz)
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        socket.setdefaulttimeout(self.prd)  # Force to timeout after one data frame period
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            nbyte, address = sock.recvfrom_into(data, self.df_pktsz)                                                
            data     = np.fromstring(str(data), 'uint64')
            
            hdr_part = np.uint64(struct.unpack("<Q", struct.pack(">Q", data[0]))[0])
            sec_ref  = (hdr_part & np.uint64(0x3fffffff00000000)) >> np.uint64(32)
            idf_ref  = hdr_part & np.uint64(0x00000000ffffffff)

            hdr_part  = np.uint64(struct.unpack("<Q", struct.pack(">Q", data[1]))[0])
            epoch_idx = (hdr_part & np.uint64(0x00000000fc000000)) >> np.uint64(26)
            
            for epoch in EPOCHS:
                if epoch[1] == epoch_idx:
                    break
            epoch_ref = int(epoch[0].unix/86400.0)

            sock.close()

            return epoch_ref, sec_ref, idf_ref
        except:
            sock.close()
            self.state = "error"
            raise PipelineError("{} fail".format(inspect.stack()[0][3]))

    def _check_connection_beam(self, destination):
        """
        To check the connection of one beam with given ip and port numbers
        """
        nport = len(destination)
        alive = np.zeros(nport, dtype = int)
        nchk_alive = np.zeros(nport, dtype = int)

        destination_dead  = []   # The destination where we can not receive data
        destination_alive = []   # The destination where we can receive data
        for i in range(nport):
            ip   = destination[i].split(";")[0]
            port = int(destination[i].split(";")[1])
            alive, nchk_alive = self._check_connection_port(ip, port)                                                

            if alive == 1:
                destination_alive.append(destination[i]+";{}".format(nchk_alive))
            else:
                destination_dead.append(destination[i])

        if (len(destination_alive) == 0): # No alive ports, error
            self.state = "error"
            raise PipelineError("The stream is not alive")

        return destination_alive, destination_dead

    def _check_connection_port(self, ip, port):
        """
        To check the connection of single port
        """
        alive = 1
        nchk_alive = 0
        data = bytearray(self.df_pktsz)
        sock = socket.socket(socket.AF_INET,
                             socket.SOCK_DGRAM)
        socket.setdefaulttimeout(self.prd)  # Force to timeout after one data frame period
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            nbyte, address = sock.recvfrom_into(data, self.df_pktsz)                                                
            if (nbyte != self.df_pktsz):
                alive = 0
            else:
                source = []
                alive = 1
                for i in range(self.ndf_check_chk):
                    buf, address = sock.recvfrom(self.df_pktsz)
                    
                    data_uint64 = np.fromstring(str(buf), 'uint64')
                    hdr_uint64  = np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[2]))[0])
                    
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
            
    def _decode_capture_stdout(self, stdout, callback):
        log.info('New stdout of capture is {}'.format(str(stdout)))
        if stdout.find("CAPTURE_READY") != -1:
            self.capture_ready_counter += 1
            
        print stdout

@register_pipeline("Search")
class Search(Pipeline):
    def __init__(self):
        super(Search, self).__init__()
        self.capture_executions    = []
        self.capture_ready_counter = 0
        
    def configure(self, utc_start_capture, freq, ip, general_config, pipeline_config):
        if (self.state != "idle"):
            raise PipelineError(
                "Can only configure pipeline in idle state")

        # Setup parameters of the pipeline
        self.state              = "configuring"
        self._general_config    = general_config
        self._pipeline_config   = pipeline_config
        self._ip                = ip
        self._numa              = int(ip.split(".")[3]) - 1
        self._freq              = freq

        self._prd                = PAF_CONFIG["prd"]
        self._first_port         = PAF_CONFIG["first_port"]
        self._df_res             = PAF_CONFIG["df_res"]
        self._df_dtsz            = PAF_CONFIG["df_dtsz"]
        self._df_pktsz           = PAF_CONFIG["df_pktsz"]
        self._df_hdrsz           = PAF_CONFIG["df_hdrsz"]
        self._ncpu_numa          = PAF_CONFIG["ncpu_numa"]
        self._nchan_chk          = PAF_CONFIG["nchan_chk"]
        self._nbyte_baseband     = PAF_CONFIG["nbyte_baseband"]
        self._ndim_pol_baseband  = PAF_CONFIG["ndim_pol_baseband"]
        self._npol_samp_baseband = PAF_CONFIG["npol_samp_baseband"]

        self._nbeam               = self._pipeline_config["nbeam"]
        self._nchk_port           = self._pipeline_config["nchk_port"]
        self._nport_beam          = self._pipeline_config["nport_beam"]
        self._nchan_keep_band     = self._pipeline_config["nchan_keep_band"]
        self._rbuf_baseband_key   = self._pipeline_config["rbuf_baseband_key"]
        self._rbuf_filterbank_key = self._pipeline_config["rbuf_filterbank_key"]
        
        self._dm                      = self._general_config["dm"],
        self._pad                     = self._general_config["pad"]
        self._bind                    = self._general_config["bind"]
        self._nstream                 = self._general_config["nstream"]
        self._cufft_nx                = self._general_config["cufft_nx"]
        self._zap_chans               = self._general_config["zap_chans"]
        self._ndf_stream              = self._general_config["ndf_stream"]
        self._detect_thresh           = self._general_config["detect_thresh"]
        self._ndf_check_chk           = self._general_config["ndf_check_chk"]
        self._nchan_filterbank        = self._general_config["nchan_filterbank"]
        self._nbyte_filterbank        = self._general_config["nbyte_filterbank"]
        self._rbuf_baseband_nblk      = self._general_config["rbuf_baseband_nblk"]
        self._ndim_pol_filterbank     = self._general_config["ndim_pol_filterbank"]
        self._rbuf_baseband_nread     = self._general_config["rbuf_baseband_nread"]
        self._npol_samp_filterbank    = self._general_config["npol_samp_filterbank"]
        self._rbuf_filterbank_nblk    = self._general_config["rbuf_filterbank_nblk"]
        self._rbuf_baseband_ndf_chk   = self._general_config["rbuf_baseband_ndf_chk"]
        self._rbuf_filterbank_nread   = self._general_config["rbuf_filterbank_nread"]
        self._tbuf_baseband_ndf_chk   = self._general_config["tbuf_baseband_ndf_chk"]        
        self._rbuf_filterbank_ndf_chk = self._general_config["rbuf_filterbank_ndf_chk"]

        self._blk_res                 = self._df_res * self._rbuf_baseband_ndf_chk
        self._nchk_beam               = self._nchk_port*self._nport_beam
        self._nchan_baseband          = self._nchan_chk*self._nchk_beam
        self._ncpu_pipeline           = self._ncpu_numa/self._nbeam
        self._rbuf_baseband_blksz     = self._nchk_port*self._nport_beam*self._df_dtsz*self._rbuf_baseband_ndf_chk
        
        if self._rbuf_baseband_ndf_chk%(self._ndf_stream * self._nstream):
            self.state = "error"
            raise PipelineError("data frames in ring buffer block can only "
                                "be processed by baseband2filterbank with integer repeats")

        self._rbuf_filterbank_blksz   = int(self._nchan_filterbank * self._rbuf_baseband_blksz*
                                            self._nbyte_filterbank*self._npol_samp_filterbank*
                                            self._ndim_pol_filterbank/float(self._nbyte_baseband*                                         
                                                                            self._npol_samp_baseband*
                                                                            self._ndim_pol_baseband*
                                                                            self._nchan_baseband*
                                                                            self._cufft_nx))

        # To be safe, kill all related softwares and free shared memory
        commands = ["pkill -f capture_main",
                    "pkill -f baseband2filterbank_main",
                    "pkill -f heimdall",
                    "pkill -f dada_dbdisk",
                    "ipcrm -a"]
        execution_instances = []
        for command in commands:
            execution_instance = ExecuteCommand(command)
            execution_instances.append(execution_instance)
            try:
                execution_instance.execute()
            except Exception as e: # have to do this way as ExecuteCommand is not able to setup self.state
                self.state = "error"
                raise e
            
        # Wait until the cleanup is done
        for execution_instance in execution_instances:
            execution_instance.finish()
            
        # Create ring buffers
        execution_instances = []
        for i in range(self._nbeam):
            # Ring buffer for baseband data
            command = ("dada_db -l -p  -k {:} " 
                       "-b {:} -n {:} -r {:}").format(self._rbuf_baseband_key[i],
                                                      self._rbuf_baseband_blksz,
                                                      self._rbuf_baseband_nblk,
                                                      self._rbuf_baseband_nread)
            execution_instance = ExecuteCommand(command)
            execution_instances.append(execution_instance)            
            try:
                execution_instance.execute()
            except Exception as e: # have to do this way as ExecuteCommand is not able to setup self.state
                self.state = "error"
                raise e

            # Ring buffer for filterbank data
            command = ("dada_db -l -p  -k {:} " 
                       "-b {:} -n {:} -r {:}").format(self._rbuf_filterbank_key[i],
                                                      self._rbuf_filterbank_blksz,
                                                      self._rbuf_filterbank_nblk,
                                                      self._rbuf_filterbank_nread)
            execution_instance = ExecuteCommand(command)
            execution_instances.append(execution_instance)            
            try:
                execution_instance.execute()
            except Exception as e: # have to do this way as ExecuteCommand is not able to setup self.state
                self.state = "error"
                raise e

        # Wait until the buffer creation is done
        for execution_instance in execution_instances:
            execution_instance.finish()
        
        # Start capture
        self._runtime_directory = []
        self._socket_address = []
        self._control_socket = []
        for i in range(self._nbeam):
            # To get directory for data and socket for control 
            runtime_directory  = "{}/beam{:02}".format(DATA_ROOT, beam)
            socket_address  = "{}/beam{:02}/capture.socket".format(DATA_ROOT, beam)
            control_socket  = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            self._runtime_directory.append(runtime_directory)
            self._socket_address.append(socket_address)
            self._control_socket.append(control_socket)

            # To setup address
            destination = []
            for j in range(self._nport_beam):
                port = self._first_port + i*self._nport_beam + j
                destination.append("{};{};{}".format(self._ip, port, self._nchk_port))
                
            if EXECUTE:
                destination_alive, dead_info = self._check_connection_beam(destination)
                first_alive_ip   = destination_alive[0].split(";")[0]
                first_alive_port = int(destination_alive[0].split(";")[1])
                
                beam_index = self._acquire_beam_index(first_alive_ip, first_alive_port)
                ref_info = self._synced_refinfo(utc_start_capture, first_alive_ip, first_alive_port)
            else:
                destination_alive = []
                dead_info         = []

                beam_index = i
                ref_info = [0, 0, 0]
            
            # To setup CPU bind information and dead_info
            cpu = self._numa*self._ncpu_numa + i*self._ncpu_pipeline
            alive_info = []
            for info in destination_alive:
                alive_info.append("{};{}".format(info, cpu))
                cpu += 1
            buf_control_cpu     = self._numa*self._ncpu_numa + i*self._ncpu_pipeline + self._nport_beam
            capture_control_cpu = self._numa*self._ncpu_numa + i*self._ncpu_pipeline + self._nport_beam
            capture_control     = "1;{}".format(capture_control_cpu)
                
            self._beam.append(beam_index)
            ref_info = "{};{};{}".format(ref_info[0], ref_info[1], ref_info[2])
            
            # Execute the capture
            software = "{}/src/capture_main".format(PAF_ROOT)        
            command  = ("{} -a {} -b {} -c {} -e {} -f {} -g {} -i {} -j {} "
                        "-k {} -l {} -m {} -n {} -o {} -p {} -q {} ").format(
                            software, self._rbuf_baseband_key[i], self._df_hdrsz, " -c ".join(alive_info),
                            self._freq, ref_info, runtime_directory, buf_control_cpu, capture_control, self._bind,
                            self._rbuf_baseband_ndf_chk, self._tbuf_baseband_ndf_chk,
                            DADA_HDR_FNAME, PAF_CONFIG["instrument_name"], SOURCE_DEFAULT, self._pad)
            if EXECUTE:
                capture_execution = ExecuteCommand(command)            
                capture_execution.callbacks.add(self._decode_capture_stdout)
                self._capture_executions.append(capture_execution)            
                try:
                    capture_execution.execute()
                except Exception as e: # have to do this way as ExecuteCommand is not able to setup self.state
                    self.state = "error"
                    raise e
                
        if EXECUTE: # Ready when all capture threads and the capture control thread of all capture instances are ready
            while True:
                if self.capture_ready_counter == (self._nport_beam + 1) * self._nbeam:
                    break
                
        self.state = "ready"
        print self.state, "HERE\n"
             
    def start(self, utc_start_process, source_name, ra, dec):
        if self.state != "ready":
            raise PipelineError("Pipeline can only be started from ready state")                
        self.state = "starting"
        print self.state, "HERE\n"

        # Start baseband2filterbank and heimdall software
        for i in range(self.nbeam):
            self.baseband2filterbank_cpu = self.numa*self.ncpu_numa +\
                                           (i + 1)*self.ncpu_pipeline - 1
            self.heimdall_cpu            = self.numa*self.ncpu_numa +\
                                            (i + 1)*self.ncpu_pipeline - 1
            self.dbdisk_cpu              = self.numa*self.ncpu_numa +\
                                           (i + 1)*self.ncpu_pipeline - 1
            self.baseband2filterbank(self.rbuf_baseband_key[i],
                                     self.rbuf_filterbank_key[i],
                                     self.runtime_directory[i])
            if HEIMDALL:
                self.heimdall(self.rbuf_filterbank_key[i],
                              self.runtime_directory[i])
            if DBDISK:
                 self.dbdisk(self.rbuf_filterbank_key[i],
                             self.runtime_directory[i])
            
        # To see if we are ready to "running" or error to quit
        if EXECUTE: 
            ready = 0
            while True:
                try:
                    for i in range(self.nbeam):
                        stdout_line = self.baseband2filterbank_process[i].stdout.readline()
                        if stdout_line.find("BASEBAND2FILTERBANK_READY") != -1:
                            ready = ready + 1
                except:
                    self.state = "error"
                    raise PipelineError("baseband2filterbank_main quit unexpectedly")
                if ready == self.nbeam: # Ready to start
                    break

        # Enable the SOD of baseband ring buffer with given time and then "running"
        if EXECUTE:
            #start_buffer_idx = self.start_buf(utc_start_process, utc_start_capture)
            start_buffer_idx = 0 # Let capture software to enable sod at the last available buffer
            for i in range(self.nbeam):
                self.capture_control(self.control_socket[i],
                                     "START-OF-DATA;{};{};{};{}".format(
                                         source_name, ra, dec, start_buffer_idx),
                                     self.socket_address[i])

        # Monitor the process to see if they quit unexpectedly and also stop them with deconfigure
        if EXECUTE:
            #self.process_event = threading.Event()
            #self.process_monitor_thread = threading.Thread(target=self.process_monitor, args=(self.process_event, ))
            self.process_monitor_thread = threading.Thread(target=self.process_monitor)
            self.process_monitor_thread.start()

        self.state = "running"
        print self.state, "HERE\n"
        
    def stop(self):
        if self.state != "running":
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"
        
        if EXECUTE:
            for i in range(self.nbeam): #Stop data 
                self.capture_control(self.control_socket[i],
                                     "END-OF-DATA", self.socket_address[i])
                
            for i in range(self.nbeam): #wait process done
                returncode = None
                while returncode == None:
                    returncode = self.baseband2filterbank_process[i].returncode
                if returncode:
                    self.state = "error"
                    raise PipelineError("Failed to finish baseband2filterbank")

                if HEIMDALL:
                    returncode == None
                    while returncode == None:
                        returncode = self.heimdall_process[i].returncode
                    if returncode:
                        self.state = "error"
                        raise PipelineError("Failed to finish HEIMDALL")
                    
                if DBDISK:                    
                    returncode == None
                    while returncode == None:
                        returncode = self.dbdisk_process[i].returncode
                    if returncode:
                        self.state = "error"
                        raise PipelineError("Failed to finish DBDISK")
            self.process_monitor_thread.join()

        self.state = "ready"
        print self.state, "HERE\n"

    def deconfigure(self):
        if self.state not in ["ready", "error"]:
            raise PipelineError(
                "Pipeline can only be deconfigured from ready state")

        print self.state, "HERE\n"
        if self.state == "ready": # Normal deconfigure
            self.state = "deconfiguring"
            if EXECUTE:
                for i in range(self.nbeam): # Stop capture
                    self.capture_control(self.control_socket[i],
                                         "END-OF-CAPTURE", self.socket_address[i])                    
                self.capture_event.set()
                self.capture_traffic_monitor_thread.join()
                self.capture_process_monitor_thread.join()
                
                #for i in range(self.nbeam): # Stop capture
                #    returncode = None
                #    while returncode == None:
                #        returncode = self.capture_process[i].returncode
                #    print self.state, "HERE\n"
                #    
                #    if returncode:
                #        self.state = "error"
                #        raise PipelineError("Failed to finish CAPTURE")
                    
                for i in range(self.nbeam): # Stop capture
                    self.delete_rbuf(self.rbuf_baseband_key[i])
                    self.delete_rbuf(self.rbuf_filterbank_key[i])

                # Delete ring buffers            
                for i in range(self.nbeam * 2):
                    self.deleterbuf_process[i].communicate()                                    
                    if self.deleterbuf_process[i].returncode:
                        self.state = "error"
                        raise PipelineError("Failed to finish DELETABUF")

        else: # Force deconfigure
            self.state = "deconfiguring"
            check_call(["ipcrm", "-a"])
            self.kill_process("capture_main")
            self.kill_process("baseband2filterbank_main")
            self.kill_process("dada_dbdisk")
            self.kill_process("heimdall")
            
        self.state = "idle"

    def dbdisk(self, key, runtime_directory):
        cmd = "dada_dbdisk -b {} -k {} -D {} -o -s -z".format(
            self.dbdisk_cpu, key, runtime_directory)
        
        log.info(cmd)
        print cmd
        if EXECUTE:
            try:
                proc = Popen(shlex.split(cmd),
                             stderr=PIPE,
                             stdout=PIPE)
                self.dbdisk_process.append(proc)
            except:
                self.state = "error"
                raise PipelineError("dada_dbdisk fail")

    def baseband2filterbank(self, key_in, key_out,
                            runtime_directory):
        software = "{}/src/baseband2filterbank_main".format(PAF_ROOT)
        cmd = ("taskset -c {} nvprof {} -a {} -b {}" 
               " -c {} -d {} -e {} -f {} -i {} -j {}"
               " -k {} -l {} ").format(
                   self.baseband2filterbank_cpu, software, key_in, key_out,
                   self.rbuf_filterbank_ndf_chk, self.nstream,
                   self.ndf_stream, runtime_directory, self.nchk_beam,
                   self.cufft_nx, self.nchan_filterbank, self.nchan_keep_band)
        if SOD:
            cmd = cmd + "-g 1"
        else:
            cmd = cmd + "-g 0"
            
        log.info(cmd)
        print cmd
        if EXECUTE:
            try:
                proc = Popen(shlex.split(cmd),
                             stderr=PIPE,
                             stdout=PIPE)
                self.baseband2filterbank_process.append(proc)
            except:
                self.state = "error"
                raise PipelineError("baseband2filterbank fail")

    def heimdall(self, key, runtime_directory):
        zap = ""
        for zap_chan in self.zap_chans:
            zap += " -zap_chans {} {}".format(zap_chan[0], zap_chan[1])

        cmd = ("taskset -c {} nvprof heimdall -k {}" 
               " -dm {} {} {} -detect_thresh {} -output_dir {}").format(
                self.heimdall_cpu, key, self.dm[0][0], self.dm[0][1], zap,
                self.detect_thresh, runtime_directory)
        log.info(cmd)
        print cmd
        if EXECUTE:
            try:
                proc = Popen(shlex.split(cmd),
                             stderr=PIPE,
                             stdout=PIPE)
                self.heimdall_process.append(proc)
            except:
                self.state = "error"
                raise PipelineError("Heimdall fail")

@register_pipeline("Search2Beams")
class Search2Beams(Search):
    def __init__(self):
        super(Search2Beams, self).__init__()

    def configure(self, utc_start_capture, freq, ip):
        super(Search2Beams, self).configure(utc_start_capture, freq, ip, SEARCH_CONFIG_GENERAL, SEARCH_CONFIG_2BEAMS) 
                                            
    def start(self, utc_start_process, source_name, ra, dec):
        super(Search2Beams, self).start(utc_start_process, source_name, ra, dec)
        
    def stop(self):
        super(Search2Beams, self).stop()
        
    def deconfigure(self):
        super(Search2Beams, self).deconfigure()
        
@register_pipeline("Search1Beam")
class Search1Beam(Search):
    def __init__(self):
        super(Search1Beam, self).__init__()

    def configure(self, utc_start_capture, freq, ip):
        super(Search1Beam, self).configure(utc_start_capture, freq, ip, SEARCH_CONFIG_GENERAL, SEARCH_CONFIG_1BEAM) 
        
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
    freq              = 1340.5
    #freq              = 1337.0
    utc_start_capture = Time.now() + PAF_CONFIG["prd"]*units.s # "YYYY-MM-DDThh:mm:ss", now + stream period (27 seconds)
    utc_start_process = utc_start_capture + PAF_CONFIG["prd"]*units.s
    
    #source_name   = "UNKNOWN"
    source_name   = "DEBUG"
    ra            = "00:00:00.00"   # "HH:MM:SS.SS"
    dec           = "00:00:00.00"   # "DD:MM:SS.SS"
    host_id       = check_output("hostname").strip()[-1]
    
    parser = argparse.ArgumentParser(description='To run the pipeline for my test')
    parser.add_argument('-a', '--numa', type=int, nargs='+',
                        help='The ID of numa node')
    args     = parser.parse_args()
    numa     = args.numa[0]    
    ip       = "10.17.{}.{}".format(host_id, numa + 1)
    
    print "\nCreate pipeline ...\n"
    #search_mode = Search2Beams()
    search_mode = Search1Beam()
    print "\nConfigure it ...\n"
    search_mode.configure(utc_start_capture, freq, ip)

    print "\nStart it ...\n"
    search_mode.start(utc_start_process, source_name, ra, dec)

    time.sleep(10)
    print "\nStop it ...\n"
    search_mode.stop()

    print "\nDeconfigure it ...\n"
    search_mode.deconfigure()
