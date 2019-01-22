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

# To do,
# 2. sensor to report status, DONE;
# 3. Why full bandwidth is worse;
# 4. why thread quit, not thread quit, it only stucks there because of the full stdout buffer;

log = logging.getLogger("mpikat.paf_pipeline")

EXECUTE        = True
#EXECUTE        = False
TRACK_STDERR   = False
SOD            = True   # To start filterbank data or not

HEIMDALL       = False   # To run heimdall on filterbank file or not
DBDISK         = True   # To run dbdisk on filterbank file or not

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
              "port0":               17100,
}

SEARCH_CONFIG_GENERAL = {"rbuf_baseband_ndf_chk":   16384,                 
                         "rbuf_baseband_nblk":      4,
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
                         
                         "ndf_stream":      	    128,
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
                       "nchan_keep_band":        16384,
                       "nbeam":                  1,
                       "nport_beam":             3,
                       "nchk_port":              16,
}

SEARCH_CONFIG_2BEAMS = {"rbuf_baseband_key":       ["dada", "dadc"],
                        "rbuf_filterbank_key":     ["dade", "dadg"],
                        "nchan_keep_band":         12288,
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

class Pipeline(object):
    def __init__(self):
        self.callbacks            = set()
        self._state               = "idle"   # Idle at the very beginning
        self.capture_process      = []
        self.createrbuf_process   = []
        self.deleterbuf_process   = []
        self.beam                 = []
        self._sensors             = []
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
        self._beam_sensor = Sensor.float(
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
            description = "The average packet loss rate",
            default = 0,
            initial_status = Sensor.NOMINAL)
        self.sensors.append(self._average_sensor)
        
    @property
    def sensors(self):
        return self._sensors
    
    def stream_status(self):
        if self.state in ["ready", "starting", "running"]:
            i = 0
            status = {"nbeam":   self.nbeam}
            for capture_process in self.capture_process:
                stdout_line = capture_process.stdout.readline()
                if "CAPTURE_STATUS" in stdout_line:
                    status = stdout_line.split()

                try:
                    status.update({"process{}".format(i):
                                   {"beam":    self.beam[i],
                                    "average":   [float(status[1]), float(status[2])],
                                    "instant":   float(status[3]),},})
                    i += 1
                except:
                    self.state = "error"
                    raise PipelineError("Stream status update fail")
                    
            return status
        else:
            self.state = "error"
            raise PipelineError("Can only run stream_status with ready, starting and running state")

    def beam_id(self, ip, port):
        """
        To get the beam ID
        """
        data = bytearray(self.df_pktsz)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        socket.setdefaulttimeout(self.prd)  # Force to timeout after one data frame period
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            beam = []
            for i in range(self.ndf_check_chk):
                nbyte, address = sock.recvfrom_into(data, self.df_pktsz)
                data_uint64 = np.fromstring(str(data), 'uint64')
                hdr_uint64  = np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[2]))[0])
                beam.append(hdr_uint64 & np.uint64(0x000000000000ffff))

            if (len(list(set(beam)))>1):
                self.state = "error"
                raise PipelineError("Beams are mixed up, please check the routing table")

            sock.close()
            return beam[0]
        except:
            sock.close()
            self.state = "error"
            raise PipelineError("Can not get beam ID")

    def synced_refinfo(self, utc_start_capture, ip, port):
        utc_start_capture = Time(utc_start_capture, format='isot', scale='utc')
        
        # Capture one packet to see what is current epoch, seconds and idf
        # We need to do that because the seconds is not always matched with estimated value
        epoch_ref, sec_ref, idf_ref = self.refinfo(ip, port)
        print epoch_ref, sec_ref, idf_ref
        
        if(utc_start_capture.unix - epoch_ref * 86400.0 - sec_ref) > PAF_CONFIG["prd"]:
            sec_ref = sec_ref + PAF_CONFIG["prd"]
        idf_ref = int((utc_start_capture.unix - epoch_ref * 86400.0 - sec_ref)/self.df_res)
        
        return epoch_ref, int(sec_ref), idf_ref

    def start_buf(self, utc_start_process, utc_start_capture):
        """
        To get the start buffer with given start time in UTC
        """
        utc_start_capture = Time(utc_start_capture, format='isot', scale='utc')
        utc_start_process = Time(utc_start_process, format='isot', scale='utc')
        
        blk_res = self.df_res * self.rbuf_baseband_ndf_chk

        start_buffer_idx = int((utc_start_process.unix - utc_start_capture.unix)/blk_res)
        return start_buffer_idx
        
    def refinfo(self, ip, port):
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
            nbyte, address = sock.recvfrom_into(data,
                                                self.df_pktsz)
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
            raise PipelineError("Can not get reference information for capture")

    def kill_process(self, process_name):
        try:
            cmd = "pkill -f {}".format(process_name)
            check_call(shlex.split(cmd))
        except:
            pass # We only kill running process

    def connections(self, destination):
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
            alive, nchk_alive = self.connection(ip, port)                                                

            if alive == 1:
                destination_alive.append(destination[i]+";{}".format(nchk_alive))
            else:
                destination_dead.append(destination[i])

        if (len(destination_alive) == 0): # No alive ports, error
            self.state = "error"
            raise PipelineError("The stream is not alive")

        return destination_alive, destination_dead

    def connection(self, ip, port):
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
            raise PipelineError("Failed to check the connection")
        return alive, nchk_alive

    def create_rbuf(self, key, blksz, nblk, nreader):                    
        cmd = ("dada_db -l -p"
               " -k {:} -b {:}" 
               " -n {:} -r {:}").format(key,
                                       blksz,
                                       nblk,
                                       nreader)
        log.info(cmd)
        print cmd
        if EXECUTE:
            try:
                proc = Popen(shlex.split(cmd),
                             stderr=PIPE,
                             stdout=PIPE)
                self.createrbuf_process.append(proc)
            except:
                self.state = "error"
                raise PipelineError("Can not create ring buffer")

    def delete_rbuf(self, key):
        cmd = "dada_db -d -k {:}".format(key)
        log.info(cmd)
        print cmd
        if EXECUTE:
            try: 
                proc = Popen(shlex.split(cmd),
                             stderr=PIPE,
                             stdout=PIPE)
                self.deleterbuf_process.append(proc)
            except:
                self.state = "error"
                raise PipelineError("Can not delete ring buffer")

    def capture(self, key, runtime_dir):                
        software = "{}/src/capture/capture_main".format(PAF_ROOT)        
        cmd = ("{} -a {} -b {} -c {} -e {} -f {}"
               " -g {} -i {} -j {} -k {} -l {}"
               " -m {} -n {} -o {} -p {} -q {}").format(
                   software, key, self.df_hdrsz,
                   " -c ".join(self.alive_info),
                   self.freq, self.ref_info, runtime_dir,
                   self.buf_ctrl_cpu, self.cpt_ctrl, self.bind,
                   self.rbuf_baseband_ndf_chk, self.tbuf_baseband_ndf_chk,
                   DADA_HDR_FNAME, PAF_CONFIG["instrument_name"],
                   SOURCE_DEFAULT, self.pad)
        if (len(self.dead_info) != 0):
            cmd = cmd + " -e {}".format(" -d ".join(self.dead_info))
            
        log.info(cmd)
        print cmd
        if EXECUTE:
            try:
                proc = Popen(shlex.split(cmd),
                             stderr=PIPE,
                             stdout=PIPE)
                self.capture_process.append(proc)
            except:
                self.state = "error"
                raise PipelineError("Capture fail")

    def capture_control(self, ctrl_socket, command, socket_addr):
        if EXECUTE:
            try:
                ctrl_socket.sendto(command, socket_addr)
            except:
                self.state = "error"
                raise PipelineError("Capture control fail")

@register_pipeline("Search")
class Search(Pipeline):
    def __init__(self):
        super(Search, self).__init__()
        
        self.heimdall_process               = []
        self.dbdisk_process                 = []
        self.baseband2filterbank_process    = []
        
        #self.capture_returncode             = []
        #self.baseband2filterbank_returncode = []
        #if HEIMDALL:
        #    self.heimdall_returncode        = []
        #if DBDISK:
        #    self.dbdisk_returncode          = []
        
    def configure(self, utc_start_capture, freq, ip, general_config, pipeline_config):
        # Kill running process if there is any and free memory
        check_call(["ipcrm", "-a"])
        self.kill_process("capture_main")
        self.kill_process("baseband2filterbank_main")
        self.kill_process("heimdall")
        self.kill_process("dada_dbdisk")
        
        if (self.state != "idle"):
            raise PipelineError(
                "Can only configure pipeline in idle state")

        self.state             = "configuring"
        self.general_config    = general_config
        self.pipeline_config   = pipeline_config
        self.ip                = ip
        self.numa              = int(ip.split(".")[3]) - 1
        self.freq              = freq

        self.prd                = PAF_CONFIG["prd"]
        self.port0              = PAF_CONFIG["port0"]
        self.df_res             = PAF_CONFIG["df_res"]
        self.df_dtsz            = PAF_CONFIG["df_dtsz"]
        self.df_pktsz           = PAF_CONFIG["df_pktsz"]
        self.df_hdrsz           = PAF_CONFIG["df_hdrsz"]
        self.ncpu_numa          = PAF_CONFIG["ncpu_numa"]
        self.nchan_chk          = PAF_CONFIG["nchan_chk"]
        self.nbyte_baseband     = PAF_CONFIG["nbyte_baseband"]
        self.ndim_pol_baseband  = PAF_CONFIG["ndim_pol_baseband"]
        self.npol_samp_baseband = PAF_CONFIG["npol_samp_baseband"]

        self.nbeam               = self.pipeline_config["nbeam"]
        self.nchk_port           = self.pipeline_config["nchk_port"]
        self.nport_beam          = self.pipeline_config["nport_beam"]
        self.nchan_keep_band     = self.pipeline_config["nchan_keep_band"]
        self.rbuf_baseband_key   = self.pipeline_config["rbuf_baseband_key"]
        self.rbuf_filterbank_key = self.pipeline_config["rbuf_filterbank_key"]
        
        self.dm                      = self.general_config["dm"],
        self.pad                     = self.general_config["pad"]
        self.bind                    = self.general_config["bind"]
        self.nstream                 = self.general_config["nstream"]
        self.cufft_nx                = self.general_config["cufft_nx"]
        self.zap_chans               = self.general_config["zap_chans"]
        self.ndf_stream              = self.general_config["ndf_stream"]
        self.detect_thresh           = self.general_config["detect_thresh"]
        self.ndf_check_chk           = self.general_config["ndf_check_chk"]
        self.nchan_filterbank        = self.general_config["nchan_filterbank"]
        self.nbyte_filterbank        = self.general_config["nbyte_filterbank"]
        self.rbuf_baseband_nblk      = self.general_config["rbuf_baseband_nblk"]
        self.ndim_pol_filterbank     = self.general_config["ndim_pol_filterbank"]
        self.rbuf_baseband_nread     = self.general_config["rbuf_baseband_nread"]
        self.npol_samp_filterbank    = self.general_config["npol_samp_filterbank"]
        self.rbuf_filterbank_nblk    = self.general_config["rbuf_filterbank_nblk"]
        self.rbuf_baseband_ndf_chk   = self.general_config["rbuf_baseband_ndf_chk"]
        self.rbuf_filterbank_nread   = self.general_config["rbuf_filterbank_nread"]
        self.tbuf_baseband_ndf_chk   = self.general_config["tbuf_baseband_ndf_chk"]        
        self.rbuf_filterbank_ndf_chk = self.general_config["rbuf_filterbank_ndf_chk"]

        self.nchk_beam               = self.nchk_port*self.nport_beam
        self.nchan_baseband          = self.nchan_chk*self.nchk_beam
        self.ncpu_pipeline           = self.ncpu_numa/self.nbeam
        self.rbuf_baseband_blksz     = self.nchk_port*self.nport_beam*self.df_dtsz*\
                                       self.rbuf_baseband_ndf_chk
        
        if self.rbuf_baseband_ndf_chk%(self.ndf_stream * self.nstream):
            self.state = "error"
            raise PipelineError("data frames in ring buffer block can not "
                                "be processed by filterbank with integer repeats")

        self.rbuf_filterbank_blksz   = int(self.nchan_filterbank * self.rbuf_baseband_blksz*
                                         self.nbyte_filterbank*self.npol_samp_filterbank*
                                         self.ndim_pol_filterbank/
                                         float(self.nbyte_baseband*
                                               self.npol_samp_baseband*
                                               self.ndim_pol_baseband*
                                               self.nchan_baseband*self.cufft_nx))

        # Create ring buffers
        for i in range(self.nbeam):
            self.create_rbuf(self.rbuf_baseband_key[i],
                             self.rbuf_baseband_blksz,
                             self.rbuf_baseband_nblk,
                             self.rbuf_baseband_nread)
            self.create_rbuf(self.rbuf_filterbank_key[i],
                             self.rbuf_filterbank_blksz,
                             self.rbuf_filterbank_nblk,
                             self.rbuf_filterbank_nread)
        if EXECUTE:
            for i in range(self.nbeam * 2):
                self.createrbuf_process[i].communicate()
                if self.createrbuf_process[i].returncode:
                    self.state = "error"
                    raise PipelineError("Failed to create ring buffer")
                
        # Start capture
        self.runtime_dir = []
        self.socket_addr = []
        self.ctrl_socket = []
        for i in range(self.nbeam):
            destination = []
            for j in range(self.nport_beam):
                port = self.port0 + i*self.nport_beam + j
                destination.append("{};{};{}".format(self.ip, port, self.nchk_port))
            if EXECUTE:
                destination_alive, self.dead_info = self.connections(destination)
            else:
                destination_alive = []
                for item in destination:
                    nchk_actual = item.split(";")[2]
                    destination_alive.append(item + ";{}".format(nchk_actual))
                self.dead_info = []
            first_alive_ip   = destination_alive[0].split(";")[0]
            first_alive_port = int(destination_alive[0].split(";")[1])
                        
            cpu = self.numa*self.ncpu_numa + i*self.ncpu_pipeline
            self.alive_info = []
            for info in destination_alive:
                self.alive_info.append("{};{}".format(info, cpu))
                cpu += 1
            self.buf_ctrl_cpu = self.numa*self.ncpu_numa + i*self.ncpu_pipeline + self.nport_beam
            cpt_ctrl_cpu = self.numa*self.ncpu_numa + i*self.ncpu_pipeline + self.nport_beam
            self.cpt_ctrl     = "1;{}".format(cpt_ctrl_cpu)

            if EXECUTE:
                beam = self.beam_id(first_alive_ip, first_alive_port)
            else:
                beam = i
            self.beam.append(beam)
            
            runtime_dir  = "{}/beam{:02}".format(DATA_ROOT, beam)
            socket_addr  = "{}/beam{:02}/capture.socket".format(DATA_ROOT, beam)
            ctrl_socket  = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            self.runtime_dir.append(runtime_dir)
            self.socket_addr.append(socket_addr)
            self.ctrl_socket.append(ctrl_socket)
            
            if EXECUTE:
                ref_info = self.synced_refinfo(utc_start_capture, first_alive_ip, first_alive_port)
            else:
                ref_info = [0, 0, 0]
            
            self.ref_info = "{};{};{}".format(ref_info[0], ref_info[1], ref_info[2])
            self.capture(self.rbuf_baseband_key[i], runtime_dir)                         

        # To see if we are ready to start or error to quit
        if EXECUTE:
            ready = 0
            while True:
                try: 
                    for i in range(self.nbeam):
                        stdout_line = self.capture_process[i].stdout.readline()
                        if stdout_line.find("CAPTURE_READY") != -1:
                            ready = ready + 1
                except:
                    self.state = "error"
                    raise PipelineError("capture_main quit unexpectedly")
                    
                if ready == self.nbeam: # Ready to start
                    self.state = "ready"
                    break

        # Update sensor values and to check if the capture process quits unexpectedly
        if EXECUTE:
            while True:
                for i in range(self.nbeam):
                    if(self.capture_process[i].returncode):
                        self.state = "error"
                        raise PipelineError("Capture fail")
                    
                    stdout_line = self.capture_process[i].stdout.readline()
                    if "CAPTURE_STATUS" in stdout_line:
                        status = stdout_line.split()
                        self._beam_sensor.set_value(float(self.beam[i]))
                        self._time_sensor.set_value(float(status[1]))
                        self._average_sensor.set_value(float(status[2]))
                        self._instant_sensor.set_value(float(status[3]))
        self.state = "ready"
        
        ## Monitor the capture process to see if it quits unexpectedly and also stop it with deconfigure
        #if EXECUTE:
        #    for i in range(self.nbeam):
        #        self.capture_process[i].communicate()
        #        returncode = self.capture_process[i].returncode
        #        #self.capture_returncode.append(returncode)
        #        if returncode:
        #            self.state = "error"
        #            raise PipelineError("Capture fail")
        #self.state = "ready"
                                
    def start(self, utc_start_process, source_name, ra, dec):
        if self.state != "ready":
            raise PipelineError(
                "Pipeline can only be started from ready state")
        self.state = "starting"

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
                                     self.runtime_dir[i])
            if HEIMDALL:
                self.heimdall(self.rbuf_filterbank_key[i],
                              self.runtime_dir[i])
            if DBDISK:
                 self.dbdisk(self.rbuf_filterbank_key[i],
                             self.runtime_dir[i])
                 
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
            start_buffer_idx = self.start_buf(utc_start_process, utc_start_capture)
            for i in range(self.nbeam):
                self.capture_control(self.ctrl_socket[i],
                                     "START-OF-DATA;{};{};{};{}".format(
                                         source_name, ra, dec, start_buffer_idx),
                                     self.socket_addr[i])
        self.state = "running"       

        # Monitor the process to see if they quit unexpectedly and also stop then with deconfigure
        if EXECUTE:
            for i in range(self.nbeam):
                self.baseband2filterbank_process[i].communicate()
                returncode = self.baseband2filterbank_process[i].returncode
                #self.baseband2filterbank_returncode.append(returncode)
                if returncode:
                    self.state = "error"
                    raise PipelineError("Capture fail")
                if HEIMDALL:                    
                    self.heimdall_process[i].communicate()
                    returncode = self.heimdall_process[i].returncode
                    #self.heimdall_returncode.append(returncode)
                    if returncode:
                        self.state = "error"
                        raise PipelineError("Capture fail")
                if DBDISK:                    
                    self.dbdisk_process[i].communicate()
                    returncode = self.dbdisk_process[i].returncode
                    #self.dbdisk_returncode.append(returncode)
                    if returncode:
                        self.state = "error"
                        raise PipelineError("Capture fail")                
            
    def stop(self):
        if self.state != "running":
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"
        if EXECUTE:
            for i in range(self.nbeam): #Stop data and wait process done
                self.capture_control(self.ctrl_socket[i],
                                     "END-OF-DATA", self.socket_addr[i])
                returncode = None
                while returncode == None:
                    returncode = self.baseband2filterbank_process[i].returncode
                if returncode:
                    self.state = "error"
                    raise PipelineError("Failed to finish capture")

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
                                                
        self.state = "ready"

    def deconfigure(self):
        if self.state not in ["ready", "error"]:
            raise PipelineError(
                "Pipeline can only be deconfigured from ready state")

        self.state = "deconfiguring"
        if self.state == "ready": # Normal deconfigure
            if EXECUTE:
                for i in range(self.nbeam): # Stop capture
                    self.capture_control(self.ctrl_socket[i],
                                         "END-OF-CAPTURE", self.socket_addr[i])
                    self.capture_process[i].communicate()
                    returncode = self.capture_process[i].returncode
                    if returncode:
                        self.state = "error"
                        raise PipelineError("Failed to finish CAPTURE")

                    self.delete_rbuf(self.rbuf_baseband_key[i])
                    self.delete_rbuf(self.rbuf_filterbank_key[i])

                # Delete ring buffers            
                for i in range(self.nbeam * 2):
                    self.deleterbuf_process[i].communicate()                                    
                    if self.deleterbuf_process[i].returncode:
                        self.state = "error"
                        raise PipelineError("Failed to finish DELETABUF")

        else: # Force deconfigure
            check_call(["ipcrm", "-a"])
            self.kill_process("capture_main")
            self.kill_process("baseband2filterbank_main")
            self.kill_process("dada_dbdisk")
            self.kill_process("heimdall")
            
        self.state = "idle"

    def dbdisk(self, key, runtime_dir):
        cmd = "dada_dbdisk -b {} -k {} -D {} -o -s -z".format(
            self.dbdisk_cpu, key, runtime_dir)
        
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
                            runtime_dir):
        software = "{}/src/baseband2filterbank/baseband2filterbank_main".format(PAF_ROOT)
        cmd = ("taskset -c {} nvprof {} -a {} -b {}" 
               " -c {} -d {} -e {} -f {} -i {} -j {}"
               " -k {} -l {} ").format(
                   self.baseband2filterbank_cpu, software, key_in, key_out,
                   self.rbuf_filterbank_ndf_chk, self.nstream,
                   self.ndf_stream, runtime_dir, self.nchk_beam,
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

    def heimdall(self, key, runtime_dir):
        zap = ""
        for zap_chan in zap_chans:
            zap += " -zap_chans {} {}".format(zap_chan[0], zap_chan[1])

            cmd = ("taskset -c {} nvprof heimdall -k {}" 
                   " -dm {} {} {} -detect_thresh {} -output_dir {}").format(
            self.heimdall_cpu, key, self.dm[0], self.dm[1], self.zap,
            self.detect_thresh, runtime_dir)
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
    #freq              = 1340.5
    freq              = 1337.0
    utc_start_capture = Time.now() + PAF_CONFIG["prd"]*units.s # "YYYY-MM-DDThh:mm:ss", now + stream period (27 seconds)
    utc_start_process = utc_start_capture + PAF_CONFIG["prd"]*units.s
    
    source_name   = "UNKNOWN"
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
    search_mode = Search2Beams()
    #search_mode = Search1Beam()
    print "\nConfigure it ...\n"
    search_mode.configure(utc_start_capture, freq, ip)

    print "\nStart it ...\n"
    search_mode.start(utc_start_process, source_name, ra, dec)
    ##search_mode.stream_status()
    #print "\nStop it ...\n"
    #search_mode.stop()
    #print "\nDeconfigure it ...\n"
    #search_mode.deconfigure()
