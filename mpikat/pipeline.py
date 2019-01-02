#!/usr/bin/env python

import ConfigParser
import threading
import json
import os
from astropy.time import Time
import astropy.units as units
import multiprocessing
import numpy as np
import socket
import struct
from subprocess import check_output

# To do,
# 1. Dict for more parameters, DONE;
# 2. To check the state before each operation, DONE; 
# 3. To capture the state of each running porcess and determine it is "error" or not, DO NOT NEED TO DO IT;
# 4. To pass packet loss rate to controller;
# 5. Capture expection and pass to controller, DONE;

# https://stackoverflow.com/questions/16768290/understanding-popen-communicate

EXECUTE     = 0
SYSTEM_CONF = {"nchan_chk":    	     7,      
               "samp_rate":    	     0.84375,  
               "prd":                27,       
               "df_res":             1.08E-4,  
               "ndf_prd":            250000,
               
               "df_dtsz":      	     7168,     
               "df_pktsz":     	     7232,     
               "df_hdrsz":     	     64,
               
               "nbyte_baseband":     2,        
               "npol_samp_baseband": 2,        
               "ndim_pol_baseband":  2,        

               "ncpu_numa":          10,       
               "port0":              17100,                               
}

SEARCH_CONF = {"nchan_baseband":          336,
               "nchk_baseband":           48,
               
               "rbuf_filterbank_key":     ["dada", "dadc"],
               "rbuf_filterbank_ndf_chk": 10240,
               "rbuf_filterbank_nblk":    6,
               "rbuf_filterbank_nread":   1,
               
               "rbuf_heimdall_key":       ["dade", "dadg"],
               "rbuf_heimdall_ndf_chk":   10240,
               "rbuf_heimdall_nblk":      6,
               "rbuf_heimdall_nread":     1,
               
               "nchan_filterbank":        1024,
               "cufft_nx":                64,
               "nchan_keep_band":         16384,
               
               "nbyte_filterbank":        1,
               "npol_samp_filterbank":    1,
               "ndim_pol_filterbank":     1,
               
               "ndf_stream":      	  256,
               "nstream":                 2,
               
               "seek_byte":               0,
               "bind":                    1,
               
               "pad":                     1,
               "ndf_check_chk":           1204,
               "tbuf_filterbank_ndf_chk": 250,
               
               "detect_thresh":           10,
               "dm":                      [1, 1000],
               "zap_chans":               [[512, 1023], [304, 310]],
}

PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "error"]

# Epoch of BMF timing system, it updates every 0.5 year, [UTC datetime, EPOCH]
EPOCHS = [
    Time("2025-07-01T00:00:00", format='isot', scale='utc'),
    Time("2025-01-01T00:00:00", format='isot', scale='utc'),
    Time("2024-07-01T00:00:00", format='isot', scale='utc'),
    Time("2024-01-01T00:00:00", format='isot', scale='utc'),
    Time("2023-07-01T00:00:00", format='isot', scale='utc'),
    Time("2023-01-01T00:00:00", format='isot', scale='utc'),
    Time("2022-07-01T00:00:00", format='isot', scale='utc'),
    Time("2022-01-01T00:00:00", format='isot', scale='utc'),
    Time("2021-07-01T00:00:00", format='isot', scale='utc'),
    Time("2021-01-01T00:00:00", format='isot', scale='utc'),
    Time("2020-07-01T00:00:00", format='isot', scale='utc'),
    Time("2020-01-01T00:00:00", format='isot', scale='utc'),
    Time("2019-07-01T00:00:00", format='isot', scale='utc'),
    Time("2019-01-01T00:00:00", format='isot', scale='utc'),
    Time("2018-07-01T00:00:00", format='isot', scale='utc'),
    Time("2018-01-01T00:00:00", format='isot', scale='utc'),
]

class PipelineError(Exception):
    pass

class Pipeline(object):
    def __init__(self):
        os.system("ipcrm -a") # Remove shared memory at the very beginning (if there is any)
        
        self.callbacks = set()
        self._state = "idle"   # Idle at the very beginning

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

    def acquire_beam_id(self, ip, port):
        """
        To get the beam ID 
        """
        df_pktsz = SYSTEM_CONF["df_pktsz"]
        prd      = SYSTEM_CONF["prd"]
        
        data = bytearray(df_pktsz) 
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        socket.setdefaulttimeout(prd)  # Force to timeout after one data frame period
        server_address = (ip, port)
        sock.bind(server_address)

        try:
            nbyte, address = sock.recvfrom_into(data, df_pktsz)
            data_uint64 = np.fromstring(str(data), 'uint64')
            hdr_uint64  = np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[2]))[0])
            beam_id     = hdr_uint64 & np.uint64(0x000000000000ffff)
            sock.close()
            
            return beam_id
        except:
            sock.close()
            self.state = "error"

    def kill_process(self, process_name):
        try:
            pids = check_output(["pidof", process_name]).split()
            for pid in pids:
                os.system("kill {}".format(pid))            
        except:
            pass # We only kill running process
        
    def connections(self, destination, ndf_check_chk):
        """
        To check the connection of one beam with given ip and port numbers
        """
        nport = len(destination)
        alive = np.zeros(nport, dtype = int)
        nchk_alive = np.zeros(nport, dtype = int)
        
        for i in range(nport):
            ip   = destination[i].split(":")[0]
            port = int(destination[i].split(":")[1])
            alive[i], nchk_alive[i] = self.connection(
                ip, port, ndf_check_chk)
        destination_alive = []   # The destination where we can receive data
        destination_dead   = []   # The destination where we can not receive data
        for i in range(nport):
            ip = destination[i].split(":")[0]
            port = destination[i].split(":")[1]
            nchk_expect = destination[i].split(":")[2]
            nchk_actual = nchk_alive[i]
            if alive[i] == 1:
                destination_alive.append("{}:{}:{}:{}".format(
                    ip, port, nchk_expect, nchk_actual))                                                                       
            else:
                destination_dead.append("{}:{}:{}".format(
                    ip, port, nchk_expect))
        if (len(destination_alive) == 0): # No alive ports, error
            self.state = "error"
            
        return destination_alive, destination_dead
    
    def connection(self, ip, port, ndf_check_chk):
        """
        To check the connection of single port
        """
        df_pktsz = SYSTEM_CONF["df_pktsz"]
        prd      = SYSTEM_CONF["prd"]
        
        alive = 1
        nchk_alive = 0
        data = bytearray(df_pktsz) 
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        socket.setdefaulttimeout(prd)  # Force to timeout after one data frame period
        server_address = (ip, port)
        sock.bind(server_address)

        nbyte, address = sock.recvfrom_into(data, df_pktsz)
        data_uint64 = np.fromstring(str(data), 'uint64')
        hdr_uint64  = np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[0]))[0])
        try:
            nbyte, address = sock.recvfrom_into(data, df_pktsz)
            if (nbyte != df_pktsz):
                alive = 0
            else:
                source = []
                alive = 1
                for i in range(ndf_check_chk):
                    buf, address = sock.recvfrom(df_pktsz)
                    source.append(address)
                nchk_alive = len(set(source))
        except:
            alive = 0        

        sock.close()
        return alive, nchk_alive
    
    def utc2refinfo(self, utc_start):
        """
        To convert input start time in UTC to the reference information for capture
        utc_start should follow this format "YYYY-MM-DDThh:mm:ss"
        """
        prd    = SYSTEM_CONF["prd"]
        df_res = SYSTEM_CONF["df_res"]
        
        utc_start = Time(utc_start, format='isot', scale='utc')
        for epoch in EPOCHS:
            if epoch < utc_start:
                break

        delta_second = utc_start.unix - epoch.unix
        sec = int(delta_second - (delta_second%prd))
        idf = int((delta_second%prd)/df_res)
                
        return int(epoch.unix/86400.0), sec, idf
    
    def create_rbuf(self, key, blksz, 
                    nblk, nreader):
        cmd = "dada_db -l -p -k {:} -b {:} \
        -n {:} -r {:}".format(key, blksz, nblk, nreader)
        
        if EXECUTE:
            try:
                check_output(cmd, shell = True)
            except:
                self.state = "error"

    def remove_rbuf(self, key):
        cmd = "dada_db -d -k {:}".format(key)
        print cmd
        if EXECUTE:
            try:
                check_output(cmd, shell = True)
            except:
                self.state = "error"

    def capture(self, key,
                alive_info, dead_info,
                freq, refinfo, runtime_dir,
                buf_ctrl_cpu, capture_trl, bind,
                rbufin_ndf_chk, tbuf_ndf_chk, 
                dada_hdr_fname, instrument_name,
                source_info, pad):
        df_pktsz  = SYSTEM_CONF["df_pktsz"]
        df_hdrsz  = SYSTEM_CONF["df_hdrsz"]
        nchan_chk = SYSTEM_CONF["nchan_chk"]
        prd       = SYSTEM_CONF["prd"]
        ndf_prd   = SYSTEM_CONF["ndf_prd"]
        
        software = "/home/pulsar/xinping/"\
                   "phased-array-feed/src/"\
                   "capture/capture_main"
        print software
        if (len(dead_info) == 0):
            cmd = "{} -a {} -b {} -c {} -d {} -f {} -g {} -i {} -j {} -k {} -l {} \
            -m {} -n {} -o {} -p {} -q {} -r {} -s {} -t {} -u {}".format(
                software, key, df_pktsz, df_hdrsz,                                                                                                                                                 
                " -d ".join(alive_info),
                freq, nchan_chk, refinfo, runtime_dir,
                buf_ctrl_cpu, capture_trl, bind,
                prd, rbufin_ndf_chk, tbuf_ndf_chk, ndf_prd,
                dada_hdr_fname, instrument_name, source_info, pad)
        else:
            cmd = "{} -a {} -b {} -c {} -d {} -e {} -f {} -g {} -i {} -j {} -k {} -l {} \
            -m {} -n {} -o {} -p {} -q {} -r {} -s {} -t {} -u {}".format(            
                software, key, df_pktsz, df_hdrsz, 
                " -d ".join(alive_info), " -e ".join(dead_info),
                freq, nchan_chk, refinfo, runtime_dir,
                buf_ctrl_cpu, capture_trl, bind,
                prd, rbufin_ndf_chk, tbuf_ndf_chk, ndf_prd,
                dada_hdr_fname, instrument_name, source_info, pad)

        print cmd
        if EXECUTE:
            try:
                check_output(cmd, shell = True)
            except:
                self.state = "error"
 
    def diskdb(self, cpu, key,
               fname, seek_byte):
        cmd = "taskset -c {} dada_diskdb -k {} -f {} -o {} -s".format(
            cpu, key,fname, seek_byte)
                                                                      
        print cmd
        if EXECUTE:           
            try:
                check_output(cmd, shell = True)
            except:
                self.state = "error"

    def baseband2filterbank(self, cpu, key_in, key_out,
                            rbufin_ndf_chk, nrepeat, nstream,
                            ndf_stream, runtime_dir):
        software = "/home/pulsar/xinping/"\
                   "phased-array-feed/src/"\
                   "baseband2filterbank/"\
                   "baseband2filterbank_main"
        cmd = "taskset -c {} nvprof {} -a {} -b {} \
        -c {} -d {} -e {} -f {} -g {}".format(                 
            cpu, software, key_in, key_out,
            rbufin_ndf_chk, nrepeat, nstream,
            ndf_stream, runtime_dir)
        print cmd
        if EXECUTE:
            try:
                check_output(cmd, shell = True)
            except:
                self.state = "error"
        
    def heimdall(self, cpu, key,
                 dm, zap_chans,
                 detect_thresh, runtime_dir):
        zap = ""
        for zap_chan in zap_chans:
            zap += " -zap_chans {} {}".format(zap_chan[0], zap_chan[1])
            
        cmd = "taskset -c {} nvprof heimdall -k {} -dm {} {} {} \
        -detect_thresh {} -output_dir {}".format( 
            cpu, key,dm[0], dm[1], zap,
            detect_thresh, runtime_dir)
        print cmd
        if EXECUTE:            
            try:
                check_output(cmd, shell = True)
            except:
                self.state = "error"

    def capture_control(self, ctrl_socket, command, socket_addr):
        cmd = "{}\n".format(command)
        cmd = "ctrl_socket.sendto({}\n, {})".format(
            command, socket_addr)
        
        if EXECUTE:            
            try:
                check_output(cmd, shell = True)
            except:
                self.state = "error"

                
class SearchWithFile(Pipeline):
    """
    For now, the process part only support full bandwidth, 
    does not support partal bandwidth or simultaneous spectral output
    """
    def __init__(self):
        super(SearchWithFile, self).__init__()

    def configure(self, fname, ip, pipeline_conf, nprocess, ncpu_pipeline):
        if (self.state != "idle"):
            raise PipelineError(
                "Can only configure pipeline in idle state")
        
        self.state          = "configuring"
        self.pipeline_conf  = pipeline_conf
        self.nprocess       = nprocess
        self.ncpu_pipeline  = ncpu_pipeline
        self.fname          = fname
        self.ip             = ip
        self.node           = int(ip.split(".")[2])
        self.numa           = int(ip.split(".")[3]) - 1        

        self.rbuf_filterbank_blksz   = self.pipeline_conf["nchk_baseband"]*\
                                       SYSTEM_CONF["df_dtsz"]*\
                                       self.pipeline_conf["rbuf_filterbank_ndf_chk"]
        
        self.nrepeat         	 = int(self.pipeline_conf["rbuf_filterbank_ndf_chk"]/
                                       (self.pipeline_conf["ndf_stream"] * self.pipeline_conf["nstream"]))

        # Kill running process if there is any
        self.kill_process("dada_diskdb")
        self.kill_process("baseband2filterbank_main")
        self.kill_process("heimdall")
        
        runtime_dir = []
        for i in range(self.nprocess):
            runtime_dir.append("/beegfs/DENG/pacifix{}_numa{}_process{}".format(
                self.node, self.numa, i))
        self.runtime_dir         = runtime_dir
        
        self.rbuf_heimdall_blksz   = int(self.pipeline_conf["nchan_filterbank"] * self.rbuf_filterbank_blksz*
                                         self.pipeline_conf["nbyte_filterbank"]*self.pipeline_conf["npol_samp_filterbank"]*self.pipeline_conf["ndim_pol_filterbank"]/
                                         float(SYSTEM_CONF["nbyte_baseband"]*SYSTEM_CONF["npol_samp_baseband"]*
                                               SYSTEM_CONF["ndim_pol_baseband"]*self.pipeline_conf["nchan_baseband"]*self.pipeline_conf["cufft_nx"]))

        # Create ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.create_rbuf,
                                            args = (self.pipeline_conf["rbuf_filterbank_key"][i],
                                                    self.rbuf_filterbank_blksz,
                                                    self.pipeline_conf["rbuf_filterbank_nblk"],
                                                    self.pipeline_conf["rbuf_filterbank_nread"], )))
            threads.append(threading.Thread(target = self.create_rbuf,
                                            args = (self.pipeline_conf["rbuf_heimdall_key"][i],
                                                    self.rbuf_heimdall_blksz,
                                                    self.pipeline_conf["rbuf_heimdall_nblk"],
                                                    self.pipeline_conf["rbuf_heimdall_nread"], )))
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()
        self.state = "ready"
        
    def start(self):
        if self.state != "ready":
            raise PipelineError(
                "Pipeline can only be started from ready state")
        
        self.state = "starting"
        # Start diskdb, baseband2filterbank and heimdall software
        ncpu_numa     = SYSTEM_CONF["ncpu_numa"]
        threads = []
        for i in range(self.nprocess):
            self.diskdb_cpu              = self.numa*ncpu_numa + i*self.ncpu_pipeline
            self.baseband2filterbank_cpu = self.numa*ncpu_numa + i*self.ncpu_pipeline + 1
            self.heimdall_cpu            = self.numa*ncpu_numa + i*self.ncpu_pipeline + 2
            threads.append(threading.Thread(target = self.diskdb,
                                            args = (self.diskdb_cpu,
                                                    self.pipeline_conf["rbuf_filterbank_key"][i],
                                                    self.fname, self.pipeline_conf["seek_byte"], )))
            threads.append(threading.Thread(target = self.baseband2filterbank,
                                            args = (self.baseband2filterbank_cpu,
                                                    self.pipeline_conf["rbuf_filterbank_key"][i],
                                                    self.pipeline_conf["rbuf_heimdall_key"][i],
                                                    self.pipeline_conf["rbuf_filterbank_ndf_chk"],
                                                    self.nrepeat, self.pipeline_conf["nstream"],
                                                    self.pipeline_conf["ndf_stream"], self.runtime_dir[i], )))
                                                    
            threads.append(threading.Thread(target = self.heimdall,
                                            args = (self.heimdall_cpu,
                                                    self.pipeline_conf["rbuf_heimdall_key"][i], self.pipeline_conf["dm"], 
                                                    self.pipeline_conf["zap_chans"], self.pipeline_conf["detect_thresh"],
                                                    self.runtime_dir[i], )))            
        for thread in threads:
            thread.start()
        self.state = "running"
        for thread in threads:
            thread.join()
            
    def stop(self):
        if self.state != "running":
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"
        # For this mode, it will stop automatically
        self.state = "ready"

    def deconfigure(self):
        if self.state != "ready":
            raise PipelineError(
                "Pipeline can only be deconfigured from ready state")
        
        self.state = "deconfiguring"
        # Remove ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.remove_rbuf,
                                            args = (self.pipeline_conf["rbuf_filterbank_key"][i], )))
            threads.append(threading.Thread(target = self.remove_rbuf,
                                            args = (self.pipeline_conf["rbuf_heimdall_key"][i], )))
            
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.state = "idle"

class SearchWithFileTwoProcess(SearchWithFile):
    def __init__(self):
        super(SearchWithFileTwoProcess, self).__init__()

    def configure(self, fname, ip):
        pipeline_conf = SEARCH_CONF
        nprocess      = 2
        ncpu_pipeline = 5
        try:
            super(SearchWithFileTwoProcess, self).configure(fname, ip, pipeline_conf, nprocess, ncpu_pipeline)
        except:
            raise PipelineError(
                "Can only configure pipeline in idle state")
        
    def start(self):
        try:
            super(SearchWithFileTwoProcess, self).start()
        except:
            raise PipelineError(
                "Pipeline can only be started from ready state")
        
    def stop(self):
        try:
            super(SearchWithFileTwoProcess, self).stop()
        except:
            raise PipelineError("Can only stop a running pipeline")
            
    def deconfigure(self):
        try:
            super(SearchWithFileTwoProcess, self).deconfigure()
        except:
            raise PipelineError(
                "Pipeline can only be deconfigured from ready state")

            
class SearchWithFileOneProcess(SearchWithFile):
    def __init__(self):
        super(SearchWithFileOneProcess, self).__init__()

    def configure(self, fname, ip):
        pipeline_conf = SEARCH_CONF
        nprocess      = 1
        ncpu_pipeline = 10
        try:
            super(SearchWithFileOneProcess, self).configure(fname, ip, pipeline_conf, nprocess, ncpu_pipeline)
        except:
            raise PipelineError(
                "Can only configure pipeline in idle state")

    def start(self):
        try:
            super(SearchWithFileOneProcess, self).start()
        except:
            raise PipelineError(
                "Pipeline can only be started from ready state")
            
    def stop(self):
        try:
            super(SearchWithFileOneProcess, self).stop()
        except:
            raise PipelineError("Can only stop a running pipeline")
            
    def deconfigure(self):
        try:
            super(SearchWithFileOneProcess, self).deconfigure()
        except:
            raise PipelineError(
                "Pipeline can only be deconfigured from ready state")
        
class SearchWithStream(Pipeline):
    def __init__(self):
        super(SearchWithStream, self).__init__()  
        
    def configure(self, utc_start, freq, ip, pipeline_conf, nprocess, nchk_port, nport_beam, ncpu_pipeline):
        if (self.state != "idle"):
            raise PipelineError(
                "Can only configure pipeline in idle state")

        self.state         = "configuring"
        self.pipeline_conf = pipeline_conf
        self.ncpu_pipeline = ncpu_pipeline
        
        self.utc_start     = utc_start
        self.ip            = ip
        self.node          = int(ip.split(".")[2])
        self.numa          = int(ip.split(".")[3]) - 1
        self.freq          = freq
        
        self.dada_hdr_fname = "/home/pulsar/xinping/"\
                              "phased-array-feed/config/"\
                              "header_16bit.txt"
        self.nprocess       = nprocess
        self.nchk_port      = nchk_port
        self.nport_beam     = nport_beam

        self.rbuf_filterbank_blksz   = self.pipeline_conf["nchk_baseband"]*SYSTEM_CONF["df_dtsz"]*\
                                       self.pipeline_conf["rbuf_filterbank_ndf_chk"]                                       

        self.nrepeat             = int(self.pipeline_conf["rbuf_filterbank_ndf_chk"]/
                                       (self.pipeline_conf["ndf_stream"] * self.pipeline_conf["nstream"]))
        
        # Kill running process if there is any
        self.kill_process("capture_main")
        self.kill_process("baseband2filterbank_main")
        self.kill_process("heimdall")
        
        self.rbuf_heimdall_blksz   = int(self.pipeline_conf["nchan_filterbank"] * self.rbuf_filterbank_blksz*
                                         self.pipeline_conf["nbyte_filterbank"]*self.pipeline_conf["npol_samp_filterbank"]*
                                         self.pipeline_conf["ndim_pol_filterbank"]/
                                         float(SYSTEM_CONF["nbyte_baseband"]*
                                               SYSTEM_CONF["npol_samp_baseband"]
                                               *SYSTEM_CONF["ndim_pol_baseband"]*
                                               self.pipeline_conf["nchan_baseband"]*self.pipeline_conf["cufft_nx"]))

        # Create ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.create_rbuf,
                                            args = (self.pipeline_conf["rbuf_filterbank_key"][i],
                                                    self.rbuf_filterbank_blksz,
                                                    self.pipeline_conf["rbuf_filterbank_nblk"],
                                                    self.pipeline_conf["rbuf_filterbank_nread"], )))
            threads.append(threading.Thread(target = self.create_rbuf,
                                            args = (self.pipeline_conf["rbuf_heimdall_key"][i],
                                                    self.rbuf_heimdall_blksz,
                                                    self.pipeline_conf["rbuf_heimdall_nblk"],
                                                    self.pipeline_conf["rbuf_heimdall_nread"], )))
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()
        
        # Start capture
        port0        = SYSTEM_CONF["port0"]
        ncpu_numa    = SYSTEM_CONF["ncpu_numa"]
        self.runtime_dir = []
        self.socket_addr = []
        self.ctrl_socket = []
        threads = [] 
        source_info = "UNKNOW:00 00 00.00:00 00 00.00"
        refinfo = self.utc2refinfo(self.utc_start)
        refinfo = "{}:{}:{}".format(refinfo[0], refinfo[1], refinfo[2])
        for i in range(self.nprocess):
            destination = []
            for j in range(self.nport_beam):
                port = port0 + i*self.nport_beam + j
                destination.append("{}:{}:{}".format(self.ip, port, self.nchk_port))
            destination_alive, destination_dead = self.connections(destination,
                                                                   self.pipeline_conf["ndf_check_chk"])
            cpu = self.numa*ncpu_numa + i*self.ncpu_pipeline
            destination_alive_cpu = []
            for info in destination_alive:
                destination_alive_cpu.append("{}:{}".format(info, cpu))
                cpu += 1
            print destination_alive_cpu, destination_dead
            buf_ctrl_cpu = self.numa*ncpu_numa + i*self.ncpu_pipeline + self.nport_beam
            cpt_ctrl_cpu = self.numa*ncpu_numa + i*self.ncpu_pipeline + self.nport_beam
            cpt_ctrl     = "1:{}".format(cpt_ctrl_cpu)
            print cpt_ctrl

            print destination_alive[0].split(":")[0], destination_alive[0].split(":")[1]
            self.beam_id = self.acquire_beam_id(destination_alive[0].split(":")[0],
                                                int(destination_alive[0].split(":")[1]))
            runtime_dir  = "/beegfs/DENG/beam{:02}".format(self.beam_id)
            socket_addr  = "/beegfs/DENG/beam{:02}/capture.socket".format(self.beam_id)
            ctrl_socket  = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            self.runtime_dir.append(runtime_dir)
            self.socket_addr.append(socket_addr)
            self.ctrl_socket.append(ctrl_socket)
            
            threads.append(threading.Thread(target = self.capture,
                                            args = (self.pipeline_conf["rbuf_filterbank_key"][i],
                                                    destination_alive_cpu,
                                                    destination_dead, self.freq,
                                                    refinfo, self.runtime_dir, 
                                                    buf_ctrl_cpu, cpt_ctrl, self.pipeline_conf["bind"],
                                                    self.pipeline_conf["rbuf_filterbank_ndf_chk"],
                                                    self.pipeline_conf["tbuf_filterbank_ndf_chk"],
                                                    self.dada_hdr_fname, "PAF-BMF",
                                                    source_info, self.pipeline_conf["pad"])))            
        for thread in threads:
            thread.start()
        self.state = "ready"
        for thread in threads:
            thread.join()
        
    def start(self, source_name, ra, dec, start_buf):
        if self.state != "ready":
            raise PipelineError(
                "Pipeline can only be started from ready state")
        self.state = "starting"
        
        # Start baseband2filterbank and heimdall software
        ncpu_numa    = SYSTEM_CONF["ncpu_numa"]
        threads = []
        for i in range(self.nprocess):
            self.baseband2filterbank_cpu = self.numa*ncpu_numa +\
                                           (i + 1)*self.ncpu_pipeline - 1
            self.heimdall_cpu            = self.numa*ncpu_numa +\
                                            (i + 1)*self.ncpu_pipeline - 1
            threads.append(threading.Thread(target = self.baseband2filterbank,
                                            args = (self.baseband2filterbank_cpu,
                                                    self.pipeline_conf["rbuf_filterbank_key"][i],
                                                    self.pipeline_conf["rbuf_heimdall_key"][i],
                                                    self.pipeline_conf["rbuf_filterbank_ndf_chk"],
                                                    self.nrepeat, self.pipeline_conf["nstream"],
                                                    self.pipeline_conf["ndf_stream"], self.runtime_dir[i], )))
            threads.append(threading.Thread(target = self.heimdall,
                                            args = (self.heimdall_cpu,
                                                    self.pipeline_conf["rbuf_heimdall_key"][i],
                                                    self.pipeline_conf["dm"], 
                                                    self.pipeline_conf["zap_chans"], 
                                                    self.pipeline_conf["detect_thresh"],
                                                    self.runtime_dir[i], )))
            threads.append(threading.Thread(target = self.capture_control,
                                            args = (self.ctrl_socket[i],
                                                    "START-OF-DATA:{}:{}:{}:{}\n".format(
                                                        source_name, ra, dec, start_buf),
                                                    self.socket_addr[i], )))
            
        for thread in threads:
            thread.start()
        self.state = "running"
        for thread in threads:
            thread.join()
            
    def stop(self):
        if self.state != "running":
            raise PipelineError("Can only stop a running pipeline")
        self.state = "stopping"
        for i in range(self.nprocess): #Stop data, 
            self.capture_control(self.ctrl_socket[i],
                                 "END-OF-DATA\n", self.socket_addr[i])
        self.state = "ready"
        
    def deconfigure(self):
        if self.state != "ready":
            raise PipelineError(
                "Pipeline can only be deconfigured from ready state")
        
        self.state = "deconfiguring"
        for i in range(self.nprocess): # Stop capture
            print self.socket_addr[i]
            self.capture_control(self.ctrl_socket[i],
                                 "END-OF-CAPTURE\n", self.socket_addr[i])

        # Remove ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.remove_rbuf,
                                            args = (self.pipeline_conf["rbuf_filterbank_key"][i], )))
            threads.append(threading.Thread(target = self.remove_rbuf,
                                            args = (self.pipeline_conf["rbuf_heimdall_key"][i], )))
            self.ctrl_socket[i].close()
            
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.state = "idle"

class SearchWithStreamTwoProcess(SearchWithStream):
    def __init__(self):
        super(SearchWithStreamTwoProcess, self).__init__()

    def configure(self, utc_start, freq, ip):
        pipeline_conf = SEARCH_CONF
        nprocess      = 2
        nchk_port     = 12
        nport_beam    = 3
        ncpu_pipeline = 5
        try:
            super(SearchWithStreamTwoProcess, self).configure(utc_start, freq, ip, pipeline_conf, nprocess, nchk_port, nport_beam, ncpu_pipeline)
        except:
            raise PipelineError(
                "Can only configure pipeline in idle state")
        
    def start(self, source_name, ra, dec, start_buf):
        try:
            super(SearchWithStreamTwoProcess, self).start(source_name, ra, dec, start_buf)
        except:
            raise PipelineError(
                "Pipeline can only be started from ready state")
        
    def stop(self):
        try:
            super(SearchWithStreamTwoProcess, self).stop()
        except:
            raise PipelineError("Can only stop a running pipeline")
        
    def deconfigure(self):
        try:
            super(SearchWithStreamTwoProcess, self).deconfigure()
        except:
            raise PipelineError(
                "Pipeline can only be deconfigured from ready state")
        
class SearchWithStreamOneProcess(SearchWithStream):
    def __init__(self):
        super(SearchWithStreamOneProcess, self).__init__()

    def configure(self, utc_start, freq, ip):
        pipeline_conf = SEARCH_CONF
        nprocess      = 1
        nchk_port     = 8
        nport_beam    = 6
        ncpu_pipeline = 10
        try:
            super(SearchWithStreamOneProcess, self).configure(utc_start, freq, ip, pipeline_conf, nprocess, nchk_port, nport_beam, ncpu_pipeline)
        except:
            raise PipelineError(
                "Can only configure pipeline in idle state")

    def start(self, source_name, ra, dec, start_buf):
        try:
            super(SearchWithStreamOneProcess, self).start(source_name, ra, dec, start_buf)
        except:
            raise PipelineError(
                "Pipeline can only be started from ready state")
        
    def stop(self):
        try:
            super(SearchWithStreamOneProcess, self).stop()
        except:
            raise PipelineError("Can only stop a running pipeline")
        
    def deconfigure(self):
        try:
            super(SearchWithStreamOneProcess, self).deconfigure()
        except:
            raise PipelineError(
                "Pipeline can only be deconfigured from ready state")
    
if __name__ == "__main__":
    # Question, why the reference seconds is 21 seconds less than the BMF number
    # The number is random. Everytime reconfigure stream, it will change the reference seconds, sometimes it is multiple times of 27 seconds, but in most case, it is not
    # To do, find a way to sync capture of beams
    # understand why the capture does not works sometimes, or try VMA;
    freq          = 1340.5
    fname         = "/beegfs/DENG/AUG/baseband/"\
                    "J1819-1458/J1819-1458.dada"
    utc_start     = Time.now() + 0*units.s # Has to be "YYYY-MM-DDThh:mm:ss"
    
    source_name   = "UNKNOWN"
    ra            = "00 00 00.00"
    dec           = "00 00 00.00"
    start_buf     = 0
    ip            = "10.17.8.2"

    #print "\nCreate pipeline ...\n"
    #search_mode = SearchWithFileOneProcess()
    #search_mode = SearchWithFileTwoProcess()
    #print "\nConfigure it ...\n"
    #search_mode.configure(fname, ip)
    #print "\nStart it ...\n"
    #search_mode.start()
    #print "\nStop it ...\n"
    #search_mode.stop()
    #print "\nDeconfigure it ...\n"
    #search_mode.deconfigure()

    print "\nCreate pipeline ...\n"
    #search_mode = SearchWithStreamTwoProcess()
    search_mode = SearchWithStreamOneProcess()
    print "\nConfigure it ...\n"
    
    search_mode.configure(utc_start, freq, ip)
    
    print "\nStart it ...\n"
    search_mode.start(source_name, ra, dec, start_buf)

    print "\nStop it ...\n"
    search_mode.stop()
    
    print "\nDeconfigure it ...\n"
    search_mode.deconfigure()
