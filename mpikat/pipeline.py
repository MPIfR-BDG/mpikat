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

EXECUTE     = 0
SECDAY      = 86400.0
system_conf = {"nchan_chk":    	7,      
               "samp_rate":    	0.84375,  
               "df_dtsz":      	7168,     
               "df_pktsz":     	7232,     
               "df_hdrsz":     	64,       
               "nbyte_baseband":     2,        
               "npol_samp_baseband": 2,        
               "ndim_pol_baseband":  2,        
               "ncpu_numa":          10,       
               "ncpu_process":       5,        
               "port0":              17100,    
               "prd":                27,       
               "df_res":             1.08E-4,  
               "ndf_prd":            250000,               
}

#class Watchdog(Thread):
#    def __init__(self, name, standdown, callback):
#        Thread.__init__(self)
#        self._client = docker.from_env()
#        self._name = name
#        self._disable = standdown
#        self._callback = callback
#        self.daemon = True
#
#    def _is_dead(self, event):
#        return (event["Type"] == "container" and
#                event["Actor"]["Attributes"]["name"] == self._name and
#                event["status"] == "die")
#
#    def run(self):
#        log.debug("Setting watchdog on container '{0}'".format(self._name))
#        for event in self._client.events(decode=True):
#            if self._disable.is_set():
#                log.debug(
#                    "Watchdog standing down on container '{0}'".format(
#                        self._name))
#                break
#            elif self._is_dead(event):
#                exit_code = int(event["Actor"]["Attributes"]["exitCode"])
#                log.debug(
#                    "Watchdog activated on container '{0}'".format(
#                        self._name))
#                log.debug(
#                    "Container logs: {0}".format(
#                        self._client.api.logs(
#                            self._name)))
#                self._callback(exit_code)
#

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

class Pipeline(object):
    def __init__(self):
        self.nchan_chk    	= system_conf["nchan_chk"]    	
        self.samp_rate    	= system_conf["samp_rate"]    	
        self.df_dtsz      	= system_conf["df_dtsz"]      	
        self.df_pktsz     	= system_conf["df_pktsz"]     	
        self.df_hdrsz     	= system_conf["df_hdrsz"]     	
        self.nbyte_baseband     = system_conf["nbyte_baseband"]    
        self.npol_samp_baseband = system_conf["npol_samp_baseband"] 
        self.ndim_pol_baseband  = system_conf["ndim_pol_baseband"]  
        self.ncpu_numa          = system_conf["ncpu_numa"]          
        self.ncpu_process       = system_conf["ncpu_process"]       
        self.port0              = system_conf["port0"]              
        self.prd                = system_conf["prd"]                
        self.df_res             = system_conf["df_res"]             
        self.ndf_prd            = system_conf["ndf_prd"]            
        
    def __del__(self):
        class_name = self.__class__.__name__
        
    def configure(self):
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def deconfigure(self):
        raise NotImplementedError

    def status(self):
        raise NotImplementedError

    def acquire_beam_id(self, ip, port):
        data = bytearray(self.df_pktsz) 
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        socket.setdefaulttimeout(self.prd)  # Force to timeout after one data frame period
        server_address = (ip, port)
        sock.bind(server_address)

        nbyte, address = sock.recvfrom_into(data, self.df_pktsz)
        data_uint64 = np.fromstring(str(data), 'uint64')
        hdr_uint64  = np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[2]))[0])
        beam_id     = hdr_uint64 & np.uint64(0x000000000000ffff)

        sock.close()
        
        return beam_id
        
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
            alive[i], nchk_alive[i] = self.connection(ip, port, ndf_check_chk)
        destination_alive = []   # The destination where we can receive data
        destination_dead   = []   # The destination where we can not receive data
        for i in range(nport):
            ip = destination[i].split(":")[0]
            port = destination[i].split(":")[1]
            nchk_expect = destination[i].split(":")[2]
            nchk_actual = nchk_alive[i]
            if alive[i] == 1:
                destination_alive.append("{}:{}:{}:{}".format(ip, port, nchk_expect, nchk_actual))                                                                       
            else:
                destination_dead.append("{}:{}:{}".format(ip, port, nchk_expect))
        return destination_alive, destination_dead
    
    def connection(self, ip, port, ndf_check_chk):
        """
        To check the connection of single port
        """
        alive = 1
        nchk_alive = 0
        data = bytearray(self.df_pktsz) 
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        socket.setdefaulttimeout(self.prd)  # Force to timeout after one data frame period
        server_address = (ip, port)
        sock.bind(server_address)

        nbyte, address = sock.recvfrom_into(data, self.df_pktsz)
        data_uint64 = np.fromstring(str(data), 'uint64')
        hdr_uint64  = np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[0]))[0])
        print ((np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[1]))[0]) & np.uint64(0x00000000fc000000)) >> np.uint64(26)), ((hdr_uint64 & np.uint64(0x3fffffff00000000)) >> np.uint64(32)), (hdr_uint64 & np.uint64(0x00000000ffffffff))  
                
        try:
            nbyte, address = sock.recvfrom_into(data, self.df_pktsz)
            if (nbyte != self.df_pktsz):
                alive = 0
            else:
                source = []
                alive = 1
                for i in range(ndf_check_chk):
                    buf, address = sock.recvfrom(self.df_pktsz)
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
        utc_start = Time(utc_start, format='isot', scale='utc')
        for epoch in EPOCHS:
            if epoch < utc_start:
                break

        delta_second = utc_start.unix - epoch.unix
        sec = int(delta_second - (delta_second%self.prd))
        idf = int((delta_second%self.prd)/self.df_res)
                
        return int(epoch.unix/SECDAY), sec, idf
    
    def create_rbuf(self, key, blksz, 
                    nblk, nreader):                    
        cmd = "dada_db -l -p -k {:} -b {:} -n {:} -r {:}".format(key, blksz,
                                                                 nblk, nreader)                                                                 
        print cmd
        if EXECUTE:
            os.system(cmd)

    def remove_rbuf(self, key):
        cmd = "dada_db -d -k {:}".format(key)
        print cmd
        if EXECUTE:
            os.system(cmd)

    def capture(self, key,
                df_pktsz, df_offset,
                alive_info, dead_info,
                freq, nchan_chk, refinfo, runtime_dir,
                buf_ctrl_cpu, capture_trl, bind,
                prd, rbufin_ndf_chk, tbuf_ndf_chk, ndf_prd,
                dada_hdr_fname, instrument_name, source_info, pad):

        software = "/home/pulsar/xinping/phased-array-feed/src/capture/capture_main"
        if (len(dead_info) == 0):
            cmd = "{} -a {} -b {} -c {} -d {} -f {} -g {} -i {} -j {} -k {} -l {} -m {} -n {} -o {} -p {} -q {} -r {} -s {} -t {} -u {}".format(software, key,
                                                                                                                                                df_pktsz, df_offset,
                                                                                                                                                " -d ".join(alive_info),
                                                                                                                                                freq, nchan_chk, refinfo, runtime_dir,
                                                                                                                                                buf_ctrl_cpu, capture_trl, bind,
                                                                                                                                                prd, rbufin_ndf_chk, tbuf_ndf_chk, ndf_prd,
                                                                                                                                                dada_hdr_fname, instrument_name, source_info, pad)
        else:
            cmd = "{} -a {} -b {} -c {} -d {} -e {} -f {} -g {} -i {} -j {} -k {} -l {} -m {} -n {} -o {} -p {} -q {} -r {} -s {} -t {} -u {}".format(software, key,
                                                                                                                                                df_pktsz, df_offset,
                                                                                                                                                " -d ".join(alive_info), " -e ".join(dead_info),
                                                                                                                                                freq, nchan_chk, refinfo, runtime_dir,
                                                                                                                                                buf_ctrl_cpu, capture_trl, bind,
                                                                                                                                                prd, rbufin_ndf_chk, tbuf_ndf_chk, ndf_prd,
                                                                                                                                                dada_hdr_fname, instrument_name, source_info, pad)
            
        print cmd
        if EXECUTE:
            os.system(cmd)
 
    def diskdb(self, cpu, key,
               fname, seek_byte):
        cmd = "taskset -c {} dada_diskdb -k {} -f {} -o {} -s".format(cpu, key,
                                                                      fname, seek_byte)
        print cmd
        if EXECUTE:
            os.system(cmd)

    def baseband2filterbank(self, cpu, key_in, key_out,
                            rbufin_ndf_chk, nrepeat, nstream,
                            ndf_stream, runtime_dir):
        software = "/home/pulsar/xinping/phased-array-feed/src/baseband2filterbank/baseband2filterbank_main"
        cmd = "taskset -c {} nvprof {} -a {} -b {} -c {} -d {} -e {} -f {} -g {}".format(cpu, software, key_in, key_out,
                                                                                         rbufin_ndf_chk, nrepeat, nstream,
                                                                                         ndf_stream, runtime_dir)
        print cmd
        if EXECUTE:
            os.system(cmd)
        
    def heimdall(self, cpu, key,
                 dm, zap_chans,
                 detect_thresh, runtime_dir):
        zap = ""
        for zap_chan in zap_chans:
            zap += " -zap_chans {} {}".format(zap_chan[0], zap_chan[1])
            
        cmd = "taskset -c {} nvprof heimdall -k {} -dm {} {} {} -detect_thresh {} -output_dir {}".format(cpu, key,
                                                                                                         dm[0], dm[1], zap,
                                                                                                         detect_thresh, runtime_dir)
        print cmd
        if EXECUTE:
            os.system(cmd)

    def capture_control(self, ctrl_socket, command, socket_addr):
        cmd = "{}\n".format(command)
        if EXECUTE:
            ctrl_socket.sendto(cmd, socket_addr)

class SearchModeFileTwoProcess(Pipeline):
    """
    For now, the process part only support full bandwidth, 
    does not support partal bandwidth or simultaneous spectral output
    """
    def __init__(self):
        super(SearchModeFileTwoProcess, self).__init__()

    def configure(self, fname, ip):
        self.nprocess       = 2
        self.fname          = fname
        self.ip             = ip
        self.node           = int(ip.split(".")[2])
        self.numa           = int(ip.split(".")[3])
        
        self.nchan_baseband       = 336
        self.nchk_baseband        = 48
        self.rbuf_filterbank_key     = ["dada", "dadc"]
        self.rbuf_filterbank_ndf_chk = 10240
        self.rbuf_filterbank_nblk    = 6
        self.rbuf_filterbank_nread   = 1

        self.rbuf_filterbank_blksz   = self.nchk_baseband*self.df_dtsz*self.rbuf_filterbank_ndf_chk
        
        self.rbuf_heimdall_key     = ["dade", "dadg"]
        self.rbuf_heimdall_ndf_chk = 10240
        self.rbuf_heimdall_nblk    = 6
        self.rbuf_heimdall_nread   = 1
        self.nchan_filterbank      = 1024
        self.cufft_nx              = 64
        self.nchan_keep_band       = 16384
        self.nbyte_filterbank      = 1
        self.npol_samp_filterbank  = 1
        self.ndim_pol_filterbank   = 1

        self.ndf_stream      	 = 256
        self.nstream         	 = 2
        self.nrepeat         	 = int(self.rbuf_filterbank_ndf_chk/(self.ndf_stream * self.nstream))
        self.seek_byte       	 = 0
        
        runtime_dir = []
        for i in range(self.nprocess):
            runtime_dir.append("/beegfs/DENG/pacifix{}_numa{}_process{}".format(self.node, self.numa, i))
        self.runtime_dir         = runtime_dir
        
        self.detect_thresh   = 10
        self.dm              = [1, 1000]
        self.zap_chans       = [[512, 1023], [304, 310]]
        
        self.rbuf_heimdall_blksz   = int(self.nchan_filterbank * self.rbuf_filterbank_blksz*self.nbyte_filterbank*self.npol_samp_filterbank*self.ndim_pol_filterbank/
                                         float(self.nbyte_baseband*self.npol_samp_baseband*self.ndim_pol_baseband*self.nchan_baseband*self.cufft_nx))

        # Create ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.create_rbuf,
                                            args = (self.rbuf_filterbank_key[i], self.rbuf_filterbank_blksz,
                                                    self.rbuf_filterbank_nblk, self.rbuf_filterbank_nread, )))
            threads.append(threading.Thread(target = self.create_rbuf,
                                            args = (self.rbuf_heimdall_key[i], self.rbuf_heimdall_blksz,
                                                    self.rbuf_heimdall_nblk, self.rbuf_heimdall_nread, )))
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()
        
    def start(self):
        # Start diskdb, baseband2filterbank and heimdall software
        threads = []
        for i in range(self.nprocess):
            self.diskdb_cpu              = self.numa*self.ncpu_numa + i*self.ncpu_process
            self.baseband2filterbank_cpu = self.numa*self.ncpu_numa + i*self.ncpu_process + 1
            self.heimdall_cpu            = self.numa*self.ncpu_numa + i*self.ncpu_process + 2
            threads.append(threading.Thread(target = self.diskdb,
                                            args = (self.diskdb_cpu, self.rbuf_filterbank_key[i], self.fname, self.seek_byte, )))
            threads.append(threading.Thread(target = self.baseband2filterbank,
                                            args = (self.baseband2filterbank_cpu, self.rbuf_filterbank_key[i], self.rbuf_heimdall_key[i],
                                                    self.rbuf_filterbank_ndf_chk, self.nrepeat, self.nstream, self.ndf_stream, self.runtime_dir[i], )))
            threads.append(threading.Thread(target = self.heimdall,
                                            args = (self.heimdall_cpu, self.rbuf_heimdall_key[i], self.dm,
                                                    self.zap_chans, self.detect_thresh, self.runtime_dir[i], )))            
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()
            
    def stop(self):
        # For this mode, it will stop automatically
        pass

    def deconfigure(self):        
        # Remove ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.remove_rbuf,
                                            args = (self.rbuf_filterbank_key[i], )))
            threads.append(threading.Thread(target = self.remove_rbuf,
                                            args = (self.rbuf_heimdall_key[i], )))
            
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def status(self):
        pass
        
class SearchModeFileOneProcess(Pipeline):    
    """
    For now, the process part only support full bandwidth, 
    does not support partal bandwidth or simultaneous spectral output
    """
    def __init__(self):
        super(SearchModeFileOneProcess, self).__init__()

    def configure(self, fname, ip):
        self.fname                   = fname
        self.ip                      = ip
        self.node                    = int(ip.split(".")[2])
        self.numa                    = int(ip.split(".")[3])
        
        self.nchan_baseband          = 336
        self.nchk_baseband           = 48
        self.rbuf_filterbank_key     = "dada"
        self.rbuf_filterbank_ndf_chk = 10240
        self.rbuf_filterbank_nblk    = 6
        self.rbuf_filterbank_nread   = 1

        self.rbuf_filterbank_blksz   = self.nchk_baseband*self.df_dtsz*self.rbuf_filterbank_ndf_chk
        
        self.rbuf_heimdall_key       = "dade"
        self.rbuf_heimdall_ndf_chk   = 10240
        self.rbuf_heimdall_nblk      = 6
        self.rbuf_heimdall_nread     = 1
        self.nchan_filterbank        = 1024
        self.cufft_nx                = 64
        self.nchan_keep_band         = 16384
        self.nbyte_filterbank        = 1
        self.npol_samp_filterbank    = 1
        self.ndim_pol_filterbank     = 1

        self.ndf_stream      = 512
        self.nstream         = 2
        self.nrepeat         = int(self.rbuf_filterbank_ndf_chk/(self.ndf_stream * self.nstream))
        self.seek_byte       = 0
        self.runtime_dir     = "/beegfs/DENG/pacifix{}_numa{}_process0".format(self.node, self.numa)
        
        self.detect_thresh   = 10
        self.dm              = [1, 1000]
        self.zap_chans       = [[512, 1023], [304, 310]]
        
        self.rbuf_heimdall_blksz   = int(self.nchan_filterbank * self.rbuf_filterbank_blksz*self.nbyte_filterbank*self.npol_samp_filterbank*self.ndim_pol_filterbank/
                                         float(self.nbyte_baseband*self.npol_samp_baseband*self.ndim_pol_baseband*self.nchan_baseband*self.cufft_nx))

        # Create ring buffers
        threads = []
        threads.append(threading.Thread(target = self.create_rbuf,
                                        args = (self.rbuf_filterbank_key, self.rbuf_filterbank_blksz,
                                                self.rbuf_filterbank_nblk, self.rbuf_filterbank_nread, )))
        threads.append(threading.Thread(target = self.create_rbuf,
                                        args = (self.rbuf_heimdall_key, self.rbuf_heimdall_blksz,
                                                self.rbuf_heimdall_nblk, self.rbuf_heimdall_nread, )))
        
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        
    def start(self):
        # Start diskdb, baseband2filterbank and heimdall software
        threads = []
        self.diskdb_cpu              = self.ncpu_numa*self.numa
        self.baseband2filterbank_cpu = self.ncpu_numa*self.numa + 1
        self.heimdall_cpu            = self.ncpu_numa*self.numa + 2
        threads.append(threading.Thread(target = self.diskdb,
                                        args = (self.diskdb_cpu, self.rbuf_filterbank_key, self.fname, self.seek_byte, )))
        threads.append(threading.Thread(target = self.baseband2filterbank,
                                        args = (self.baseband2filterbank_cpu, self.rbuf_filterbank_key, self.rbuf_heimdall_key,
                                                self.rbuf_filterbank_ndf_chk, self.nrepeat, self.nstream, self.ndf_stream, self.runtime_dir, )))
        threads.append(threading.Thread(target = self.heimdall,
                                        args = (self.heimdall_cpu, self.rbuf_heimdall_key, self.dm,
                                                self.zap_chans, self.detect_thresh, self.runtime_dir, )))            

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
            
    def stop(self):
        # For this mode, it will stop automatically
        pass

    def deconfigure(self):        
        # Remove ring buffers
        threads = []
        threads.append(threading.Thread(target = self.remove_rbuf,
                                        args = (self.rbuf_filterbank_key, )))
        threads.append(threading.Thread(target = self.remove_rbuf,
                                        args = (self.rbuf_heimdall_key, )))

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def status(self):
        pass
        
class SearchModeStreamTwoProcess(Pipeline):
    def __init__(self):
        super(SearchModeStreamTwoProcess, self).__init__()  

    def configure(self, utc_start, freq, ip):
        self.utc_start     = utc_start
        self.ip            = ip
        self.node          = int(ip.split(".")[2])
        self.numa          = int(ip.split(".")[3])
        self.freq          = freq
        
        self.dada_hdr_fname = "/home/pulsar/xinping/phased-array-feed/config/header_16bit.txt"
        self.nprocess       = 2
        self.nchk_port      = 12
        self.nport_beam     = 3
        self.pad            = 1
        self.ndf_check_chk  = 1204

        self.nchan_baseband          = 336
        self.nchk_baseband           = 48
        self.rbuf_filterbank_key     = ["dada", "dadc"]
        self.rbuf_filterbank_ndf_chk = 10240
        self.rbuf_filterbank_nblk    = 6
        self.rbuf_filterbank_nread   = 1
        self.tbuf_filterbank_ndf_chk = 250

        self.rbuf_filterbank_blksz   = self.nchk_baseband*self.df_dtsz*self.rbuf_filterbank_ndf_chk
        
        self.rbuf_heimdall_key     = ["dade", "dadg"]
        self.rbuf_heimdall_ndf_chk = 10240
        self.rbuf_heimdall_nblk    = 6
        self.rbuf_heimdall_nread   = 1
        self.nchan_filterbank      = 1024
        self.cufft_nx              = 64
        self.nchan_keep_band       = 16384
        self.nbyte_filterbank      = 1
        self.npol_samp_filterbank  = 1
        self.ndim_pol_filterbank   = 1

        self.ndf_stream      	 = 256
        self.nstream         	 = 2
        self.nrepeat             = int(self.rbuf_filterbank_ndf_chk/(self.ndf_stream * self.nstream))
        self.seek_byte       	 = 0
        self.bind                = 1 
                
        self.detect_thresh   = 10
        self.dm              = [1, 1000]
        self.zap_chans       = [[512, 1023], [304, 310]]
        
        self.rbuf_heimdall_blksz   = int(self.nchan_filterbank * self.rbuf_filterbank_blksz*self.nbyte_filterbank*self.npol_samp_filterbank*self.ndim_pol_filterbank/
                                         float(self.nbyte_baseband*self.npol_samp_baseband*self.ndim_pol_baseband*self.nchan_baseband*self.cufft_nx))

        # Create ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.create_rbuf,
                                            args = (self.rbuf_filterbank_key[i], self.rbuf_filterbank_blksz,
                                                    self.rbuf_filterbank_nblk, self.rbuf_filterbank_nread, )))
            threads.append(threading.Thread(target = self.create_rbuf,
                                            args = (self.rbuf_heimdall_key[i], self.rbuf_heimdall_blksz,
                                                    self.rbuf_heimdall_nblk, self.rbuf_heimdall_nread, )))
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()

        # Start capture
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
                port = self.port0 + i*self.nport_beam + j
                destination.append("{}:{}:{}".format(self.ip, port, self.nchk_port))
            destination_alive, destination_dead = self.connections(destination, self.ndf_check_chk)
            cpu = self.numa*self.ncpu_numa + i*self.ncpu_process
            destination_alive_cpu = []
            for info in destination_alive:
                destination_alive_cpu.append("{}:{}".format(info, cpu))
                cpu += 1
            print destination_alive_cpu, destination_dead
            buf_ctrl_cpu = self.numa*self.ncpu_numa + i*self.ncpu_process + self.nport_beam
            cpt_ctrl_cpu = self.numa*self.ncpu_numa + i*self.ncpu_process + self.nport_beam
            cpt_ctrl     = "1:{}".format(cpt_ctrl_cpu)
            print cpt_ctrl

            print destination_alive[0].split(":")[0], destination_alive[0].split(":")[1]
            self.beam_id = self.acquire_beam_id(destination_alive[0].split(":")[0], int(destination_alive[0].split(":")[1]))
            runtime_dir  = "/beegfs/DENG/beam{:02}".format(self.beam_id)
            socket_addr  = "/beegfs/DENG/beam{:02}/capture.socket".format(self.beam_id)
            ctrl_socket  = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            self.runtime_dir.append(runtime_dir)
            self.socket_addr.append(socket_addr)
            self.ctrl_socket.append(ctrl_socket)
            
            threads.append(threading.Thread(target = self.capture,
                                            args = (self.rbuf_filterbank_key[i], self.df_pktsz, self.df_hdrsz,
                                                    destination_alive_cpu, destination_dead, self.freq, self.nchan_chk, refinfo,
                                                    self.runtime_dir, buf_ctrl_cpu, cpt_ctrl, self.bind, self.prd, self.rbuf_filterbank_ndf_chk,
                                                    self.tbuf_filterbank_ndf_chk, self.ndf_prd, self.dada_hdr_fname, "PAF-BMF", source_info, self.pad)))            
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()
        
    def start(self, source_name, ra, dec, start_buf):        
        # Start baseband2filterbank and heimdall software
        threads = []
        for i in range(self.nprocess):
            self.baseband2filterbank_cpu = self.numa*self.ncpu_numa + (i + 1)*self.ncpu_process - 1
            self.heimdall_cpu            = self.numa*self.ncpu_numa + (i + 1)*self.ncpu_process - 1
            threads.append(threading.Thread(target = self.baseband2filterbank,
                                            args = (self.baseband2filterbank_cpu, self.rbuf_filterbank_key[i], self.rbuf_heimdall_key[i],
                                                    self.rbuf_filterbank_ndf_chk, self.nrepeat, self.nstream, self.ndf_stream, self.runtime_dir[i], )))
            threads.append(threading.Thread(target = self.heimdall,
                                            args = (self.heimdall_cpu, self.rbuf_heimdall_key[i], self.dm,
                                                    self.zap_chans, self.detect_thresh, self.runtime_dir[i], )))
            threads.append(threading.Thread(target = self.capture_control,
                                            args = (self.ctrl_socket[i], "START-OF-DATA:{}:{}:{}:{}\n".format(source_name, ra, dec, start_buf),
                                                    self.socket_addr[i], )))
            
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()
            
    def stop(self):
        for i in range(self.nprocess): #Stop data, baseband2filterbank and heimdall will stop automatically after this
            self.capture_control(self.ctrl_socket[i], "END-OF-DATA\n", self.socket_addr[i])
            
    def deconfigure(self):
        for i in range(self.nprocess): # Stop capture
            print self.socket_addr[i]
            self.capture_control(self.ctrl_socket[i], "END-OF-CAPTURE\n", self.socket_addr[i])

        # Remove ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.remove_rbuf,
                                            args = (self.rbuf_filterbank_key[i], )))
            threads.append(threading.Thread(target = self.remove_rbuf,
                                            args = (self.rbuf_heimdall_key[i], )))
            self.ctrl_socket[i].close()
            
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def status(self):
        pass
        
class SearchModeStreamOneProcess(Pipeline):
    def __init__(self):
        super(SearchModeStreamOneProcess, self).__init__()
        
    def configure(self,  utc_start, freq, ip):
        self.utc_start     = utc_start
        self.ip            = ip
        self.node          = int(ip.split(".")[2])
        self.numa          = int(ip.split(".")[3])
        self.freq          = freq
        
        self.dada_hdr_fname = "/home/pulsar/xinping/phased-array-feed/config/header_16bit.txt"
        self.nchk_port     = 12
        self.nport_beam    = 3
        self.pad           = 1
        self.ndf_check_chk = 1204

        self.nchan_baseband          = 336
        self.nchk_baseband           = 48
        self.rbuf_filterbank_key     = "dada"
        self.rbuf_filterbank_ndf_chk = 10240
        self.rbuf_filterbank_nblk    = 6
        self.rbuf_filterbank_nread   = 1
        self.tbuf_filterbank_ndf_chk = 250

        self.rbuf_filterbank_blksz    = self.nchk_baseband*self.df_dtsz*self.rbuf_filterbank_ndf_chk
        
        self.rbuf_heimdall_key     = "dade"
        self.rbuf_heimdall_ndf_chk = 10240
        self.rbuf_heimdall_nblk    = 6
        self.rbuf_heimdall_nread   = 1
        self.nchan_filterbank      = 1024
        self.cufft_nx              = 64
        self.nchan_keep_band       = 16384
        self.nbyte_filterbank      = 1
        self.npol_samp_filterbank  = 1
        self.ndim_pol_filterbank   = 1

        self.ndf_stream      	 = 256
        self.nstream         	 = 2
        self.nrepeat             = int(self.rbuf_filterbank_ndf_chk/(self.ndf_stream * self.nstream))
        self.seek_byte       	 = 0
        self.bind                = 1 
        
        self.detect_thresh   = 10
        self.dm              = [1, 1000]
        self.zap_chans       = [[512, 1023], [304, 310]]
        
        self.rbuf_heimdall_blksz   = int(self.nchan_filterbank * self.rbuf_filterbank_blksz*
                                         self.nbyte_filterbank*self.npol_samp_filterbank*self.ndim_pol_filterbank/
                                         float(self.nbyte_baseband*self.npol_samp_baseband*
                                               self.ndim_pol_baseband*self.nchan_baseband*self.cufft_nx))

        # Create ring buffers
        threads = []
        threads.append(threading.Thread(target = self.create_rbuf,
                                        args = (self.rbuf_filterbank_key, self.rbuf_filterbank_blksz,
                                                self.rbuf_filterbank_nblk, self.rbuf_filterbank_nread, )))
        threads.append(threading.Thread(target = self.create_rbuf,
                                        args = (self.rbuf_heimdall_key, self.rbuf_heimdall_blksz,
                                                self.rbuf_heimdall_nblk, self.rbuf_heimdall_nread, )))
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()

        # Start capture
        threads = [] 
        source_info = "UNKNOW:00 00 00.00:00 00 00.00"
        refinfo = self.utc2refinfo(self.utc_start)
        refinfo = "{}:{}:{}".format(refinfo[0], refinfo[1], refinfo[2])
        destination = []
        for i in range(self.nport_beam):
            port = self.port0 + i
            destination.append("{}:{}:{}".format(self.ip, port, self.nchk_port))
        destination_alive, destination_dead = self.connections(destination, self.ndf_check_chk)
        cpu = self.numa*self.ncpu_numa
        destination_alive_cpu = []
        for info in destination_alive:
            destination_alive_cpu.append("{}:{}".format(info, cpu))
            cpu += 1
            
        self.beam_id = self.acquire_beam_id(destination_alive[0].split(":")[0], int(destination_alive[0].split(":")[1]))
        self.runtime_dir  = "/beegfs/DENG/beam{:02}".format(self.beam_id)
        self.socket_addr  = "/beegfs/DENG/beam{:02}/capture.socket".format(self.beam_id)
        self.ctrl_socket  = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        print destination_alive_cpu, destination_dead
        buf_ctrl_cpu = self.numa*self.ncpu_numa + self.nport_beam
        cpt_ctrl_cpu = self.numa*self.ncpu_numa + self.nport_beam
        cpt_ctrl     = "1:{}".format(cpt_ctrl_cpu)
        print cpt_ctrl
        threads.append(threading.Thread(target = self.capture,
                                        args = (self.rbuf_filterbank_key, self.df_pktsz, self.df_hdrsz,
                                                destination_alive_cpu, destination_dead, self.freq, self.nchan_chk, refinfo,
                                                self.runtime_dir, buf_ctrl_cpu, cpt_ctrl, self.bind, self.prd, self.rbuf_filterbank_ndf_chk,
                                                self.tbuf_filterbank_ndf_chk, self.ndf_prd, self.dada_hdr_fname, "PAF-BMF", source_info, self.pad)))            
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()
        
    def start(self, source_name, ra, dec, start_buf):        
        # Start baseband2filterbank and heimdall software
        threads = []
        self.baseband2filterbank_cpu = (self.numa + 1)*self.ncpu_numa - 1
        self.heimdall_cpu            = (self.numa + 1)*self.ncpu_numa - 1
        threads.append(threading.Thread(target = self.baseband2filterbank,
                                        args = (self.baseband2filterbank_cpu, self.rbuf_filterbank_key, self.rbuf_heimdall_key,
                                                self.rbuf_filterbank_ndf_chk, self.nrepeat, self.nstream, self.ndf_stream, self.runtime_dir, )))
        threads.append(threading.Thread(target = self.heimdall,
                                        args = (self.heimdall_cpu, self.rbuf_heimdall_key, self.dm,
                                                self.zap_chans, self.detect_thresh, self.runtime_dir, )))
        threads.append(threading.Thread(target = self.capture_control,
                                        args = (self.ctrl_socket, "START-OF-DATA:{}:{}:{}:{}\n".format(source_name, ra, dec, start_buf),
                                                self.socket_addr, )))
            
        for thread in threads:
            thread.start()            
        for thread in threads:
            thread.join()
            
    def stop(self):
        self.capture_control(self.ctrl_socket, "END-OF-DATA\n", self.socket_addr)
            
    def deconfigure(self):
        print self.socket_addr
        self.capture_control(self.ctrl_socket, "END-OF-CAPTURE\n", self.socket_addr)

        # Remove ring buffers
        threads = []
        threads.append(threading.Thread(target = self.remove_rbuf,
                                        args = (self.rbuf_filterbank_key, )))
        threads.append(threading.Thread(target = self.remove_rbuf,
                                        args = (self.rbuf_heimdall_key, )))
        self.ctrl_socket.close()
            
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def status(self):
        pass
      
if __name__ == "__main__":
    # Question, why the reference seconds is 21 seconds less than the BMF number
    # The number is random. Everytime reconfigure stream, it will change the reference seconds, sometimes it is multiple times of 27 seconds, but in most case, it is not
    # To do, find a way to sync capture of beams
    # understand why the capture does not works sometimes, or try VMA;
    freq          = 1340.5
    fname         = "/beegfs/DENG/AUG/baseband/J1819-1458/J1819-1458.dada"
    utc_start     = Time.now() + 0*units.s # Has to be "YYYY-MM-DDThh:mm:ss"
    
    #pktsz = 7232
    #prd   = 27
    #ip   = "10.17.8.1"
    #port = 17100
    #data = bytearray(pktsz) 
    #sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #socket.setdefaulttimeout(prd)  # Force to timeout after one data frame period
    #server_address = (ip, port)
    #sock.bind(server_address)
    #
    #nbyte, address = sock.recvfrom_into(data, pktsz)
    #
    #data_uint64 = np.fromstring(str(data), 'uint64')
    #hdr_uint64  = np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[0]))[0])
    #second      = ((hdr_uint64 & np.uint64(0x3fffffff00000000)) >> np.uint64(32))
    #idf         = (hdr_uint64 & np.uint64(0x00000000ffffffff))  
    #print ((np.uint64(struct.unpack("<Q", struct.pack(">Q", data_uint64[1]))[0]) & np.uint64(0x00000000fc000000)) >> np.uint64(26)), second, idf
    #print int(second/27)*27-second
    #sock.close()
        
    source_name   = "UNKNOWN"
    ra            = "00 00 00.00"
    dec           = "00 00 00.00"
    start_buf     = 0
    ip            = "10.17.8.2"

    #print "\nCreate pipeline ...\n"
    #search_mode = SearchModeFileOneProcess()
    #search_mode = SearchModeFileTwoProcess()
    #print "\nConfigure it ...\n"
    #search_mode.configure(fname, ip)
    #print "\nStart it ...\n"
    #search_mode.start()
    #print "\nStop it ...\n"
    #search_mode.stop()
    #print "\nDeconfigure it ...\n"
    #search_mode.deconfigure()

    print "\nCreate pipeline ...\n"
    #search_mode = SearchModeStreamTwoProcess()
    search_mode = SearchModeStreamOneProcess()
    print "\nConfigure it ...\n"
    
    search_mode.configure(utc_start, freq, ip)
    
    print "\nStart it ...\n"
    #search_mode.start(source_name, ra, dec, start_buf)
    print "\nStop it ...\n"
    #search_mode.stop()
    print "\nDeconfigure it ...\n"
    search_mode.deconfigure()
