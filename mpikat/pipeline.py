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

EXECUTE     = 1
SECDAY      = 86400.0

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
        self.nchan_chk    	= 7
        self.samp_rate    	= 0.84375
        self.df_dtsz      	= 7168
        self.df_pktsz     	= 7232
        self.df_hdrsz     	= 64
        self.nbyte_baseband     = 2
        self.npol_samp_baseband = 2
        self.ndim_pol_baseband  = 2
        self.ncpu_numa          = 10
        self.ncpu_process       = 5
        self.port0              = 17100
        self.prd                = 27
        self.df_res             = 1.08E-4
        self.ndf_prd            = 250000
        
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
    
    def connections(self, destination, pktsz, prd, ndf_check_chk):
        """
        To check the connection of one beam with given ip and port numbers
        """
        nport = len(destination)
        alive = np.zeros(nport, dtype = int)
        nchk_alive = np.zeros(nport, dtype = int)
        
        for i in range(nport):
            ip   = destination[i].split(":")[0]
            port = int(destination[i].split(":")[1])
            alive[i], nchk_alive[i] = self.connection(ip, port, pktsz, ndf_check_chk, prd)
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
    
    def connection(self, ip, port, pktsz, ndf_check_chk, prd):
        """
        To check the connection of single port
        """
        alive = 1
        nchk_alive = 0
        data = bytearray(pktsz) 
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        socket.setdefaulttimeout(prd)  # Force to timeout after one data frame period
        server_address = (ip, port)
        sock.bind(server_address)
    
        try:
            nbyte, address = sock.recvfrom_into(data, pktsz)
            if (nbyte != pktsz):
                alive = 0
            else:
                source = []
                alive = 1
                for i in range(ndf_check_chk):
                    buf, address = sock.recvfrom(pktsz)
                    source.append(address)
                nchk_alive = len(set(source))
        except:
            alive = 0        

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
        sec = int(delta_second - (delta_second%self.prd)) + 21
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

    def configure(self, fname, node, numa):
        self.nprocess       = 2
        self.fname          = fname
        
        self.nchan_baseband       = 336
        self.nchk_baseband        = 48
        self.rbuf_filterbank_key     = ["dada", "dadc"]
        self.rbuf_filterbank_ndf_chk = 10240
        self.rbuf_filterbank_nblk    = 6
        self.rbuf_filterbank_nread   = 1

        self.rbuf_filterbank_blksz    = self.nchk_baseband*self.df_dtsz*self.rbuf_filterbank_ndf_chk
        
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

        #self.ndf_stream      	 = 128
        self.ndf_stream      	 = 256
        self.nstream         	 = 2
        self.nrepeat         	 = int(self.rbuf_filterbank_ndf_chk/(self.ndf_stream * self.nstream))
        self.seek_byte       	 = 0
        self.node            	 = node
        self.numa            	 = numa
        
        runtime_dir = []
        for i in range(self.nprocess):
            runtime_dir.append("/beegfs/DENG/pacifix{}_numa{}_process{}".format(node, numa, i))
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

    def configure(self, fname, node, numa):
        self.fname                   = fname
        
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
        self.node            = node
        self.numa            = numa
        self.runtime_dir         = "/beegfs/DENG/pacifix{}_numa{}_process0".format(self.node, self.numa)
        
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

    def configure(self, utc_start, freq, node, numa):
        self.utc_start     = utc_start
        self.ip            = "10.17.{}.{}".format(node, numa + 1)
        self.freq          = freq

        self.dada_hdr_fname = "/home/pulsar/xinping/phased-array-feed/config/header_16bit.txt"
        self.nprocess      = 2
        self.nchk_port     = 12
        self.nport_beam    = 3
        self.pad           = 1
        self.ndf_check_chk = 1204

        self.nchan_baseband          = 336
        self.nchk_baseband           = 48
        self.rbuf_filterbank_key     = ["dada", "dadc"]
        self.rbuf_filterbank_ndf_chk = 10240
        self.rbuf_filterbank_nblk    = 6
        self.rbuf_filterbank_nread   = 1
        self.tbuf_filterbank_ndf_chk = 250

        self.rbuf_filterbank_blksz    = self.nchk_baseband*self.df_dtsz*self.rbuf_filterbank_ndf_chk
        
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
        self.node            	 = node
        self.numa            	 = numa
        self.bind                = 1 
                
        runtime_dir = []
        socket_addr = []
        ctrl_socket = []
        
        for i in range(self.nprocess):
            runtime_dir.append("/beegfs/DENG/pacifix{}_numa{}_process{}".format(node, numa, i))
            socket_addr.append("/beegfs/DENG/pacifix{}_numa{}_process{}/capture.socket".format(node, numa, i))
            ctrl_socket.append(socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM))
            
        self.runtime_dir         = runtime_dir
        self.socket_addr         = socket_addr
        self.ctrl_socket         = ctrl_socket
        
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
        threads = [] 
        source_info = "UNKNOW:00 00 00.00:00 00 00.00"
        refinfo = self.utc2refinfo(self.utc_start)
        refinfo = "{}:{}:{}".format(refinfo[0], refinfo[1], refinfo[2])
        for i in range(self.nprocess):
            destination = []
            for j in range(self.nport_beam):
                port = self.port0 + i*self.nport_beam + j
                destination.append("{}:{}:{}".format(self.ip, port, self.nchk_port))
            destination_alive, destination_dead = self.connections(destination, self.df_pktsz, self.prd, self.ndf_check_chk)
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
            threads.append(threading.Thread(target = self.capture,
                                            args = (self.rbuf_filterbank_key[i], self.df_pktsz, self.df_hdrsz,
                                                    destination_alive_cpu, destination_dead, self.freq, self.nchan_chk, refinfo,
                                                    self.runtime_dir[i], buf_ctrl_cpu, cpt_ctrl, self.bind, self.prd, self.rbuf_filterbank_ndf_chk,
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
        
    def configure(self,  utc_start, freq, node, numa):
        self.utc_start     = utc_start
        self.ip            = "10.17.{}.{}".format(node, numa + 1)
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
        self.node            	 = node
        self.numa            	 = numa
        self.bind                = 1 
                
        self.runtime_dir         = "/beegfs/DENG/pacifix{}_numa{}_process0".format(node, numa)
        self.socket_addr         = "/beegfs/DENG/pacifix{}_numa{}_process0/capture.socket".format(node, numa)
        self.ctrl_socket         = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        
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

        # Start capture
        threads = [] 
        source_info = "UNKNOW:00 00 00.00:00 00 00.00"
        refinfo = self.utc2refinfo(self.utc_start)
        refinfo = "{}:{}:{}".format(refinfo[0], refinfo[1], refinfo[2])
        destination = []
        for i in range(self.nport_beam):
            port = self.port0 + i
            destination.append("{}:{}:{}".format(self.ip, port, self.nchk_port))
        destination_alive, destination_dead = self.connections(destination, self.df_pktsz, self.prd, self.ndf_check_chk)
        cpu = self.numa*self.ncpu_numa
        destination_alive_cpu = []
        for info in destination_alive:
            destination_alive_cpu.append("{}:{}".format(info, cpu))
            cpu += 1
            
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
    freq          = 1340.5
    fname         = "/beegfs/DENG/AUG/baseband/J1819-1458/J1819-1458.dada"
    utc_start     = Time.now() + 0*units.s # Has to be "YYYY-MM-DDThh:mm:ss"
    source_name   = "UNKNOWN"
    ra            = "00 00 00.00"
    dec           = "00 00 00.00"
    start_buf     = 0
    node          = 8
    numa          = 0

    #print "\nCreate pipeline ...\n"
    #search_mode = SearchModeFileOneProcess()
    #search_mode = SearchModeFileTwoProcess()
    #print "\nConfigure it ...\n"
    #search_mode.configure(fname, node, numa)    
    #print "\nStart it ...\n"
    #search_mode.start()
    #print "\nStop it ...\n"
    #search_mode.stop()
    #print "\nDeconfigure it ...\n"
    #search_mode.deconfigure()

    print "\nCreate pipeline ...\n"
    search_mode = SearchModeStreamTwoProcess()
    search_mode = SearchModeStreamOneProcess()
    print "\nConfigure it ...\n"
    search_mode.configure(utc_start, freq, node, numa)    
    print "\nStart it ...\n"
    search_mode.start(source_name, ra, dec, start_buf)
    print "\nStop it ...\n"
    search_mode.stop()
    print "\nDeconfigure it ...\n"
    search_mode.deconfigure()
