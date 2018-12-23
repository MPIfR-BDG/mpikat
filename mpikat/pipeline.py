#!/usr/bin/env python

import ConfigParser
import threading
import json
import os
#from astropy.time import Time

SECDAY      = 86400.0
MJD1970     = 40587.0

class Pipeline(object):
    def __init__(self):
        self.nchan_chk    = 7
        self.samp_rate    = 0.84375
        self.epoch        = "2000-01-01T00:00:00"
        self.df_dtsz      = 7168
        self.nbyte_in     = 2
        self.npol_samp_in = 2
        self.ndim_pol_in  = 2

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

    def create_rbuf(self, key, blksz, nblk, nreader):
        cmd = "dada_db -l -p -k {:} -b {:} -n {:} -r {:}".format(key, blksz, nblk, nreader)
        print cmd
        #os.system(cmd)

    def remove_rbuf(self, key):
        cmd = "dada_db -d -k {:}".format(key)
        print cmd
        #os.system(cmd)

    def capture(self):
        #cmd  = ""
        #os.system()
        pass
    
    def diskdb(self, key, fname, seek_byte):
        cmd = "dada_diskdb -k {} -f {} -o {} -s".format(key, fname, seek_byte)
        print cmd
        #os.system(cmd)

    def baseband2filterbank(self, key_in, key_out, rbufin_ndf_chk, nrepeat, nstream, ndf_stream, out_dir):
        cmd = "/home/pulsar/xinping/phased-array-feed/src/baseband2filterbank/baseband2filterbank_main -a {} -b {} -c {} -d {} -e {} -f {} -g {}".format(key_in, key_out, rbufin_ndf_chk, nrepeat, nstream, ndf_stream, out_dir)
        print cmd
        #os.system(cmd)
        
    def heimdall(self, key, out_dir, dm, zap_chans, detect_thresh):
        zap = ""
        for zap_chan in zap_chans:
            zap += " -zap_chans {} {}".format(zap_chan[0], zap_chan[1])
        cmd       = "heimdall -k {} -output_dir {} -dm {} {} {} -detect_thresh {}".format(key, out_dir, dm[0], dm[1], zap, detect_thresh)
        print cmd
        #os.system(cmd)
        
class SearchModeFileTwoProcess(Pipeline):
    """
    For now, the process part only support full bandwidth, does not support partal bandwidth or simultaneous spectral output
    """
    def __init__(self):
        super(SearchModeFileTwoProcess, self).__init__()

    def configure(self, fname, node):
        self.nprocess       = 2
        self.fname          = fname
        
        self.nchan_in       = 336
        self.nchk_in        = 48
        self.rbuf_filterbank_key     = ["dada", "dadc"]
        self.rbuf_filterbank_ndf_chk = 10240
        self.rbuf_filterbank_nblk    = 8
        self.rbuf_filterbank_nread   = 1

        self.rbuf_filterbank_blksz    = self.nchk_in*self.df_dtsz*self.rbuf_filterbank_ndf_chk
        
        self.rbuf_heimdall_key     = ["dade", "dadg"]
        self.rbuf_heimdall_ndf_chk = 10240
        self.rbuf_heimdall_nblk    = 6
        self.rbuf_heimdall_nread   = 1
        self.nchan_out       = 1024
        self.cufft_nx        = 64
        self.nchan_keep_band = 16384
        self.nbyte_out       = 1
        self.npol_samp_out   = 1
        self.ndim_pol_out    = 1

        self.ndf_stream      = 512
        self.nstream         = 2
        self.nrepeat         = 10
        self.seek_byte       = 0
        self.node            = node

        out_dir = []
        for i in range(self.nprocess):
            out_dir.append("/beegfs/DENG/pacifix{}_process{}".format(node, i))
        self.out_dir         = out_dir
        
        self.detect_thresh   = 10
        self.dm              = [1, 1000]
        self.zap_chans       = [[512, 1023], [304, 310]]
        
        self.rbuf_heimdall_blksz   = int(self.nchan_out * self.rbuf_filterbank_blksz*self.nbyte_out*self.npol_samp_out*self.ndim_pol_out/float(self.nbyte_in*self.npol_samp_in*self.ndim_pol_in*self.nchan_in*self.cufft_nx))

        # Create ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.create_rbuf, args = (self.rbuf_filterbank_key[i], self.rbuf_filterbank_blksz, self.rbuf_filterbank_nblk, self.rbuf_filterbank_nread, )))
            threads.append(threading.Thread(target = self.create_rbuf, args = (self.rbuf_heimdall_key[i], self.rbuf_heimdall_blksz, self.rbuf_heimdall_nblk, self.rbuf_heimdall_nread, )))

        for thread in threads:
            thread.start()
            
        for thread in threads:
            thread.join()
        
    def start(self):
        # Start diskdb software
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.diskdb, args = (self.rbuf_filterbank_key[i], self.fname, self.seek_byte, )))
            threads.append(threading.Thread(target = self.baseband2filterbank, args = (self.rbuf_filterbank_key[i], self.rbuf_heimdall_key[i], self.rbuf_filterbank_ndf_chk, self.nrepeat, self.nstream, self.ndf_stream, self.out_dir[i], )))
            threads.append(threading.Thread(target = self.heimdall, args = (self.rbuf_heimdall_key[i], self.out_dir[i], self.dm, self.zap_chans, self.detect_thresh, )))            

        for thread in threads:
            thread.start()
            
        for thread in threads:
            thread.join()
            
        # Start process software

        # Start Heimdall
        pass

    def stop(self):
        # Stop automatically
        pass

    def deconfigure(self):        
        # Remove ring buffers
        threads = []
        for i in range(self.nprocess):
            threads.append(threading.Thread(target = self.remove_rbuf, args = (self.rbuf_filterbank_key[i], )))
            threads.append(threading.Thread(target = self.remove_rbuf, args = (self.rbuf_heimdall_key[i], )))
            
        for thread in threads:
            thread.start()
            
        for thread in threads:
            thread.join()

    def status(self):
        pass
        
class SearchModeFileOneProcess(Pipeline):
    def __init__(self):
        super(SearchModeFileOneProcess, self).__init__()
    def configure(self, nchan, fname):
        self.nprocess      = 1
        self.fname         = fname

    def configure(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def deconfigure(self):
        pass

    def status(self):
        pass
        
class SearchModeStreamTwoProcess(Pipeline):
    def __init__(self):
        super(SearchModeStreamTwoProcess, self).__init__()  

    def configure(self, utc_start, ip, nchan, freq):
        self.nprocess      = 2
        self.utc_start     = utc_start
        self.ip            = ip
        self.freq          = freq
    
    def start(self):
        pass

    def stop(self):
        pass

    def deconfigure(self):
        pass

    def status(self):
        pass
        
class SearchModeStreamOneProcess(Pipeline):
    def __init__(self):
        super(SearchModeStreamOneProcess, self).__init__()
        
    def configure(self, utc_start, ip, nchan, freq):
        self.nprocess      = 1
        self.utc_start     = utc_start
        self.ip            = ip
        self.freq          = freq
    
    def start(self):
        pass

    def stop(self):
        pass

    def deconfigure(self):
        pass

    def status(self):
        pass
      
if __name__ == "__main__":
    system_conf   = "system-control.conf"
    pipeline_conf = "pipeline-control.conf"
    mode          = "filterbank"
    nbeam         = 18
    freq          = 1340.5
    ip            = "10.17.8.1"
    fname         = "/beegfs/DENG/AUG/baseband/J1819-1458/J1819-1458.dada"
    utc_start     = "2018-08-30T19:37:27"
    node          = 0

    print "Create pipeline ...\n"
    search_mode = SearchModeFileTwoProcess()
    print "Configure it ...\n"
    search_mode.configure(fname, node)
    print "Start it ..."
    search_mode.start()
    print "Stop it ...\n"
    search_mode.stop()
    print "Deconfigure it ...\n"
    search_mode.deconfigure()
