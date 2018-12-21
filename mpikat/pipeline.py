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
        os.system(cmd)
  
class SearchModeFileTwoProcess(Pipeline):
    """
    For now, the process part only support full bandwidth, does not support partal bandwidth or simultaneous spectral output
    """
    def __init__(self):
        super(SearchModeFileTwoProcess, self).__init__()

    def configure(self, fname):
        self.nprocess = 2
        self.fname    = fname
        
        self.nchan_in       = 336
        self.nchk_in        = 48
        self.rbufin_key     = ["aaaa", "baaa"]
        self.rbufin_ndf_chk = 10240
        self.rbufin_nblk    = 8
        self.rbufin_nread   = 1

        self.rbufin_blksz    = self.nchk_in * self.df_dtsz * self.rbufin_ndf_chk
        self.create_rbuf(self.rbufin_key[0], self.rbufin_blksz, self.rbufin_nblk, self.rbufin_nread)
        
        self.rbufout_key     = ["caaaa", "daaa"]
        self.rbufout_ndf_chk = 10240
        self.rbufout_nblk    = 8
        self.rbufout_nread   = 1
        self.nchan_out       = 1024
        self.cufft_nx        = 64
        self.nchan_keep_band = 16384
        self.nbyte_out       = 1
        self.npol_samp_out   = 1
        self.ndim_pol_out    = 1        
        
    def start(self):
        pass

    def stop(self):
        pass

    def deconfigure(self):
        pass

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
    
    search_mode = SearchModeFileTwoProcess()
    search_mode.configure(fname)
