#!/usr/bin/env python

class pipelines(object):
    
    def ConfigSectionMap(fname, section):
        # Play with configuration file
        Config = ConfigParser.ConfigParser()
        Config.read(fname)
    
        dict_conf = {}
        options = Config.options(section)
        for option in options:
            try:
                dict_conf[option] = Config.get(section, option)
                if dict_conf[option] == -1:
                    DebugPrint("skip: %s" % option)
            except:
                print("exception on %s!" % option)
                dict_conf[option] = None
        return dict_conf

    def __init__(self,
                 mode,
                 nprocesses,
                 ip, # Figure out with table
                 ports, # Figure out with table
                 
                 ip_fits, # Hard code
                 port_fits, # Hard code
                 log_dir,  # Figure out with table
                 #cpus,           
                 #gpu, 
                 nchan, # from Json
                 nchan_out, # decide inside, depend on mode;
                 integrate_time, # From Json
                 frequency,      # from Json or TOS
                 start_time):

        # Setup inside the class;
        #system_conf,
        #key_capture,
        #key_process1,
        #key_process2,
        #nblk_capture,
        #nblk_capture_tmp,
        #nblk_process1,
        #nblk_process2,
        #nreader_capture,
        #nreader_process1,
        #nreader_process2,
        #ndf_capture,
        #ndf_process1,
        #ndf_process2,
        
        self.system_conf   = system_conf    # Where we put the configuration of PAF system
        self.nprocesses    = nprocesses     # Number of processes in each Docker
        self.ip_capture    = ip_capture     # IP address the capture will listen to
        self.ports_capture = ports_capture  # Ports the capture will listen to
        self.ip_fits       = ip_fits        # The IP address of FITSwriter interface
        self.port_fits     = port_fits      # The port of FITSwriter interface
        self.cpus          = cpus
        self.gpu           = gpu

        self.key_capture   = key_capture    # The key for capture ring buffer
        self.key_process1  = key_process1   # The first key for process ring buffer, the ring buffer for filterbank and spectral data
        self.key_process2  = key_process2   # The second key for process ring buffer, which is the ring buffer for FITSwriter only for search mode
        
        self.nblk_capture  = nblk_capture   # Number of buffer blocks 
        self.nblk_process1 = nblk_process1   
        self.nblk_process2 = nblk_process2
        
        self.nreader_capture  = nreader_capture  # Number of buffer readers
        self.nreader_process1 = nreader_process1   
        self.nreader_process2 = nreader_process2

        self.ndf_capture  = ndf_capture     # This will determine the size of ring buffer block in bytes
        self.ndf_process1 = ndf_process1   
        self.ndf_process2 = ndf_process2

        self.nchan          = nchan
        self.nchan_out      = nchan_out
        self.integrate_time = integrate_time
        
        self.frequency     = frequency      # The center frequency, the real center frequency or the center frequency of the band?
        self.start_time    = start_time     # UTC string, integer seconds, to sync the capture of different beams;
        self.mode          = mode           # The mode we will use, search, spectral and fold
        self.log_dir       = log_dir        # Where to put log files
        
        self.df_res        = self.ConfigSectionMap(system_conf, "EthernetInterfaceBMF")['df_res']  # To get information from system.conf file
        
    def configure(self):
        # Create ring buffer for capture and launch the capture software with sod disabled at the beginning
        # Data should comes to ring buffer, but no processing happen
        
        if self.mode == "search":
            # Create ring buffer for filterbank output (process1) and for FITSwriter data (process2)
            return True
        if self.mode == "spectral" or self.mode == "fold":
            # Create ring buffer for spectral/baseband output (process1)
            return True
        
        return True

    def start(self):              
        if self.mode == "search":
            # Run the baseband2filterbank, heimdall and spectral2udp
            # Enable sod of ring buffer
            # Should start to process
            return True
        if self.mode == "spectral":
            # Run the baseband2spectral and spectral2udp
            # Enable sod of ring buffer
            # Should start to process
            return True
        if self.mode == "fold":
            # Run the baseband2baseband
            # Enable sod of ring buffer
            # Should start to process
            return True
        
        return True

    def stop(self):                  
        if self.mode == "search":
            # Disable sod of ring buffer
            # Stop the baseband2filterbank, heimdall and spectral2udp
            return True
        if self.mode == "spectral":
            # Disable sod of ring buffer
            # stop the baseband2spectral and spectral2udp
            return True
        if self.mode == "fold":
            # Disable sod of ring buffer
            # stop the baseband2baseband and dspsr
            return True
        
        return True

    def deconfigure(self):
        # Disable sod of ring buffer
        # Stop the capture
        # delete capture ring buffer
        
        if self.mode == "search":
            # Delete ring buffer for filterbank output (process1) and for FITSwriter data (process2)
            return True
        if self.mode == "spectral" or self.mode == "fold":
            # Delete ring buffer for spectral/baseband output (process1)
            return True
        
        return True
