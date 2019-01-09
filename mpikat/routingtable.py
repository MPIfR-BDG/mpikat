#!/usr/bin/env python

import numpy as np
import csv
import math
import paramiko
from os.path import join
import time

Stream2Beams, Stream1Beam = (0,1)

BASE_PORT       = 17100
CENTER_FREQ     = [950.5, 1340.5, 1550.5]
NCHUNK_PER_BEAM = 48 # Number of frequency chunks of a beam with full bandwidth
NCHAN_PER_CHUNK = 7
        
CONFIG2BEAMS = {"nbeam":           36,  # Expected number from configuration, the real number depends on the number of alive NiCs
                "nbeam_per_nic":   2,
                "nport_per_beam":  3,
                "nchunk_per_port": 11,
}

CONFIG1BEAM  = {"nbeam":           18,  # Expected number from configuration, the real number depends on the number of alive NiCs
                "nbeam_per_nic":   1,
                "nport_per_beam":  6,
                "nchunk_per_port": 8,
}

PARAMIKO_BUFSZ     = 10240
TOSSIX_USERNAME    = "pulsar"
TOSSIX_IP          = "134.104.74.36"
TOSSIX_SCRIPT_ROOT = "/home/pulsar/aaron/askap-trunk/"

class RemoteAccess(object):
    def __init__(self):
        pass
    
    def connect(self, ip, username, bufsz, password=None):
        self.ip         = ip
        self.username   = username
        self.bufsz      = bufsz
        self.password   = password

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(hostname=ip, username=username, password=password)
        print "Successful connection {} ...".format(self.ip)

        print "Invoke shell from {}... ".format(self.ip)
        self.remote_shell = self.ssh_client.invoke_shell()

        print "Create SCP connection with {}... ".format(self.ip)
        self.sftp_client = self.ssh_client.open_sftp()        
        
    def control(self, command, sleep):
        print "Run \"{}\" on {}\n".format(command, self.ip)
        self.remote_shell.send("{}\n".format(command))
        time.sleep(sleep)
        print self.remote_shell.recv(self.bufsz)

    def scp(self, src, dst, sleep):
        print src, dst
        print "\n\n\n\n\n"
        self.sftp_client.put(src, dst)
        time.sleep(sleep)
    
    def disconnect(self):
        print "Disconnect from {} ...".format(self.ip)
        self.ssh_client.close()

    def __del__(self):
        class_name = self.__class__.__name__
        
class RoutingTable(object):
    def __init__(self):
        pass
    
    def __del__(self):
        class_name = self.__class__.__name__

    def configure(self, destinations, nbeam, nchunk, nchunk_offset, center_freq_band, directory, fname):
        """ To configure the class and check the input
        destinations: The MAC and IP of alive NiCs;
        [['0x7cfe90c0c930',	'10.17.0.1'],
         ['0x7cfe90c0cc10',	'10.17.0.2'],
         ['0x7cfe90c0ca60',	'10.17.1.1'],
         ['0x7cfe90c0cd50',	'10.17.1.2'],
         ['0x7cfe90c0cce1',	'10.17.2.1'],
         ['0x7cfe90c0cc20',	'10.17.2.2'],
         ['0x7cfe90c0cd40',	'10.17.3.1'],
         ['0x7cfe90c0cd60',	'10.17.3.2'],
         ['0x7cfe90c0cc00',	'10.17.4.1'],
         ['0x7cfe90c0cbf0',	'10.17.4.2'],
         ['0x248a07e26090',	'10.17.5.1'],
         ['0x248a07e1a330',	'10.17.5.2'],
         ['0x248a07e25e30',	'10.17.6.1'],
         ['0x248a07e25f40',	'10.17.6.2'],
         ['0x248a07e1b580',	'10.17.7.1'],
         ['0x248a07e260b0',	'10.17.7.2'],
         ['0x248a07e25a50',	'10.17.8.1'],
         ['0x248a07e1ac50',	'10.17.8.2']]

        nbeam:            the number of beams we want to use, int
        nchunk:           the number of frequency chunks of each beam, int
        nchunk_offset:    the number of frequency chunks we want to shift, int
                          + means we shift the center frequency towards the band top
        center_freq_band: the center_freq from telescope control system, which is the center frequency of the full band, float
        directory:        Where we put the table on local machine
        fname:            The name of routing table
        """

        self.destinations     = destinations
        self.nbeam            = nbeam
        self.nchunk           = nchunk
        self.nchunk_offset    = nchunk_offset
        self.center_freq_band = center_freq_band
        self.directory        = directory
        self.fname            = fname
        
        # To check the input
        if self.nbeam == 36:
            self.config = CONFIG2BEAMS
        elif self.nbeam == 18:
            self.config = CONFIG1BEAM
        else:
            raise RoutingTableError("We can only work with 18 beams or 36 beams configuration, but {} is given".format(self.nbeam))

        self.nchunk_expect = self.config["nchunk_per_port"] * self.config["nport_per_beam"]
        if self.nchunk_expect != self.nchunk:
            raise RoutingTableError("We expect {} chunks of each beam for {} beams configuration, but {} is given".format(
                self.nchunk_expect, self.nbeam, self.nchunk))
            
        if(center_freq_band not in CENTER_FREQ):
            raise RoutingTableError("Center frequency has to be in {}".format(CENTER_FREQ))
        else:
            self.center_freq_band = center_freq_band
            
        # Check the required frequency chunks
        start_chunk      = int(math.floor((NCHUNK_PER_BEAM - self.nchunk)/2.0)) # The start chunk before adding offset
        self.first_chunk = start_chunk + self.nchunk_offset
        self.last_chunk  = start_chunk + self.nchunk + self.nchunk_offset
        print self.first_chunk, self.last_chunk
        if ((self.first_chunk<0) or (self.last_chunk)>(NCHUNK_PER_BEAM - 1)):
            raise RoutingTableError("Required frequency chunks are out of range")
            
    def generate_table(self):
        """" To generate routing table """
        # Fill in default value, with which the BMF will not send any data into GPU nodes
        csvheader = 'BANDID,MAC1,IP1,PORT1,MAC2,IP2,PORT2,MAC3,IP3,PORT3,MAC4,IP4,PORT4,MAC5,IP5,PORT5,MAC6,IP6,PORT6,MAC7,IP7,PORT7,MAC8,IP8,PORT8,MAC9,IP9,PORT9,MAC10,IP10,PORT10,MAC11,IP11,PORT11,MAC12,IP12,PORT12,MAC13,IP13,PORT13,MAC14,IP14,PORT14,MAC15,IP15,PORT15,MAC16,IP16,PORT16,MAC17,IP17,PORT17,MAC18,IP18,PORT18,MAC19,IP19,PORT19,MAC20,IP20,PORT20,MAC21,IP21,PORT21,MAC22,IP22,PORT22,MAC23,IP23,PORT23,MAC24,IP24,PORT24,MAC25,IP25,PORT25,MAC26,IP26,PORT26,MAC27,IP27,PORT27,MAC28,IP28,PORT28,MAC29,IP29,PORT29,MAC30,IP30,PORT30,MAC31,IP31,PORT31,MAC32,IP32,PORT32,MAC33,IP33,PORT33,MAC34,IP34,PORT34,MAC35,IP35,PORT35,MAC36,IP36,PORT36'      
        cols   = 109
        table  = []
        for row in range(NCHUNK_PER_BEAM):
            line=[]
            for col in range(cols):
                if(col==0):
                    line.append(row)
                elif(col%3==1):
                    line.append('0x020000000000')
                elif(col%3==2):
                    line.append('00.00.0.0')
                else:
                    line.append('00000')
            table.append(line)

        # Put actual data into the table
        nbeam_actual = len(self.destinations)*self.config["nbeam_per_nic"]
        nchunk_nic   = self.nchunk_expect*self.config["nbeam_per_nic"]
        for beam in range(nbeam_actual):
            beam_idx = 1 + beam * 3    # Position of beam info in output file
        
            for sb in range(self.first_chunk, self.nchunk + self.first_chunk):
                nic_idx = int(math.floor((beam * self.nchunk + sb - self.first_chunk)/nchunk_nic))
                
                table[sb][beam_idx]     = destinations[nic_idx][0] #MAC
                table[sb][beam_idx + 1] = destinations[nic_idx][1] #IP
            
                port = BASE_PORT + int(math.floor(math.floor(beam * self.nchunk + sb)%nchunk_nic/self.config["nchunk_per_port"])) #PORT
                table[sb][beam_idx+2]=port
                print sb, beam_idx, nic_idx, port

        # Write out table
        table_fp=open(join(self.directory, self.fname), "w")
        table_fp.write(csvheader)
        table_fp.write('\n')
        for row in range(NCHUNK_PER_BEAM):
            line=",".join(map(str,table[row]))+"\n"
            table_fp.write(line)
        table_fp.close()

    def upload_table(self):
        """ To upload routing table to beamformer and configure stream"""
        tossix = RemoteAccess()
        tossix.connect(TOSSIX_IP, TOSSIX_USERNAME, PARAMIKO_BUFSZ)

        # Copy table to tossix
        tossix.scp(join(self.directory, self.fname), join("{}/Code/Components/OSL/scripts/ade/files/stream_setup".format(TOSSIX_SCRIPT_ROOT), self.fname), 1)

        # Initial the tossix
        tossix.control("bash\n", 1)
        tossix.control(". {}/initaskap.sh".format(TOSSIX_SCRIPT_ROOT), 1)
        tossix.control(". {}/Code/Components/OSL/scripts/osl_init_env.sh".format(TOSSIX_SCRIPT_ROOT), 1)
        tossix.control("cd {}/Code/Components/OSL/scripts/ade".format(TOSSIX_SCRIPT_ROOT), 1)

        # Configure metadata and streaming
        tossix.control("python osl_a_metadata_streaming.py", 10)
        tossix.control("python osl_a_abf_config_stream.py --param 'ade_bmf.stream10G.streamSetup={}'".format(self.fname), 10)

        # Disconnect
        tossix.disconnect()
        
    def center_freq_stream(self):
        """" To calculate the real center frequency of streaming data"""
        return self.center_freq_band + 0.5*(self.last_chunk + self.first_chunk - NCHUNK_PER_BEAM) * NCHAN_PER_CHUNK
    
if __name__=="__main__":
    destinations = [['0x7cfe90c0c930',	'10.17.0.1'],
                    ['0x7cfe90c0cc10',	'10.17.0.2'],
                    ['0x7cfe90c0ca60',	'10.17.1.1'],
                    ['0x7cfe90c0cd50',	'10.17.1.2'],
                    ['0x7cfe90c0cce1',	'10.17.2.1'],
                    ['0x7cfe90c0cc20',	'10.17.2.2'],
                    ['0x7cfe90c0cd40',	'10.17.3.1'],
                    ['0x7cfe90c0cd60',	'10.17.3.2'],    
                    ['0x7cfe90c0cc00',	'10.17.4.1'],
                    ['0x7cfe90c0cbf0',	'10.17.4.2'],
                    ['0x248a07e26090',	'10.17.5.1'],
                    ['0x248a07e1a330',	'10.17.5.2'],
                    ['0x248a07e25e30',	'10.17.6.1'],
                    ['0x248a07e25f40',	'10.17.6.2'],
                    ['0x248a07e1b580',	'10.17.7.1'],
                    ['0x248a07e260b0',	'10.17.7.2'],    
                    ['0x248a07e25a50',	'10.17.8.1'],
                    ['0x248a07e1ac50',	'10.17.8.2']]  

    center_freq   = 1340.5   
    nchunk        = 48      
    nbeam         = 18
    
    nchunk        = 33      
    nbeam         = 36      
    nchunk_offset = -5
    fname         = "36beams.csv"
    directory     = ""

    routing_table = RoutingTable()
    routing_table.configure(destinations, nbeam, nchunk, nchunk_offset, center_freq, directory, fname)
    routing_table.generate_table()
    print routing_table.center_freq_stream()
    routing_table.upload_table()
