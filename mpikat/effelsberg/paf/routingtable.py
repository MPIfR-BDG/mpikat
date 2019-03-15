#!/usr/bin/env python

import logging
import math
import paramiko
import ipaddress
import time
import tempfile
import socket
import numpy as np
from os.path import join
import argparse

log = logging.getLogger("mpikat.paf_routingtable")

Stream2Beams, Stream1Beam = (0, 1)

BASE_PORT = 17100
CENTER_FREQ = [950.5, 1340.5, 1550.5]
NCHUNK_PER_BEAM = 48  # Number of frequency chunks of a beam with full bandwidth
BW_PER_CHUNK = 7  # MHz

CONFIG2BEAMS = {"nbeam":           36,  # Expected number from configuration, the real number depends on the number of alive NiCs
                "nbeam_per_nic":   2,
                "nport_per_beam":  3,
                "nchunk_per_port": 11,
                }

CONFIG1BEAM = {"nbeam":           18,  # Expected number from configuration, the real number depends on the number of alive NiCs
               "nbeam_per_nic":   1,
               "nport_per_beam":  3,
               "nchunk_per_port": 16,
               }

PARAMIKO_BUFSZ = 102400
TOSSIX_USERNAME = "pulsar"
TOSSIX_IP = "134.104.74.36"
TOSSIX_SCRIPT_ROOT = "/home/pulsar/aaron/askap-trunk/"


class RoutingTableError(Exception):
    pass


class RoutingTableUploadError(RoutingTableError):
    pass


class RemoteAccess(object):

    def connect(self, ip, username, bufsz=PARAMIKO_BUFSZ, password=None):
        self.ip = ip
        self.username = username
        self.bufsz = bufsz
        self.password = password

        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(
            hostname=ip, username=username, password=password)
        log.debug("Successful connection {} ...".format(self.ip))

        log.debug("Invoke shell from {}... ".format(self.ip))
        self.remote_shell = self.ssh_client.invoke_shell()

    def control(self, cmd):
        self.remote_shell.sendall(cmd + "\r\n")
        self.remote_shell.sendall("echo 'PARAMIKO_COMMAND_DONE'" + "\r\n")
        output = ""
        while True:
            try:
                data = self.remote_shell.recv(1 << 15)
            except socket.timeout:
                continue
            output += data
            if output.find("\r\nPARAMIKO_COMMAND_DONE\r\n") != -1:
                log.debug(output)
                return
            if output.find("\r\nFAIL\r\n") != -1:
                raise RoutingTableError("Command {} fail".format(cmd))

    def scp(self, src, dst):
        log.debug("Create SCP connection with {}... ".format(self.ip))
        self.sftp_client = self.ssh_client.open_sftp()
        log.debug("Copy {} to {}".format(src, dst))
        self.sftp_client.put(src, dst)
        log.debug("Close scp channel")
        self.sftp_client.close()

    def readbeamfile(self, src):
        log.debug("Create SCP connection with {}... ".format(self.ip))
        self.sftp_client = self.ssh_client.open_sftp()
        log.debug("Reading {}".format(src))
        beamfile = self.sftp_client.open(src)
        beamlist = []
        beam_alt_d = []
        beam_az_d = []
        for line in beamfile:
            beamlist.append(np.fromstring(line, dtype=np.float32, sep=' '))
        for i in range(1, len(beamlist)):
            beam_alt_d.append(beamlist[i][0]), beam_az_d.append(beamlist[i][1])
        log.debug("Close scp channel")
        self.sftp_client.close()
        return beam_alt_d, beam_az_d

    def disconnect(self):
        log.debug("Disconnect from {} ...".format(self.ip))
        self.ssh_client.close()


def validate_destinations(destinations):
    for mac, ip in destinations:
        try:
            int(mac, base=16)
        except ValueError:
            raise RoutingTableError("Invalid mac address: {}".format(mac))
        try:
            ipaddress.ip_address(unicode(ip))
        except ValueError:
            raise RoutingTableError("Invalid IP address: {}".format(ip))


class RoutingTable(object):

    def __init__(self, destinations, nbeam, nchunk, nchunk_offset, center_freq_band):
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
        center_freq_band: the center_freq from telescope control system, which is the center frequency of the full band, float, MHz
        """
        formatted_destimations = "\n".join(
            ["IP={}, MAC={}".format(ip, mac) for mac, ip in destinations])
        log.info("Generating routing table for the following destinations:\n{}".format(
            formatted_destimations))
        validate_destinations(destinations)
        self.table_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=True)
        self.fname = self.table_file.name
        self.destinations = destinations
        self.nbeam = nbeam
        self.nchunk = nchunk
        self.nchunk_offset = nchunk_offset
        self.center_freq_band = center_freq_band
        log.debug("Routing table temporary file: {}".format(self.fname))
        log.debug(("Routing table input parameters: nbeam={}, nchunk={}, "
                   "nchunk_offset={}, center_Freq_band={}, fname={}").format(
                  self.nbeam, self.nchunk, self.nchunk_offset,
                  self.center_freq_band, self.fname))

        # To check the input
        if self.nbeam == 36:
            self.config = CONFIG2BEAMS
        elif self.nbeam == 18:
            self.config = CONFIG1BEAM
        else:
            raise RoutingTableError(
                ("We can only work with 18 beams or 36 beams configuration, "
                 "but {} is given").format(self.nbeam))
        log.info("Routing table configuation mode: {}".format(self.config))
        self.nchunk_expect = self.config[
            "nchunk_per_port"] * self.config["nport_per_beam"]
        if self.nchunk_expect != self.nchunk:
            raise RoutingTableError("We expect {} chunks of each beam for {} beams configuration, but {} is given".format(
                self.nchunk_expect, self.nbeam, self.nchunk))
        if(center_freq_band not in CENTER_FREQ):
            raise RoutingTableError(
                "Center frequency has to be in {}".format(CENTER_FREQ))
        else:
            self.center_freq_band = center_freq_band
        # Check the required frequency chunks
        # The start chunk before adding offset
        start_chunk = int(math.floor((NCHUNK_PER_BEAM - self.nchunk) / 2.0))
        self.first_chunk = start_chunk + self.nchunk_offset
        self.last_chunk = start_chunk + self.nchunk + self.nchunk_offset
        if ((self.first_chunk < 0) or (self.last_chunk) > NCHUNK_PER_BEAM):
            raise RoutingTableError(
                "Required frequency chunks are out of range")
        self.generate_table()
        log.debug("Table generated with the name {}".format(self.fname))

    def generate_table(self):
        """" To generate routing table """
        # Fill in default value, with which the BMF will not send any data into
        # GPU nodes
        csvheader = ('BANDID,MAC1,IP1,PORT1,MAC2,IP2,PORT2,MAC3,IP3,PORT3,MAC4,IP4,PORT4,MAC5,IP5,PORT5,MAC6,IP6,PORT6,'
                     'MAC7,IP7,PORT7,MAC8,IP8,PORT8,MAC9,IP9,PORT9,MAC10,IP10,PORT10,MAC11,IP11,PORT11,MAC12,IP12,PORT12,'
                     'MAC13,IP13,PORT13,MAC14,IP14,PORT14,MAC15,IP15,PORT15,MAC16,IP16,PORT16,MAC17,IP17,PORT17,MAC18,IP18,PORT18,'
                     'MAC19,IP19,PORT19,MAC20,IP20,PORT20,MAC21,IP21,PORT21,MAC22,IP22,PORT22,MAC23,IP23,PORT23,MAC24,IP24,PORT24,'
                     'MAC25,IP25,PORT25,MAC26,IP26,PORT26,MAC27,IP27,PORT27,MAC28,IP28,PORT28,MAC29,IP29,PORT29,MAC30,IP30,PORT30,'
                     'MAC31,IP31,PORT31,MAC32,IP32,PORT32,MAC33,IP33,PORT33,MAC34,IP34,PORT34,MAC35,IP35,PORT35,MAC36,IP36,PORT36')

        cols = 109
        table = []
        for row in range(NCHUNK_PER_BEAM):
            line = []
            for col in range(cols):
                if(col == 0):
                    line.append(row)
                elif(col % 3 == 1):
                    line.append('0x020000000000')
                elif(col % 3 == 2):
                    line.append('00.00.0.0')
                else:
                    line.append('00000')
            table.append(line)

        # Put actual data into the table
        nbeam_actual = len(self.destinations) * self.config["nbeam_per_nic"]
        nchunk_nic = self.nchunk_expect * self.config["nbeam_per_nic"]
        for beam in range(nbeam_actual):
            beam_idx = 1 + beam * 3    # Position of beam info in output file

            for sb in range(self.first_chunk, self.nchunk + self.first_chunk):
                nic_idx = int(math.floor(
                    (beam * self.nchunk + sb - self.first_chunk) / nchunk_nic))

                table[sb][beam_idx] = self.destinations[nic_idx][0]  # MAC
                table[sb][beam_idx + 1] = self.destinations[nic_idx][1]  # IP

                port = BASE_PORT + int(math.floor(math.floor(beam * self.nchunk + sb -
                                                             self.first_chunk) % nchunk_nic / self.config["nchunk_per_port"]))  # PORT
                table[sb][beam_idx + 2] = port
        log.debug("Completed routing table: {}".format(table))

        # Write out table
        self.table_file.seek(0)
        self.table_file.write(csvheader)
        self.table_file.write('\n')
        for row in range(NCHUNK_PER_BEAM):
            line = ",".join(map(str, table[row])) + "\n"
            self.table_file.write(line)
        self.table_file.flush()  # flush to make sure that all lines are in the file

    def upload_table(self):
        """ To upload routing table to beamformer and configure stream"""
        log.info("Uploading routing table to Tossix")
        tossix = RemoteAccess()
        log.debug("Connecting SSH session")
        tossix.connect(TOSSIX_IP, TOSSIX_USERNAME, PARAMIKO_BUFSZ)

        # Copy table to tossix
        log.debug("Secure copying routing table")
        tossix.scp(self.fname, join("{}/Code/Components/OSL/scripts/ade/files/stream_setup".format(
            TOSSIX_SCRIPT_ROOT), self.fname.split("/")[-1]))

        # Initial the tossix
        log.info("Applying routing table on Tossix")
        tossix.control("bash")
        tossix.control(". {}/initaskap.sh".format(TOSSIX_SCRIPT_ROOT))
        tossix.control(
            ". {}/Code/Components/OSL/scripts/osl_init_env.sh".format(TOSSIX_SCRIPT_ROOT))
        tossix.control(
            "cd {}/Code/Components/OSL/scripts/ade".format(TOSSIX_SCRIPT_ROOT))

        # Configure metadata and streaming
        tossix.control("python osl_a_metadata_streaming.py")
        tossix.control(
            "python osl_a_abf_config_stream.py --param 'ade_bmf.stream10G.streamSetup={}'".format(self.fname.split("/")[-1]))

        # Disconnect
        tossix.disconnect()

    def center_freq_stream(self):
        """" To calculate the real center frequency of streaming data"""
        return self.center_freq_band + 0.5 * (self.last_chunk + self.first_chunk - NCHUNK_PER_BEAM) * BW_PER_CHUNK

if __name__ == "__main__":
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

    center_freq = 1340.5
    
    parser = argparse.ArgumentParser(
        description='To run the pipeline for my test')
    parser.add_argument('-a', '--nbeam', type=int, nargs='+',
                        help='The number of beams on each GPU')
    args = parser.parse_args()
    nbeam = args.nbeam[0]
    nchunk_offset = 0
    if nbeam == 1:
        nchunk = 48
        nbeam = 18
    if nbeam == 2:
        nchunk = 33
        nbeam = 36

    routing_table = RoutingTable(
        destinations, nbeam, nchunk, nchunk_offset, center_freq)
    time.sleep(10)
    print(routing_table.center_freq_stream())
    routing_table.upload_table()
