#!/usr/bin/env python

import numpy as np
import csv
import math

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
                    ['0x248a07e1ac50',	'10.17.8.2']]  # This should be an input

    cfreq = 1340.5   # This should be an input
    nchan = 252      # This should be an input
    nbeam = 36       # This should be an input, expected number of beams, the actual number of beams depend on the available NiC

    nNiC  = len(destinations)
    nchk_full = 48   # Number of chunks for full beam
    baseport = 17100

    cfreq_full = [950.5, 1340.5, 1550.5]    # The number we use on TOS is round to int
    cfreq_partial = [950.5, 1340.5, 1354.5, 1550.5]    # Here only consider the center band

    if(nchan == 336 and nbeam == 36):
        print "For now, we do not support full bandwidth with 36 beams\n"
        exit()

    if(nchan == 252 and nbeam == 18):
        print "Please use 36 beams if only partial bandwidth applied\n"
        exit()

    if nchan == 336:
        if cfreq not in cfreq_full:
            print "For the full bandwidth configuration, the center frequency has to be one of them:", cfreq_full
            exit()
        nchk_offset = 0
        nchk        = nchk_full
        nport       = 6
        nchk_port   = 8
        nchk_nic    = 48

    if nchan == 252:
        if cfreq not in cfreq_partial:
            print "For the partial bandwidth configuration, the center frequency has to be one of them:", cfreq_partial
            exit()
        if cfreq in cfreq_full: # no additional offset
            nchk_offset = 6
        if cfreq == 1354.5:        # With additional offset, which is 2
            nchk_offset = 8
        nchk      = 36
        nport     = 3
        nchk_port = 12
        nchk_nic  = 72

    # Fill in default value, with which the BMF will not send any data into GPU nodes
    csvheader = 'BANDID,MAC1,IP1,PORT1,MAC2,IP2,PORT2,MAC3,IP3,PORT3,MAC4,IP4,PORT4,MAC5,IP5,PORT5,MAC6,IP6,PORT6,MAC7,IP7,PORT7,MAC8,IP8,PORT8,MAC9,IP9,PORT9,MAC10,IP10,PORT10,MAC11,IP11,PORT11,MAC12,IP12,PORT12,MAC13,IP13,PORT13,MAC14,IP14,PORT14,MAC15,IP15,PORT15,MAC16,IP16,PORT16,MAC17,IP17,PORT17,MAC18,IP18,PORT18,MAC19,IP19,PORT19,MAC20,IP20,PORT20,MAC21,IP21,PORT21,MAC22,IP22,PORT22,MAC23,IP23,PORT23,MAC24,IP24,PORT24,MAC25,IP25,PORT25,MAC26,IP26,PORT26,MAC27,IP27,PORT27,MAC28,IP28,PORT28,MAC29,IP29,PORT29,MAC30,IP30,PORT30,MAC31,IP31,PORT31,MAC32,IP32,PORT32,MAC33,IP33,PORT33,MAC34,IP34,PORT34,MAC35,IP35,PORT35,MAC36,IP36,PORT36'
    nomac  = '0x020000000000'
    noip   = '00.00.0.0'
    noport = '00000'
    rows   = nchk_full
    cols   = 109
    table  = []
    for row in range(rows):
        line=[]
        for col in range(cols):
            if(col==0):
                line.append(row)
            elif(col%3==1):
                line.append(nomac)
            elif(col%3==2):
                line.append(noip)
            else:
                line.append(noport)
        table.append(line)

    # Get actual number of beams
    if(nbeam == 36):
        nbeam_actual = 2 * nNiC
    if(nbeam == 18):
        nbeam_actual = nNiC

    # Do the real job
    for beam in range(nbeam_actual):
        beamidx = 1 + beam * 3    # Position of beam info in output file

        for sb in range(0, nchk):
            nicidx = int(math.floor((beam * nchk + sb)/(nchk_nic))) #NiC which beam is on
            nicidx = int(math.floor((beam * nchk + sb)/nchk_nic))

            print sb + nchk_offset, beamidx, nicidx

            #print beam, nchk, sb, beam * nchk + sb, nchk_nic, nicidx

            table[sb + nchk_offset][beamidx] = destinations[nicidx][0]   #MAC
            table[sb + nchk_offset][beamidx + 1] = destinations[nicidx][1] #IP

            port = baseport + math.floor(math.floor(beam * nchk + sb)%nchk_nic/nchk_port) #PORT
            table[sb + nchk_offset][beamidx+2]=str(port).split('.')[0]

    # Write out data
    fname = "stream_{:02d}beams_{:02d}chunks.csv".format(nbeam_actual, nchk)
    table_fp=open(fname,"w")
    table_fp.write(csvheader)
    table_fp.write('\n')
    for row in range(rows):
        line=",".join(map(str,table[row]))+"\n"
        table_fp.write(line)
