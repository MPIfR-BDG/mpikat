import os
import binascii
from datetime import datetime
import jinja2

DADA_HEADER = """HEADER       DADA                # Distributed aquisition and data analysis
HDR_VERSION  1.0                 # Version of this ASCII header
HDR_SIZE     4096                # Size of the header in bytes

DADA_VERSION 1.0                 # Version of the DADA Software
PIC_VERSION  1.0                 # Version of the PIC FPGA Software

# DADA parameters
OBS_ID       {{obs_id}}          # observation ID
PRIMARY      unset               # primary node host name
SECONDARY    unset               # secondary node host name
FILE_NAME    unset               # full path of the data file

FILE_SIZE    {{filesize}}          # requested size of data files
FILE_NUMBER  0                   # number of data file

# time of the rising edge of the first time sample
#UTC_START    {{utc_start}}               # yyyy-mm-dd-hh:mm:ss.fs
MJD_START    {{mjd}}                # MJD equivalent to the start UTC

OBS_OFFSET   0                   # bytes offset from the start MJD/UTC
OBS_OVERLAP  0                   # bytes by which neighbouring files overlap

# description of the source
SOURCE       {{source_name}}               # name of the astronomical source
RA           {{ra}}               # Right Ascension of the source
DEC          {{dec}}               # Declination of the source
MODE {{mode}}                      # Observation type
CALFREQ {{calfreq}}
# description of the instrument
TELESCOPE    {{telescope}}       # telescope name
INSTRUMENT   {{instrument}}              # instrument name
RECEIVER     {{receiver_name}}           # Frontend receiver
FREQ         {{frequency_mhz}}           # centre frequency in MHz
BW           {{bandwidth}}           # bandwidth of in MHz (-ve lower sb)
TSAMP        {{tsamp}}       # sampling interval in microseconds
BYTES_PER_SECOND  {{bytes_per_second}}

NBIT              {{nbit}}              # number of bits per sample
NDIM              {{ndim}}               # 1=real, 2=complex
NPOL              {{npol}}                 # number of polarizations observed
NCHAN             {{nchan}}                 # number of frequency channels
RESOLUTION        {{dsb}}
DSB

#MeerKAT specifics
DADA_KEY     {{key}}                    # The dada key to write to
DADA_MODE    4                       # The mode, 4=full dada functionality
ORDER        FTP                       # Here we are only capturing one polarisation, so data is time only
SYNC_TIME    {{sync_time}}
CLOCK_SAMPLE  {{sample_clock}}
PACKET_SIZE 8400
NTHREADS 32
#NHEAPS 256
#NGROUPS_DATA  4096
#NGROUPS_TEMP  2048
#NHEAPS_SWITCH 1024
MCAST_SOURCES {{mc_source}}   # 239.2.1.150 (+7)
PORT         {{mc_streaming_port}}
UDP_IF       unset
IBV_IF       {{interface}}  # This is the ethernet interface on which to capture
IBV_VECTOR   -1          # IBV forced into polling mode
IBV_MAX_POLL 10
BUFFER_SIZE 16777216
#BUFFER_SIZE 1048576
SAMPLE_CLOCK_START  unset # This should be updated with the sync-time of the packetiser to allow for UTC conversion from the sample clock                     
HEAP_NBYTES    4096
HEAP_SIZE    4096
#SPEAD specifcation for EDD packetiser data stream
NINDICES    2   # Although there is more than one index, we are only receiving one polarisation so only need to specify the time index
# The first index item is the running timestamp
IDX1_ITEM   0      # First item of a SPEAD heap
IDX1_STEP   4096   # The difference between successive timestamps
#IDX1_MODULO 3200000000

# The second index item distinguish between both polarizations
IDX2_ITEM   1
IDX2_LIST   0,1
IDX2_MASK   0x1

SLOTS_SKIP 10
DADA_NSLOTS 4
# end of header

"""

DADA_DEFAULTS = {
    "obs_id": "unset",
    "filesize": 320000000000,
    "mjd": 57000,
    "sync_time": 1574181085.0,
    "sample_clock": 3200000000,
    "mc_source": "239.2.1.150",
    "source": "B1937+21",
    "ra": "00:00:00.00",
    "dec": "00:00:00.00",
    "telescope": "Effelsberg",
    "instrument": "EDD",
    "mode": "PSR",
    "calfreq": 1.0,
    "receiver_name": "lband",
    "frequency_mhz": 1400.4,
    "bandwidth": 800,
    "tsamp": 0.000625,
    "mc_streaming_port" : 7148,
    "nbit": 8,
    "ndim": 1,
    "npol": 2,
    "nchan": 1,
    "resolution": 1,
    "dsb": 0,
    "key": "dada"
}


def dada_keygen():
    return binascii.hexlify(os.urandom(8))


def make_dada_key_string(key):
    return "DADA INFO:\nkey {0}".format(key)


def dada_defaults():
    out = DADA_DEFAULTS.copy()
    bytes_per_second = float(out["bandwidth"]) * 1e6 * \
        float(out["nchan"]) * 2 * out["npol"] * out["nbit"] / 8
    out.update({
        "bytes_per_second": bytes_per_second,
        "utc_start": datetime.utcnow().strftime('%Y-%m-%d-%H:%M:%S.%f')
    })
    return out


def render_dada_header(overrides):
    defaults = DADA_DEFAULTS.copy()
    defaults.update(overrides)
    bytes_per_second = defaults["bandwidth"] * 1e6 * \
        defaults["nchan"] * 2 * defaults["npol"] * defaults["nbit"] / 8
    defaults.update({
        "bytes_per_second": bytes_per_second,
        "utc_start": datetime.utcnow().strftime('%Y-%m-%d-%H:%M:%S.%f')
    })
    return jinja2.Template(DADA_HEADER).render(**defaults)
