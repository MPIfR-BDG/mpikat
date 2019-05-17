import os
import binascii
from datetime import datetime
import jinja2

DADA_HEADER = """
HEADER       DADA                # Distributed aquisition and data analysis
HDR_VERSION  1.0                 # Version of this ASCII header
HDR_SIZE     4096                # Size of the header in bytes

DADA_VERSION 1.0                 # Version of the DADA Software
PIC_VERSION  1.0                 # Version of the PIC FPGA Software

# DADA parameters
OBS_ID       {{obs_id}}          # observation ID
PRIMARY      unset               # primary node host name
SECONDARY    unset               # secondary node host name
FILE_NAME    unset               # full path of the data file

FILE_SIZE    {{filesize}}  # requested file size
FILE_NUMBER  0           # number of data file

# time of the rising edge of the first time sample
UTC_START    {{utc_start}}               # yyyy-mm-dd-hh:mm:ss.fs
MJD_START    {{mjd}}            # MJD equivalent to the start UTC

OBS_OFFSET   0                   # bytes offset from the start MJD/UTC
OBS_OVERLAP  0                   # bytes by which neighbouring files overlap

# description of the source
SOURCE {{source_name}}        # source name
RA     {{ra}}                 # RA of source
DEC    {{dec}}                # DEC of source

# description of the instrument
TELESCOPE    {{telescope}}     # telescope name
INSTRUMENT   {{instrument}}    # instrument name
RECEIVER     {{receiver_name}} # Receiver name
FREQ         {{frequency_mhz}} # observation frequency
BW           {{bandwidth}}     # bandwidth in MHz
TSAMP        {{tsamp}}         # sampling interval in microseconds
BYTES_PER_SECOND {{bytes_per_second}}

NBIT         {{nbit}}                   # number of bits per sample
NDIM         {{ndim}}                   # dimension of samples (2=complex, 1=real)
NPOL         {{npol}}                   # number of polarizations observed
NCHAN        {{nchan}}                  # number of channels here
RESOLUTION   {{resolution}}             # a parameter that is unclear
DSB          {{dsb}}
# end of header
"""

DADA_DEFAULTS = {
    "obs_id": "unset",
    "filesize": 2500000000,
    "mjd": 5555.55555,
    "source": "B1937+21",
    "ra": "00:00:00.00",
    "dec": "00:00:00.00",
    "telescope": "Meerkat",
    "instrument": "feng",
    "receiver_name": "lband",
    "frequency_mhz": 1260,
    "bandwidth": 16,
    "tsamp": 0.0625,
    "nbit": 8,
    "ndim": 2,
    "npol": 2,
    "nchan": 1,
    "resolution":1,
    "dsb":0
}


def dada_keygen():
    return binascii.hexlify(os.urandom(8))


def make_dada_key_string(key):
    return "DADA INFO:\nkey {0}".format(key)


def dada_defaults():
    out = DADA_DEFAULTS.copy()
    bytes_per_second = out["bandwidth"] * 1e6 * \
        out["nchan"] * 2 * out["npol"] * out["nbit"] / 8
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
