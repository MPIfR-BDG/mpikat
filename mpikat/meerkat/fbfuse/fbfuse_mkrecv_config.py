import jinja2
import logging

log = logging.getLogger('mpikat.fbfuse_mksend_config')

HEADER_TEMPLATE = """
HEADER       DADA                # Distributed aquisition and data analysis
HDR_VERSION  1.0                 # Version of this ASCII header
HDR_SIZE     4096                # Size of the header in bytes
DADA_VERSION 1.0                 # Version of the DADA Software

# DADA parameters
OBS_ID       unset               # observation ID
PRIMARY      unset               # primary node host name
SECONDARY    unset               # secondary node host name
FILE_NAME    unset               # full path of the data file

FILE_SIZE    10000000000         # requested size of data files
FILE_NUMBER  0                   # number of data file

# time of the rising edge of the first time sample
UTC_START    unset               # yyyy-mm-dd-hh:mm:ss.fs (set by MKRECV)
MJD_START    unset               # MJD equivalent to the start UTC (set by MKRECV)

OBS_OFFSET   0                   # bytes offset from the start MJD/UTC
OBS_OVERLAP  0                   # bytes by which neighbouring files overlap

# description of the source
SOURCE                      # name of the astronomical source
RA           unset               # Right Ascension of the source
DEC          unset               # Declination of the source

# description of the instrument
TELESCOPE    MeerKAT           # telescope name
INSTRUMENT   CBF-Feng          # instrument name
RECEIVER     unset             # Receiver name
FREQ         {{frequency_mhz}} # observation frequency
BW           {{bandwidth}}     # bandwidth in MHz
TSAMP        {{tsamp_us}}         # sampling interval in microseconds
BYTES_PER_SECOND {{bytes_per_second}}

NBIT         8             # number of bits per sample
NDIM         2             # dimension of samples (2=complex, 1=real)
NPOL         2             # number of polarizations observed
NCHAN        {{nchan}}     # number of channels here

#MeerKAT specifics
DADA_KEY     {{dada_key}}
DADA_MODE    4
NANTS        {{nantennas}}
ANTS         {{antennas_csv}}
ORDER        TAFTP
SYNC_TIME    {{sync_epoch}}
SAMPLE_CLOCK {{sample_clock}}
MCAST_SOURCES {{mcast_sources}}
PORT         {{mcast_port}}
UDP_IF       unset
IBV_IF       {{interface}}
IBV_VECTOR   -1
IBV_MAX_POLL 10
PACKET_SIZE  1500
SAMPLE_CLOCK_START 13
NTHREADS      16
NHEAPS        {{nheaps}}
NGROUPS_DATA  {{ngroups_data}}
NGROUPS_TEMP  {{ngroups_temp}}
NHEAPS_SWITCH 50

#MeerKat F-Engine
NINDICES    3
# The first index item is the running timestamp
IDX1_ITEM   0         # First item of a SPEAD heap (timestamp)
IDX1_STEP   {{timestamp_step}}   # The difference between successive timestamps

# The second index should be the antenna/F-eng
IDX2_ITEM   1                # Second item of a SPEAD heap (feng_id)
IDX2_MASK   0xffffffffffff   # Mask used to extract the F-eng ID
IDX2_LIST   {{ordered_feng_ids_csv}}

# The third index should be the frequency partition
IDX3_ITEM   2                # Third item of a SPEAD heap (frequency)
IDX3_MASK   0xffffffffffff   # Mask used to extract the frequency
IDX3_LIST   {{frequency_partition_ids_csv}}
"""


class MkrecvHeaderException(Exception):
    pass


def make_mkrecv_header(params, outfile=None):
    template = jinja2.Template(HEADER_TEMPLATE)
    template.environment.undefined = jinja2.runtime.StrictUndefined
    try:
        rendered = template.render(params)
    except jinja2.exceptions.UndefinedError as error:
        raise MkrecvHeaderException(
            "Error while rendering MKRECV configuration: {}".format(
                error.message))
    if outfile:
        with open(outfile) as f:
            f.write(rendered)
    return rendered
