import jinja2
import logging

log = logging.getLogger('mpikat.fbfuse_mksend_config')

HEADER_TEMPLATE = """
HEADER       DADA
HDR_VERSION  1.0
HDR_SIZE     4096
DADA_VERSION 1.0

# DADA parameters
OBS_ID       unset
PRIMARY      unset
SECONDARY    unset
FILE_NAME    unset

FILE_SIZE    10000000000
FILE_NUMBER  0

# time of the rising edge of the first time sample
UTC_START    unset
MJD_START    unset

OBS_OFFSET   0
OBS_OVERLAP  0

# description of the source
SOURCE       unset
RA           unset
DEC          unset

# description of the instrument
TELESCOPE    MeerKAT
INSTRUMENT   CBF-Feng
RECEIVER     unset
FREQ         {{frequency_mhz}}
BW           {{bandwidth}}
TSAMP        {{tsamp_us}}
BYTES_PER_SECOND {{bytes_per_second}}

NBIT         8
NDIM         2
NPOL         2
NCHAN        {{nchan}}

#MeerKAT specifics
DADA_KEY     {{dada_key}}
DADA_MODE    {{dada_mode}}
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
SAMPLE_CLOCK_START unset
NTHREADS      16
NHEAPS        64
NGROUPS_DATA  {{ngroups_data}}
NGROUPS_TEMP  {{ngroups_temp}}
NHEAPS_SWITCH 50

#MeerKat F-Engine
NINDICES    3
# The first index item is the running timestamp
IDX1_ITEM   0
IDX1_STEP   {{timestamp_step}}

# The second index should be the antenna/F-eng
IDX2_ITEM   1
IDX2_MASK   0xffffffffffff
IDX2_LIST   {{ordered_feng_ids_csv}}

# The third index should be the frequency partition
IDX3_ITEM   2
IDX3_MASK   0xffffffffffff
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
        with open(outfile, "w") as f:
            f.write(rendered)
    return rendered
