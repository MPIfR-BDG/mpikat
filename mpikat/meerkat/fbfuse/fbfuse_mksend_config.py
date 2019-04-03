import jinja2
import logging

log = logging.getLogger('mpikat.fbfuse_mksend_config')

HEADER_TEMPLATE = """
HEADER       DADA                # Distributed aquisition and data analysis
HDR_VERSION  1.0                 # Version of this ASCII header
HDR_SIZE     4096                # Size of the header in bytes
DADA_VERSION 1.0                 # Version of the DADA Software

PACKET_SIZE  9000                   # Jumbo frames
BUFFER_SIZE  8388608                # use default = 8 MB
NTHREADS     16
NHEAPS       36

DADA_MODE    {{dada_mode}}                     # 0 = artificial data
                                    # 1 = data from dada ringbuffer
DADA_KEY     {{dada_key}}

# The following options describe the connection to the network
NETWORK_MODE 1                      # 0 = no network
                                    # 1 = full network support
IBV_IF       unset
IBV_VECTOR   -1
IBV_MAX_POLL 10
UDP_IF       {{interface}}
NHOPS        6
RATE         {{data_rate}}
PORT         {{mcast_port}}
MCAST_DESTINATIONS {{mcast_destinations | join(',')}}

# The following options describe the timing information
SYNC_TIME    {{sync_epoch}}
SAMPLE_CLOCK {{sample_clock}}
SAMPLE_CLOCK_START 0
UTC_START           unset

# The following options describe the outgoing heap structure
HEAP_SIZE    {{heap_size}}
HEAP_ID_START 1
HEAP_ID_OFFSET 1
HEAP_ID_STEP 8192
NSCI         0
NITEMS       4

# This item is a newly created timestamp
ITEM1_ID     5632
ITEM1_STEP   {{timestamp_step}}
ITEM1_LIST   unset
ITEM1_INDEX  1
ITEM1_SCI    0


# This item is an index (beam) specfied as a list
ITEM2_ID     21845
ITEM2_STEP   unset
ITEM2_LIST   {{beam_ids | join(',')}}
ITEM2_INDEX  2
ITEM2_SCI    unset


# This item is an index (frequency) specified as a list
ITEM3_ID     16643
ITEM3_STEP   unset
ITEM3_LIST   {{subband_idx}}
ITEM3_INDEX  unset
ITEM3_SCI    unset

# This value is a required dummy item to encode "cbf_raw"
# (effectively this item is the data)
ITEM4_ID     21846
ITEM4_STEP   unset
ITEM4_LIST   unset
ITEM4_INDEX  unset
ITEM4_SCI    unset
HEAP_GROUP   {{heap_group}}
"""

class MksendHeaderException(Exception):
    pass

def make_mksend_header(params, outfile=None):
    template = jinja2.Template(HEADER_TEMPLATE)
    template.environment.undefined = jinja2.runtime.StrictUndefined
    try:
        rendered = template.render(params)
    except jinja2.exceptions.UndefinedError as error:
        raise MksendHeaderException(
            "Error while rendering MKSEND configuration: {}".format(
                error.message))
    if outfile:
        with open(outfile, "w") as f:
            f.write(rendered)
    return rendered
