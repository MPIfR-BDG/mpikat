import jinja2
import logging

log = logging.getLogger('mpikat.fbfuse_mksend_config')

HEADER_TEMPLATE = """
PACKET_SIZE  9000                   # Jumbo frames
BUFFER_SIZE  8388608                # use default = 8 MB
NTHREADS     16
NHEAPS       36

DADA_MODE    1                      # 0 = artificial data
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
NITEMS       2

# This item is a newly created timestamp
ITEM1_ID     5632
ITEM1_STEP   {{timestamp_step}}
ITEM1_LIST   unset
ITEM1_INDEX  1
ITEM1_SCI    0


# This item is an index (beam) specfied as a list
ITEM2_ID     21845
ITEM2_STEP   0
ITEM2_LIST   {{beam_ids | join(',')}}
ITEM2_INDEX  2
ITEM2_SCI    0


# This item is an index (frequency) specified as a list
ITEM3_ID     16643
ITEM3_STEP   0
ITEM3_LIST   {{subband_idx}}
ITEM3_INDEX  3
ITEM3_SCI    0

# This value is a required dummy item to encode "cbf_raw"
# (effectively this item is the data)
ITEM4_ID     21846
ITEM4_STEP   0
ITEM4_LIST   unset
ITEM4_INDEX  0
ITEM4_SCI    0
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
        with open(outfile) as f:
            f.write(rendered)
    return rendered
