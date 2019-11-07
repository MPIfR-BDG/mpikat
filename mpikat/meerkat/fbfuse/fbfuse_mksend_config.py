import jinja2
import logging

log = logging.getLogger('mpikat.fbfuse_mksend_config')

HEADER_TEMPLATE = """
HEADER       DADA
HDR_VERSION  1.0
HDR_SIZE     4096
DADA_VERSION 1.0

PACKET_SIZE  9000
BURST_SIZE 9000
BURST_RATE 1.02
BUFFER_SIZE  8388608
NTHREADS     16
NHEAPS       36

DADA_MODE    {{dada_mode}}
DADA_KEY     {{dada_key}}
NETWORK_MODE 1

UDP_IF       {{interface}}
NHOPS        6
RATE         {{data_rate}}
PORT         {{mcast_port}}
MCAST_DESTINATIONS {{mcast_destinations}}

SYNC_TIME    {{sync_epoch}}
SAMPLE_CLOCK {{sample_clock}}
SAMPLE_CLOCK_START unset
UTC_START unset

HEAP_SIZE     {{heap_size}}
HEAP_ID_START {{heap_id_start}}
HEAP_ID_OFFSET 1
HEAP_ID_STEP 8192
NSCI         0
NITEMS       4

# This item is a newly created timestamp
ITEM1_ID     5632
ITEM1_STEP   {{timestamp_step}}
ITEM1_INDEX  1

ITEM2_ID     21845
ITEM2_LIST   {{beam_ids}}
{% if multibeam %}ITEM2_INDEX 2{% endif %}

ITEM3_ID     16643
ITEM3_LIST   {{subband_idx}}

ITEM4_ID     21846
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
