import jinja2
import logging

log = logging.getLogger('mpikat.fbfuse_mksend_config')

HEADER_TEMPLATE = """
HEADER        DADA
HDR_VERSION   1.0
HDR_SIZE      4096
DADA_VERSION  1.0
DADA_KEY      {{dada_key}}
DADA_MODE     {{dada_mode}}
SYNC_TIME     {{sync_epoch}}
SAMPLE_CLOCK  {{sample_clock}}
MCAST_SOURCES {{mcast_sources}}
PORT          {{mcast_port}}
IBV_IF        {{interface}}
IBV_VECTOR    -1
IBV_MAX_POLL  10
PACKET_SIZE   1500
SAMPLE_CLOCK_START unset
NTHREADS      16
NHEAPS        64
NGROUPS_DATA  {{ngroups_data}}
NGROUPS_TEMP  {{ngroups_data//2}}
LEVEL_DATA    50
LEVEL_TEMP    50
HEAP_SIZE     {{heap_size}}

NINDICES    3
IDX1_ITEM   0
IDX1_STEP   {{timestamp_step}}

IDX2_ITEM   1
IDX2_LIST   {{ordered_feng_ids_csv}}

IDX3_ITEM   2
IDX3_LIST   {{frequency_partition_ids_csv}}
"""

MKRECV_STDOUT_KEYS = {
    "STAT": [("slot-size", int), ("heaps-completed", int),
             ("heaps-discarded", int), ("heaps-needed", int),
             ("payload-expected", int), ("payload-received", int)]
}


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


def mkrecv_stdout_parser(line):
    log.debug(line)
    tokens = line.split()
    params = {}
    if tokens[0] in MKRECV_STDOUT_KEYS:
        params = {}
        parser = MKRECV_STDOUT_KEYS[tokens[0]]
        for ii, (key, dtype) in enumerate(parser):
            params[key] = dtype(tokens[ii+1])
    return params
