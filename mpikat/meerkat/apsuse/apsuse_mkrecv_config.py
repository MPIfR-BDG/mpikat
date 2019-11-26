import jinja2
import logging
import time
from collections import deque

log = logging.getLogger('mpikat.apsuse_mkrecv_config')

HEADER_TEMPLATE = """
HEADER        DADA
HDR_VERSION   1.0
HDR_SIZE      4096
DADA_VERSION  1.0
DADA_KEY      {{dada_key}}
DADA_MODE     {{dada_mode}}

BW            {{bandwidth}}
FREQ          {{centre_frequency}}
NCHAN         {{nchannels}}
NBIT          8
TSAMP         {{sampling_interval}}
SOURCE        not_a_source

SYNC_TIME     {{sync_epoch}}
SAMPLE_CLOCK  {{sample_clock}}
MCAST_SOURCES {{mcast_sources}}
PORT          {{mcast_port}}
IBV_IF        {{interface}}
IBV_VECTOR    -1
IBV_MAX_POLL  10
BUFFER_SIZE   33554432
PACKET_SIZE   9000
SAMPLE_CLOCK_START unset
NTHREADS      6
NHEAPS        64
NGROUPS_DATA  {{ngroups_data}}
LEVEL_DATA    50
LEVEL_TEMP    50
HEAP_SIZE     {{heap_size}}

NINDICES    3
IDX1_ITEM   0
IDX1_STEP   {{timestamp_step}}
IDX1_MODULO {{timestamp_modulus}}

IDX2_ITEM   1
IDX2_LIST   {{beam_ids_csv}}

IDX3_ITEM   2
IDX3_LIST   {{freq_ids_csv}}
"""


MKRECV_STDOUT_KEYS = {
    "STAT": [("slot-size", int),
             ("heaps-completed", int),
             ("heaps-discarded", int),
             ("heaps-needed", int),
             ("payload-expected", int),
             ("payload-received", int),
             ("global-heaps-completed", int),
             ("global-heaps-discarded", int),
             ("global-heaps-needed", int),
             ("global-payload-expected", int),
             ("global-payload-received", int)]
}


class MkrecvHeaderException(Exception):
    pass


class MkrecvStdoutHandler(object):
    def __init__(self, logging_interval=10.0,
                 window_size=10, warning_level=99.8,
                 callback=None):
        self._logging_interval = logging_interval
        self._window_size = window_size
        self._warning_level = warning_level
        self._callback = callback
        self._last_log = 0.0
        self._stats_buffer = deque(maxlen=self._window_size)
        self._current_percentage = 0.0
        self._average_percentage = 0.0
        self._total_percentage = 0.0
        self._has_started = False
        self._warning_state = False

    def __call__(self, line):
        params = mkrecv_stdout_parser(line)
        if not params:
            return
        self._current_percentage = (100 * params['heaps-completed']
                                    / float(params['slot-size']))
        self._total_percentage = (100 * params['global-payload-received']
                                  / float(params['global-payload-expected']))
        self._stats_buffer.append(self._current_percentage)
        self._average_percentage = (
            sum(self._stats_buffer)/len(self._stats_buffer))

        if not self._has_started:
            log.info("MKRECV capture started")
            self._has_started = True

        if self._average_percentage < self._warning_level:
            if not self._warning_state:
                log.warning(("Capture percentage dropped below "
                             "{}% tolerance: {:9.5f}%".format(
                                self._warning_level,
                                self._average_percentage)))
                self._warning_state = True
        else:
            if self._warning_state:
                log.info("Capture percentage recovered to non-warning levels")
                self._warning_state = False

        if self._warning_state:
            log_func = log.warning
        else:
            log_func = log.info

        now = time.time()
        if (now - self._last_log) > self._logging_interval:
            log_func(("Capture percentages: "
                      "{:9.5f}% (snap-shot) "
                      "{:9.5f}% (accumulated) "
                      "{:9.5f} (last {} buffers)").format(
              self._current_percentage, self._total_percentage,
              self._average_percentage, self._window_size))
            self._last_log = now
            if self._callback:
                self._callback(
                    self._current_percentage, self._total_percentage,
                    self._average_percentage, self._window_size)


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
    if len(tokens) == 0:
        return None
    if tokens[0] in MKRECV_STDOUT_KEYS:
        params = {}
        parser = MKRECV_STDOUT_KEYS[tokens[0]]
        for ii, (key, dtype) in enumerate(parser):
            params[key] = dtype(tokens[ii+1])
    return params
