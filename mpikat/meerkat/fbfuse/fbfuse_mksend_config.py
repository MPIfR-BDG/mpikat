import jinja2
import logging
import time
from subprocess import Popen, PIPE
from mpikat.utils.pipe_moitor import PipeMonitor
from mpikat.utils.process_tools import ProcessMonitor

log = logging.getLogger('mpikat.fbfuse_mksend_config')

HEADER_TEMPLATE = """
HEADER       DADA
HDR_VERSION  1.0
HDR_SIZE     4096
DADA_VERSION 1.0

PACKET_SIZE  9000
BUFFER_SIZE  8388608
NTHREADS     16
NHEAPS       36

DADA_MODE    {{dada_mode}}
DADA_KEY     {{dada_key}}
NETWORK_MODE 1

IBV_IF       {{interface}}
NHOPS        6
RATE         {{data_rate}}
PORT         {{mcast_port}}
MCAST_DESTINATIONS {{mcast_destinations | join(',')}}

SYNC_TIME    {{sync_epoch}}
SAMPLE_CLOCK {{sample_clock}}
SAMPLE_CLOCK_START 0
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
ITEM2_LIST   {{beam_ids | join(',')}}
{{'ITEM2_INDEX  2' if len(beam_ids) > 1 else ''}}

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


class MsendProcessManager(object):
    def __init__(self, header_file):
        self._header_file = header_file
        self._mksend_proc = None
        self._stdout_mon = None
        self._stderr_mon = None
        self._proc_mon = None

    def _stdout_parser(self, line):
        return None

    def start(self):
        self._mksend_proc = Popen(
            ["mksend", "--header", self._header_file, "--quiet"],
            stdout=PIPE, stderr=PIPE, shell=False, close_fds=True)
        self._proc_mon = ProcessMonitor(
            self._mksend_proc, self.stop)
        self._proc_mon.start()
        self._stdout_mon = PipeMonitor(
            self._mksend_proc.stdout, self._stdout_parser)
        self._stdout_mon.start()
        self._stderr_mon = PipeMonitor(
            self._mksend_proc.stderr, lambda line: log.error(line))
        self._stderr_mon.stop()

    def stop(self, timeout=5):
        self._proc_mon.stop()
        self._proc_mon.join()
        self._stdout_mon.stop()
        self._stdout_mon.join()
        self._stderr_mon.stop()
        self._stderr_mon.join()
        start = time.time()
        self._mksend_proc.terminate()
        while self._mksend_proc.poll() is None:
            time.sleep(0.2)
            if (time.time() - start) > timeout:
                self._mksend_proc.kill()