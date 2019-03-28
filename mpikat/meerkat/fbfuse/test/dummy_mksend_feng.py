import jinja2
import logging
from subprocess import Popen, PIPE
from mpikat.utils.pipe_monitor import PipeMonitor
from mpikat.core.ip_manager import ContiguousIpRange

log = logging.getLogger('mpikat.dummy_mksend_feng')

HEADER_TEMPLATE = """
HEADER       DADA                # Distributed aquisition and data analysis
HDR_VERSION  1.0                 # Version of this ASCII header
HDR_SIZE     4096                # Size of the header in bytes
DADA_VERSION 1.0                 # Version of the DADA Software

PACKET_SIZE  1500                   # Jumbo frames
BUFFER_SIZE  8388608                # use default = 8 MB
NTHREADS     16
NHEAPS       36

DADA_MODE    0                     # 0 = artificial data
                                   # 1 = data from dada ringbuffer
DADA_KEY     dada

# The following options describe the connection to the network
NETWORK_MODE 1                      # 0 = no network
                                    # 1 = full network support
IBV_IF       {{interface}}
IBV_VECTOR   -1
IBV_MAX_POLL 10
UDP_IF       unset
NHOPS        6
RATE         {{rate}}
PORT         {{port}}
MCAST_DESTINATIONS {{destinations}}

# The following options describe the timing information
SYNC_TIME    {{sync_time}}
SAMPLE_CLOCK 1712000000.0
SAMPLE_CLOCK_START 0
UTC_START           unset

# The following options describe the outgoing heap structure
HEAP_SIZE    {{heap_size}}
HEAP_ID_START 1
HEAP_ID_OFFSET 1
HEAP_ID_STEP 8192
NSCI         0
NITEMS       3

# This item is a newly created timestamp
ITEM1_ID     5632
ITEM1_STEP   {{timestep}}
ITEM1_LIST   unset
ITEM1_INDEX  1
ITEM1_SCI    0

# This item is an index (antenna) specfied as a list
ITEM2_ID     21845
ITEM2_STEP   0
ITEM2_LIST   {{feng_ids}}
ITEM2_INDEX  3
ITEM2_SCI    0

# This item is an index (frequency) specified as a list
ITEM3_ID     16643
ITEM3_STEP   0
ITEM3_LIST   {{frequencies}}
ITEM3_INDEX  2
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


def make_mksend_header(params, outfile=None):
    template = jinja2.Template(HEADER_TEMPLATE)
    template.environment.undefined = jinja2.runtime.StrictUndefined
    try:
        rendered = template.render(params)
    except jinja2.exceptions.UndefinedError as error:
        raise Exception(
            "Error while rendering MKSEND configuration: {}".format(
                error.message))
    if outfile:
        with open(outfile, "w") as f:
            f.write(rendered)
    return rendered


class CustomMonitor(PipeMonitor):
    def parse_line(self, line):
        print(line)


if __name__ == "__main__":
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Start and MKSEND stream')

    parser.add_argument('--nantennas', dest='nants', type=int,
                        help='The number of F-engines to stream from')
    parser.add_argument('--nbands', dest='nbands', type=int,
                        help='The number of frequency bands')
    parser.add_argument('--nchans', dest='nchans', type=int,
                        help='The number of channels per band')
    parser.add_argument('--base-ip', dest='base_ip', type=str,
                        help='The base multicast group to send to')
    parser.add_argument('--interface', dest='interface', type=str,
                        help='The interface to capture on')
    parser.add_argument('--rate', dest='rate', type=float,
                        help='The data rate in Gbps', default=1.0)
    args = parser.parse_args()

    ip_range = ContiguousIpRange(args.base_ip, 7148, args.nbands)

    params = {
        "sync_time": 12353524243.0,
        "interface": args.interface,
        "rate": args.rate * 1e9 / args.nbands,
        "destinations": ",".join([str(i) for i in ip_range]),
        "heap_size": args.nchans * 1024,
        "timestep": 2097152,
        "feng_ids": ",".join(map(str, range(args.nants))),
        "frequencies": ",".join(map(str, range(0, args.nbands * args.nchans, args.nchans))),
        "heap_group": args.nants,
        "port": 7148
    }
    outfile = "dummy_mksenf_feng.cfg"
    header = make_mksend_header(params, outfile=outfile)
    print(header)
    cmdline = ["mksend", "--header", outfile]
    proc = Popen(cmdline, stdout=PIPE, stderr=PIPE, shell=False,
                 close_fds=True)
    monitor = CustomMonitor(proc.stdout, {})
    monitor.start()
