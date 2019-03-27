import jinja2
import logging
from path import Path
from tornado.gen import coroutine
from subprocess import Popen, PIPE
from mpikat.utils.process_tools import process_watcher

log = logging.getLogger('mpikat.fbfuse_psrdada_cpp_wrapper')

PSRDADA_CPP_BASE_DIR = Path("/software/psrdada_cpp")
PSRDADA_CPP_BUILD_DIR = PSRDADA_CPP_BASE_DIR / "build"
PSRDADA_CPP_FBFUSE_DIR = PSRDADA_CPP_BASE_DIR / "psrdada_cpp/meerkat/fbfuse"
FBFUSE_CONSTANTS_FILE = PSRDADA_CPP_FBFUSE_DIR / "fbfuse_constants.hpp"

TEMPLATE = """
#ifndef PSRDADA_CPP_MEERKAT_FBFUSE_CONSTANTS_HPP
#define PSRDADA_CPP_MEERKAT_FBFUSE_CONSTANTS_HPP

#define FBFUSE_TOTAL_NANTENNAS {{total_nantennas}}   // The total number of antennas in the input DADA buffer
#define FBFUSE_NCHANS {{fbfuse_nchans}}              // The number of channels to be processes by this instance
#define FBFUSE_NCHANS_TOTAL {{total_nchans}}         // This is the F-engine channel count
#define FBFUSE_NSAMPLES_PER_HEAP 256
#define FBFUSE_NPOL 2

#define FBFUSE_CB_TSCRUNCH {{coherent_tscrunch}}
#define FBFUSE_CB_FSCRUNCH {{coherent_fscrunch}}
#define FBFUSE_CB_NANTENNAS {{coherent_nantennas}}
#define FBFUSE_CB_ANTENNA_OFFSET {{coherent_antenna_offset}}
#define FBFUSE_CB_NBEAMS {{coherent_nbeams}}
#define FBFUSE_CB_NTHREADS 1024
#define FBFUSE_CB_WARP_SIZE 32
#define FBFUSE_CB_NWARPS_PER_BLOCK (FBFUSE_CB_NTHREADS/FBFUSE_CB_WARP_SIZE)
#define FBFUSE_CB_NSAMPLES_PER_BLOCK (FBFUSE_CB_TSCRUNCH * FBFUSE_CB_NTHREADS / FBFUSE_CB_WARP_SIZE)
#define FBFUSE_CB_PACKET_SIZE 8192      //Do not change
#define FBFUSE_CB_HEAP_SIZE 8192
#define FBFUSE_CB_NCHANS_OUT FBFUSE_NCHANS / FBFUSE_CB_FSCRUNCH
#define FBFUSE_CB_NCHANS_PER_PACKET FBFUSE_CB_NCHANS_OUT
#define FBFUSE_CB_NSAMPLES_PER_PACKET (FBFUSE_CB_PACKET_SIZE / FBFUSE_CB_NCHANS_PER_PACKET)
#define FBFUSE_CB_NPACKETS_PER_HEAP (FBFUSE_CB_HEAP_SIZE / FBFUSE_CB_PACKET_SIZE)
#define FBFUSE_CB_NSAMPLES_PER_HEAP (FBFUSE_CB_NPACKETS_PER_HEAP * FBFUSE_CB_NSAMPLES_PER_PACKET)

#define FBFUSE_IB_TSCRUNCH {{incoherent_tscrunch}}
#define FBFUSE_IB_FSCRUNCH {{incoherent_fscrunch}}
#define FBFUSE_IB_NANTENNAS FBFUSE_TOTAL_NANTENNAS
#define FBFUSE_IB_ANTENNA_OFFSET 0
#define FBFUSE_IB_NBEAMS 1              //Do not change
#define FBFUSE_IB_NSAMPLES_PER_BLOCK (FBFUSE_IB_TSCRUNCH*FBFUSE_IB_NTHREADS / FBFUSE_IB_WARP_SIZE)
#define FBFUSE_IB_PACKET_SIZE 8192      //Do not change
#define FBFUSE_IB_HEAP_SIZE 8192
#define FBFUSE_IB_NCHANS_OUT FBFUSE_NCHANS / FBFUSE_IB_FSCRUNCH
#define FBFUSE_IB_NCHANS_PER_PACKET FBFUSE_IB_NCHANS_OUT
#define FBFUSE_IB_NSAMPLES_PER_PACKET (FBFUSE_IB_PACKET_SIZE / FBFUSE_IB_NCHANS_PER_PACKET)
#define FBFUSE_IB_NPACKETS_PER_HEAP (FBFUSE_IB_HEAP_SIZE / FBFUSE_IB_PACKET_SIZE)
#define FBFUSE_IB_NSAMPLES_PER_HEAP (FBFUSE_IB_NPACKETS_PER_HEAP * FBFUSE_IB_NSAMPLES_PER_PACKET)

#endif //PSRDADA_CPP_MEERKAT_FBFUSE_CONSTANTS_HPP
"""


class PsrdadaCppHeaderException(Exception):
    pass


class PsrdadaCppCompilationException(Exception):
    pass


class PsrdadaCppCompilationTimeout(PsrdadaCppCompilationException):
    pass


def make_psrdada_cpp_header(params, outfile=None):
    log.info("Generating PSRDADA_CPP header with params:\n{}".format(params))
    template = jinja2.Template(TEMPLATE)
    template.environment.undefined = jinja2.runtime.StrictUndefined
    try:
        rendered = template.render(params)
    except jinja2.exceptions.UndefinedError as error:
        raise PsrdadaCppHeaderException(
            "Error while rendering MKRECV configuration: {}".format(
                error.message))
    if outfile:
        with open(outfile, "w") as f:
            f.write(rendered)
        log.debug("Wrote header to file: {}".format(outfile))
    return rendered


@coroutine
def compile_psrdada_cpp(params):
    make_psrdada_cpp_header(params, outfile=str(FBFUSE_CONSTANTS_FILE))
    with PSRDADA_CPP_BUILD_DIR:
        cmake_cmd = ["cmake",
                     "-DENABLE_CUDA=true",
                     "-DCMAKE_BUILD_TYPE=release",
                     str(PSRDADA_CPP_BASE_DIR)]
        cmake_proc = Popen(cmake_cmd, stdout=PIPE, stderr=PIPE)
        log.info("Running CMake on PSRDADA_CPP")
        yield process_watcher(cmake_proc, timeout=60)

        make_cmd = ["make", "fbfuse", "-j", "16"]
        make_proc = Popen(make_cmd, stdout=PIPE, stderr=PIPE)
        log.info("Making PSRDADA_CPP")
        yield process_watcher(make_proc, timeout=600)

        make_install_cmd = ["make", "install", "fbfuse, ""-j", "16"]
        make_install_proc = Popen(make_install_cmd, stdout=PIPE, stderr=PIPE)
        log.info("Installing PSRDADA_CPP")
        yield process_watcher(make_install_proc, timeout=60)
