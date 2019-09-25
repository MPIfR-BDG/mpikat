"""
Copyright (c) 2019 Tobias Winchen <twinchen@mpifr-bonn.mpg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
from mpikat.utils.process_tools import ManagedProcess, command_watcher
from mpikat.utils.process_monitor import SubprocessMonitor
from mpikat.utils.db_monitor import DbMonitor
from mpikat.utils.mkrecv_stdout_parser import MkrecvSensors
from mpikat.effelsberg.edd.pipeline.pipeline_register import register_pipeline
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer
import mpikat.utils.numa as numa

from tornado.gen import coroutine
from katcp import Sensor, AsyncReply, FailReply

from math import ceil
import os
import time
import logging
import signal
from optparse import OptionParser
import coloredlogs
import json
import tempfile

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.VLBIPipeline")
log.setLevel('DEBUG')

POLARIZATIONS = ["polarization_0", "polarization_1"]

DEFAULT_CONFIG = {
        "input_bit_depth" : 12,                             # Input bit-depth
        "samples_per_heap": 4096,                           # this needs to be consistent with the mkrecv configuration
        "samples_per_block": 512 * 1024 * 1024,             # 512 Mega sampels per buffer block to allow high res  spectra - the theoretical mazimum is thus 256 M Channels. Need to be power of two
        "enabled_polarizations" : ["polarization_1"],
        "sample_clock" : 2600000000,
        "sync_time" : 1554915838,

#        "output_bit_depth": 2,

        "null_output": False,                               # Disable sending of data for testing purposes
        "dummy_input": False,                               # Use dummy input instead of mkrecv process.
        "log_level": "debug",

        "output_rate_factor": 1.10,                         # True output date rate is multiplied by this factor for sending.
        "payload_size": 8192,                               # Size of the payload for one VDIF package
        "vdif_header_size": 32,                             # Size of the VDIF header in bytes 

        "polarization_0" :
        {
            "ibv_if": "10.10.1.10",
            "mcast_sources": "225.0.0.152+3",
            "mcast_dest": "225.0.0.123",
            "port_rx": "7148",
            "port_tx": "8125",
            "dada_key": "dada",                             # output keys are the reverse!
            "numa_node": "1",                               # we only have one ethernet interface on numa node 1
            "thread_id": "0",                               # for vdif header 
            "station_id": "0",                               # for vdif header 
        },
         "polarization_1" :
        {
            "ibv_if": "10.10.1.11",
            "mcast_sources": "225.0.0.156+3",
            "mcast_dest": "225.0.0.124",
            "port_rx": "7148",
            "port_tx": "8125",
            "dada_key": "dadc",
            "numa_node": "1",                               # we only have one ethernet interface on numa node 1
            "thread_id": "1",                               # for vdif header 
            "station_id": "1",                               # for vdif header 
        }
    }

# static configuration for mkrec. all items that can be configured are passed
# via cmdline
mkrecv_header = """
## Dada header configuration
HEADER          DADA
HDR_VERSION     1.0
HDR_SIZE        4096
DADA_VERSION    1.0

## MKRECV configuration
PACKET_SIZE         8400
IBV_VECTOR          -1          # IBV forced into polling mode
IBV_MAX_POLL        10
BUFFER_SIZE         128000000

DADA_MODE           4    # The mode, 4=full dada functionality

SAMPLE_CLOCK_START  0 # This is updated with the sync-time of the packetiser to allow for UTC conversion from the sample clock

NTHREADS            32
NHEAPS              64
NGROUPS_TEMP        65536

#SPEAD specifcation for EDD packetiser data stream
NINDICES            1      # Although there is more than one index, we are only receiving one polarisation so only need to specify the time index

# The first index item is the running timestamp
IDX1_ITEM           0      # First item of a SPEAD heap
IDX1_STEP           4096   # The difference between successive timestamps. This is the number of sampels per heap

# Add side item to buffer
#SCI_LIST            2
"""


@register_pipeline("VLBIPipeline")
class VLBIPipeline(EDDPipeline):
    """@brief VLBI pipeline class."""
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")

    def __init__(self, ip, port, scpi_ip, scpi_port):
        """@brief initialize the pipeline."""
        EDDPipeline.__init__(self, ip, port, scpi_ip, scpi_port)

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        EDDPipeline.setup_sensors(self)

        self._edd_config_sensor = Sensor.string(
            "current-config",
            description="The current configuration for the EDD backend",
            default=json.dumps(DEFAULT_CONFIG, indent=4),
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._edd_config_sensor)

        self._output_rate_status = Sensor.float(
            "output-rate",
            description="Output data rate [Gbyte/s]",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._output_rate_status)



        self._polarization_sensors = {}
        for p in POLARIZATIONS:
            self._polarization_sensors[p] = {}
            self._polarization_sensors[p]["mkrecv_sensors"] = MkrecvSensors(p)
            for s in self._polarization_sensors[p]["mkrecv_sensors"].sensors.itervalues():
                self.add_sensor(s)
            self._polarization_sensors[p]["input-buffer-fill-level"] = Sensor.float(
                    "input-buffer-fill-level-{}".format(p),
                    description="Fill level of the input buffer for polarization{}".format(p),
                    params=[0, 1]
                    )
            self.add_sensor(self._polarization_sensors[p]["input-buffer-fill-level"])
            self._polarization_sensors[p]["input-buffer-total-write"] = Sensor.float(
                    "input-buffer-total-write-{}".format(p),
                    description="Total write into input buffer for polarization {}".format(p),
                    params=[0, 1]
                    )

            self.add_sensor(self._polarization_sensors[p]["input-buffer-total-write"])
            self._polarization_sensors[p]["output-buffer-fill-level"] = Sensor.float(
                    "output-buffer-fill-level-{}".format(p),
                    description="Fill level of the output buffer for polarization {}".format(p)
                    )
            self._polarization_sensors[p]["output-buffer-total-read"] = Sensor.float(
                    "output-buffer-total-read-{}".format(p),
                    description="Total read from output buffer for polarization {}".format(p)
                    )
            self.add_sensor(self._polarization_sensors[p]["output-buffer-total-read"])
            self.add_sensor(self._polarization_sensors[p]["output-buffer-fill-level"])



    @coroutine
    def _create_ring_buffer(self, bufferSize, blocks, key, numa_node):
         """
         Create a ring buffer of given size with given key on specified numa node.
         Adds and register an appropriate sensor to thw list
         """
         # always clear buffer first. Allow fail here
         yield command_watcher("dada_db -d -k {key}".format(key=key), allow_fail=True)

         cmd = "numactl --cpubind={numa_node} --membind={numa_node} dada_db -k {key} -n {blocks} -b {bufferSize} -p -l".format(key=key, blocks=blocks, bufferSize=bufferSize, numa_node=numa_node)
         log.debug("Running command: {0}".format(cmd))
         yield command_watcher(cmd)

         M = DbMonitor(key, self._buffer_status_handle)
         M.start()
         self._dada_buffers.append({'key': key, 'monitor': M})


    def _buffer_status_handle(self, status):
        """
        Process a change in the buffer status
        """
        for p in POLARIZATIONS:
            if status['key'] == self._config[p]['dada_key']:
                self._polarization_sensors[p]["input-buffer-total-write"].set_value(status['written'])
                self._polarization_sensors[p]["input-buffer-fill-level"].set_value(status['fraction-full'])
            elif status['key'] == self._config[p]['dada_key'][::-1]:
                self._polarization_sensors[p]["output-buffer-fill-level"].set_value(status['fraction-full'])
                self._polarization_sensors[p]["output-buffer-total-read"].set_value(status['read'])


    @coroutine
    def configure(self, config_json):
        """@brief destroy any ring buffer and create new ring buffer."""
        """
        @brief   Configure the EDD gated spectrometer

        @param   config_json    A JSON dictionary object containing configuration information

        @detail  The configuration dictionary is highly flexible. An example is below:
                 @code
                     {
                         "nbeams": 1,
                         "nchans": 2048,
                         "freq_res": "something in Hz"
                         "integration_time": 1.0,
                         "mc_address": "255.0.0.152+8"
                         "mc_port": 7148
                     }
                 @endcode
        """
        log.info("Configuring EDD backend for processing")
        log.debug("Configuration string: '{}'".format(config_json))
        if self.state != "idle":
            raise FailReply('Cannot configure pipeline. Pipeline state {}.'.format(self.state))
        # alternatively we should automatically deconfigure
        #yield self.deconfigure()

        self.state = "configuring"
        # Merge retrieved config into default via recursive dict merge
        def __updateConfig(oldo, new):
            old = oldo.copy()
            for k in new:
                if isinstance(old[k], dict):
                    old[k] = __updateConfig(old[k], new[k])
                else:
                    old[k] = new[k]
            return old

        if isinstance(config_json, str):
            cfg = json.loads(config_json)
        elif isinstance(config_json, dict):
            cfg = config_json
        else:
            self.state = "idle"     # no states changed
            raise FailReply("Cannot handle config type {}. Config has to bei either json formatted string or dict!".format(type(config_json)))
        try:
            self._config = __updateConfig(DEFAULT_CONFIG, cfg)
        except KeyError as error:
            self.state = "idle"     # no states changed
            raise FailReply("Unknown configuration option: {}".format(str(error)))


        cfs = json.dumps(self._config, indent=4)
        log.info("Received configuration:\n" + cfs)
        self._edd_config_sensor.set_value(cfs)

        # calculate input buffer parameters
        self.input_heapSize =  self._config["samples_per_heap"] * self._config['input_bit_depth'] / 8
        nHeaps = self._config["samples_per_block"] / self._config["samples_per_heap"]
        input_bufferSize = nHeaps * (self.input_heapSize)       #ToDo: Get this from the buffer configuration, bastract this -> psrdada_cpp has this calculation!
        log.info('Input dada parameters created from configuration:\n\
                heap size:        {} byte\n\
                heaps per block:  {}\n\
                buffer size:      {} byte'.format(self.input_heapSize, nHeaps, input_bufferSize))

        # calculate output buffer parameters
        size_of_samples = ceil(1. * self._config["samples_per_block"] * 2 / 8.) # byte
        number_of_packages = int(ceil(size_of_samples / self._config['payload_size']))
        output_buffer_size = number_of_packages * (self._config['payload_size'] + self._config['vdif_header_size'])

        integration_time = self._config["samples_per_block"] / float(self._config["sample_clock"])

        rate = output_buffer_size / integration_time# in spead documentation BYTE per second and not bit!
        rate *= self._config["output_rate_factor"]        # set rate to (100+X)% of expected rate
        self._output_rate_status.set_value(rate / 1E9)


        log.info('Output parameters calculated from configuration:\n\
                total size of data samples:        {} byte\n\
                number_of_packages:  {}\n\
                size of output buffer:      {} byte\n\
                rate ({:.0f}%):        {} Gbps'.format(size_of_samples,
                    number_of_packages, output_buffer_size,
                    self._config["output_rate_factor"]*100, rate / 1E9))

        self._subprocessMonitor = SubprocessMonitor()

        for i, k in enumerate(self._config['enabled_polarizations']):
            numa_node = self._config[k]['numa_node']

            # configure dada buffer
            bufferName = self._config[k]['dada_key']
            yield self._create_ring_buffer(input_bufferSize, 64, bufferName, numa_node) #ToDo: Magic Number 64

            ofname = bufferName[::-1]
            # we write nSlice blocks on each go
            yield self._create_ring_buffer(output_buffer_size, 8, ofname, numa_node) #ToDo: Magick Number 8

            # Configure + launch gated spectrometer
            # here should be a smarter system to parse the options from the
            # controller to the program without redundant typing of options
            physcpu = numa.getInfo()[numa_node]['cores'][0]
            cmd = "taskset {physcpu} VLBI --input_key={dada_key} --speadheap_size={heapSize} --thread_id={thread_id} --station_id={station_id} --payload_size={payload_size} --sample_rate={sample_clock} --nbits={input_bit_depth} -o {ofname} --log_level={log_level} --output_type=dada".format(dada_key=bufferName, ofname=ofname, heapSize=self.input_heapSize, numa_node=numa_node, physcpu=physcpu, thread_id=self._config[k]['thread_id'], station_id=self._config[k]['station_id'], **self._config)
            log.debug("Command to run: {}".format(cmd))

            cudaDevice = numa.getInfo()[self._config[k]["numa_node"]]["gpus"][0]
            gated_cli = ManagedProcess(cmd, env={"CUDA_VISIBLE_DEVICES": cudaDevice})
            self._subprocessMonitor.add(gated_cli, self._subprocess_error)
            self._subprocesses.append(gated_cli)

            if not self._config["null_output"]:
                cfg = self._config.copy()
                cfg.update(self._config[k])

                physcpu = ",".join(numa.getInfo()[numa_node]['cores'][1:2])
                cmd = "taskset {physcpu} vdif_send --input_key {ofname} --if_ip {ibv_if} --dest_ip {mcast_dest} --port {port_tx} --max_rate {rate}".format(                        ofname=ofname, physcpu=physcpu, rate=rate, **cfg)
                log.debug("Command to run: {}".format(cmd))

                mks = ManagedProcess(cmd)
                self._subprocessMonitor.add(mks, self._subprocess_error)
                self._subprocesses.append(mks)
            else:
                log.warning("Selected null output. Not sending data!")


        self._subprocessMonitor.start()
        self.state = "ready"


    @coroutine
    def capture_start(self, config_json=""):
        """@brief start the dspsr instance then turn on dada_junkdb instance."""
        log.info("Starting EDD backend")
        if self.state != "ready":
            raise FailReply("pipleine state is not in state = ready, but in state = {} - cannot start the pipeline".format(self.state))
            #return

        self.state = "starting"
        try:
            mkrecvheader_file = tempfile.NamedTemporaryFile(delete=False)
            log.debug("Creating mkrec header file: {}".format(mkrecvheader_file.name))
            mkrecvheader_file.write(mkrecv_header)
            # DADA may need this
            mkrecvheader_file.write("NBIT {}\n".format(self._config["input_bit_depth"]))
            mkrecvheader_file.write("HEAP_SIZE {}\n".format(self.input_heapSize))

            mkrecvheader_file.write("\n#OTHER PARAMETERS\n")
            mkrecvheader_file.write("samples_per_block {}\n".format(self._config["samples_per_block"]))

            mkrecvheader_file.write("\n#PARAMETERS ADDED AUTOMATICALLY BY MKRECV\n")
            mkrecvheader_file.close()

            for i, k in enumerate(self._config['enabled_polarizations']):
                cfg = self._config.copy()
                cfg.update(self._config[k])
                if not self._config['dummy_input']:
                    numa_node = self._config[k]['numa_node']
                    physcpu = ",".join(numa.getInfo()[numa_node]['cores'][2:7])
                    cmd = "taskset {physcpu} mkrecv_nt --quiet --header {mkrecv_header} --dada-key {dada_key} \
                    --sync-epoch {sync_time} --sample-clock {sample_clock} \
                    --ibv-if {ibv_if} --port {port_rx} {mcast_sources}".format(mkrecv_header=mkrecvheader_file.name, physcpu=physcpu,
                            **cfg )
                    mk = ManagedProcess(cmd, stdout_handler=self._polarization_sensors[k]["mkrecv_sensors"].stdout_handler)
                else:
                    log.warning("Creating Dummy input instead of listening to network!")
                    cmd = "dada_junkdb -c 1 -R 1000 -t 3600 -k {dada_key} {mkrecv_header}".format(mkrecv_header=mkrecvheader_file.name,
                            **cfg )

                    mk = ManagedProcess(cmd)

                self.mkrec_cmd.append(mk)
                self._subprocessMonitor.add(mk, self._subprocess_error)

        except Exception as e:
            log.error("Error starting pipeline: {}".format(e))
            self.state = "error"
        else:
            self.state = "running"


    @coroutine
    def capture_stop(self):
        """@brief stop the dada_junkdb and dspsr instances."""
        log.info("Stoping EDD backend")
        if self.state != 'running':
            log.warning("pipleine state is not in state = running but in state {}".format(self.state))
            # return
        log.debug("Stopping")
        if self._subprocessMonitor is not None:
            self._subprocessMonitor.stop()

        # stop mkrec process
        log.debug("Stopping mkrecv processes ...")
        for proc in self.mkrec_cmd:
            proc.terminate()
        # This will terminate also the gated spectromenter automatically

        yield self.deconfigure()


    @coroutine
    def deconfigure(self):
        """@brief deconfigure the dspsr pipeline."""
        log.info("Deconfiguring EDD backend")
        if self.state == 'runnning':
            yield self.capture_stop()

        self.state = "deconfiguring"
        if self._subprocessMonitor is not None:
            self._subprocessMonitor.stop()
        for proc in self._subprocesses:
            proc.terminate()

        self.mkrec_cmd = []

        log.debug("Destroying dada buffers")
        for k in self._dada_buffers:
            k['monitor'].stop()
            cmd = "dada_db -d -k {0}".format(k['key'])
            log.debug("Running command: {0}".format(cmd))
            yield command_watcher(cmd)

        self._dada_buffers = []
        self.state = "idle"


@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


if __name__ == "__main__":
    launchPipelineServer(VLBIPipeline)
