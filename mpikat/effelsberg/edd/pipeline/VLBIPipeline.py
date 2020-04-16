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
from mpikat.utils.sensor_watchdog import SensorWatchdog
from mpikat.utils.db_monitor import DbMonitor
from mpikat.utils.mkrecv_stdout_parser import MkrecvSensors
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer, updateConfig, state_change
from mpikat.effelsberg.edd.EDDDataStore import EDDDataStore
import mpikat.utils.numa as numa

from tornado.gen import coroutine
from katcp import Sensor, AsyncReply, FailReply

from math import ceil
import numpy as np
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

# DADA BUFFERS TO BE USED
DADABUFFERS = ["dada", "dadc"]

DEFAULT_CONFIG = {

        "id": "VLBIPipeline",
        "type": "VLBIPipeline",
        "supported_input_formats": {"MPIFR_EDD_Packetizer": [1]},
        "samples_per_block": 512 * 1024 * 1024,

         "input_data_streams":
        {
            "polarization_0" :
            {
                "source": "",                               # name of the source for automatic setting of paramters
                "description": "",
                "format": "MPIFR_EDD_Packetizer:1",         # Format has version seperated via colon
                "ip": "225.0.0.152+3",
                "port": "7148",
                "bit_depth" : 8,
                "sample_rate" : 3200000000,
                "sync_time" : 1581164788.0,
                "samples_per_heap": 4096,                     # this needs to be consistent with the mkrecv configuration
            },
             "polarization_1" :
            {
                "source": "",                               # name of the source for automatic setting of paramters, e.g.: "packetizer1:h_polarization
                "description": "",
                "format": "MPIFR_EDD_Packetizer:1",
                "ip": "225.0.0.156+3",
                "port": "7148",
                "bit_depth" : 8,
                "sample_rate" : 3200000000,
                "sync_time" : 1581164788.0,
                "samples_per_heap": 4096,                           # this needs to be consistent with the mkrecv configuration
            }
        },

        "thread_id": {"polarization_0": "0", "polarization_1": "1"},
        "station_id": {"polarization_0": "0", "polarization_1": "1"},

        "null_output": False,                               # Disable sending of data for testing purposes
        "dummy_input": False,                               # Use dummy input instead of mkrecv process.
        "log_level": "debug",

        "output_directory": "/mnt",                         # ToDo: Should be a output data stream def.
        "output_type": 'network',                           # ['network', 'disk', 'null']  ToDo: Should be a output data stream def.
        "output_rate_factor": 1.10,                         # True output date rate is multiplied by this factor for sending.
        "idx1_modulo": "auto",
        "payload_size": "auto",                               # Size of the payload for one VDIF package, or 'auto' for automatic calculation
        "vdif_header_size": 32,                             # Size of the VDIF header in bytes 

        "output_data_streams":
        {
            "polarization_0" :
            {
                "format": "EDDVDIF:1",
                "ip": "225.0.0.123",
                "port": "8125",
            },
            "polarization_1" :
            {
                "format": "EDDVDIF:1",
                "ip": "225.0.0.124",
                "port": "8125",
            },
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

SAMPLE_CLOCK_START  0 # This is updated with the sync-time of the packetiser to allow for UTC conversion from the sample clock

DADA_NSLOTS         3


NTHREADS            32
NHEAPS              64
NGROUPS_TEMP        65536

#SPEAD specifcation for EDD packetiser data stream
NINDICES            1      # Although there is more than one index, we are only receiving one polarisation so only need to specify the time index

# The first index item is the running timestamp
IDX1_ITEM           0      # First item of a SPEAD heap

# Add side item to buffer
#SCI_LIST            2
"""


def EDD_VDIF_Frame_Size(sampling_rate):
    """
    payload calculator, translated from Jan Wagner's octave script
    """
    bw_GHz =  sampling_rate / 2E9

    rate_Msps = bw_GHz*2000
    rate_Mbps = 2*rate_Msps # % 2-bit
    log.debug('Bandwidth {:.3f} GHz --> {:.3f} Msps --> {:.3f} Mbit/sec'.format(bw_GHz, rate_Msps, rate_Mbps))

    vdifGranularity = 8 # % VDIF specs, granularity of payload size

    num = np.arange(1024, 9001, vdifGranularity) * 8*1000 #   % bits per frame, various payload sizes
    den = rate_Mbps #;                              % datarate bits per sec
    fitting_payloads = num[num % den == 0]/(8*1000);    # % corresponding frame payloads in byte

    rate_Bps = rate_Mbps*1e6/8 #;
    final_payloads = fitting_payloads[rate_Bps % fitting_payloads == 0] #;
    final_fpss = rate_Bps / final_payloads;
    final_framens = final_payloads*4*1e3 / rate_Msps;

    return final_payloads, final_fpss, final_framens



class VLBIPipeline(EDDPipeline):
    """@brief VLBI pipeline class."""
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")

    def __init__(self, ip, port):
        """@brief initialize the pipeline."""
        EDDPipeline.__init__(self, ip, port, DEFAULT_CONFIG)
        self._dada_buffers = []
        self.mkrec_cmd = []


    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        EDDPipeline.setup_sensors(self)

        self._integration_time_status = Sensor.float(
            "integration-time",
            description="Integration time [s]",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._integration_time_status)

        self._output_rate_status = Sensor.float(
            "output-rate",
            description="Output data rate [Gbyte/s]",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._output_rate_status)

        self._polarization_sensors = {}


    def add_input_stream_sensor(self, streamid):
        """
        @brief add sensors for i/o buffers for an input stream with given streamid.
        """
        self._polarization_sensors[streamid] = {}
        self._polarization_sensors[streamid]["mkrecv_sensors"] = MkrecvSensors(streamid)
        for s in self._polarization_sensors[streamid]["mkrecv_sensors"].sensors.itervalues():
            self.add_sensor(s)
        self._polarization_sensors[streamid]["input-buffer-fill-level"] = Sensor.float(
                "input-buffer-fill-level-{}".format(streamid),
                description="Fill level of the input buffer for polarization{}".format(streamid),
                params=[0, 1])
        self.add_sensor(self._polarization_sensors[streamid]["input-buffer-fill-level"])
        self._polarization_sensors[streamid]["input-buffer-total-write"] = Sensor.float(
                "input-buffer-total-write-{}".format(streamid),
                description="Total write into input buffer for polarization {}".format(streamid),
                params=[0, 1])

        self.add_sensor(self._polarization_sensors[streamid]["input-buffer-total-write"])
        self._polarization_sensors[streamid]["output-buffer-fill-level"] = Sensor.float(
                "output-buffer-fill-level-{}".format(streamid),
                description="Fill level of the output buffer for polarization {}".format(streamid)
                )
        self._polarization_sensors[streamid]["output-buffer-total-read"] = Sensor.float(
                "output-buffer-total-read-{}".format(streamid),
                description="Total read from output buffer for polarization {}".format(streamid)
                )
        self.add_sensor(self._polarization_sensors[streamid]["output-buffer-total-read"])
        self.add_sensor(self._polarization_sensors[streamid]["output-buffer-fill-level"])


    @coroutine
    def _create_ring_buffer(self, bufferSize, blocks, key, numa_node):
         """
         @brief Create a ring buffer of given size with given key on specified numa node.
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
        @brief Process a change in the buffer status
        """
        for streamid, stream_description in self._config["input_data_streams"].iteritems():
            if status['key'] == stream_description['dada_key']:
                self._polarization_sensors[streamid]["input-buffer-total-write"].set_value(status['written'])
                self._polarization_sensors[streamid]["input-buffer-fill-level"].set_value(status['fraction-full'])
        for streamid, stream_description in self._config["input_data_streams"].iteritems():
            if status['key'] == stream_description['dada_key'][::-1]:
                self._polarization_sensors[streamid]["output-buffer-fill-level"].set_value(status['fraction-full'])
                self._polarization_sensors[streamid]["output-buffer-total-read"].set_value(status['read'])


    @state_change(target="configured", allowed=["idle"], intermediate="configuring")
    @coroutine
    def configure(self, config_json):
        """
        @brief   Configure the EDD VLBi pipeline 

        @param   config_json    A JSON dictionary object containing configuration information

        """
        log.info("Configuring EDD backend for processing")
        log.debug("Configuration string: '{}'".format(config_json))

        yield self.set(config_json)

        cfs = json.dumps(self._config, indent=4)
        log.info("Final configuration:\n" + cfs)



        self.__numa_node_pool = []
        # remove numa nodes with missing capabilities
        for node in numa.getInfo():
            if len(numa.getInfo()[node]['gpus']) < 1:
                log.debug("Not enough gpus on numa node {} - removing from pool.".format(node))
                continue
            elif len(numa.getInfo()[node]['net_devices']) < 1:
                log.debug("Not enough nics on numa node {} - removing from pool.".format(node))
                continue
            else:
                self.__numa_node_pool.append(node)

        log.debug("{} numa nodes remaining in pool after cosntraints.".format(len(self.__numa_node_pool)))

        if len(self._config['input_data_streams']) > len(self.__numa_node_pool):
            raise FailReply("Not enough numa nodes to process {} polarizations!".format(len(self._config['input_data_streams'])))

        self._subprocessMonitor = SubprocessMonitor()
        #ToDo: Check that all input data streams have the same format, or allow different formats
        for i, streamid in enumerate(self._config['input_data_streams']):
            # calculate input buffer parameters
            stream_description = self._config['input_data_streams'][streamid]
            stream_description["dada_key"] = DADABUFFERS[i]
            self.add_input_stream_sensor(streamid)
            self.input_heapSize =  stream_description["samples_per_heap"] * stream_description['bit_depth'] / 8

            nHeaps = self._config["samples_per_block"] / stream_description["samples_per_heap"]
            input_bufferSize = nHeaps * (self.input_heapSize)
            log.info('Input dada parameters created from configuration:\n\
                    heap size:        {} byte\n\
                    heaps per block:  {}\n\
                    buffer size:      {} byte'.format(self.input_heapSize, nHeaps, input_bufferSize))


            final_payloads, final_fpss, final_framens = EDD_VDIF_Frame_Size(stream_description['sample_rate'])

            if self._config['payload_size'] == 'auto':
                payload_size = final_payloads[-1]
            else:
                payload_size = int(self._config['payload_size'])

            log.info('Possible frame payload sizes (add 32 for framesize):')
            for k in range(final_payloads.size):
                if payload_size == final_payloads[k]:
                    M = "*"
                else:
                    M = " "
                log.info(' {}{:5.0f} byte  {:8.0f} frames per sec  {:6.3f} nsec/frame'.format(M, final_payloads[k], final_fpss[k], final_framens[k]))

            if payload_size not in final_payloads:
                log.warning("Payload size {} possibly not conform with VDIF format!".format(payload_size))

            # calculate output buffer parameters
            size_of_samples = ceil(1. * self._config["samples_per_block"] * 2 / 8.) # byte for two bit mode
            number_of_packages = ceil(size_of_samples / float(payload_size))

            output_buffer_size = number_of_packages * (payload_size + self._config['vdif_header_size'])

            integration_time = self._config["samples_per_block"] / float(stream_description["sample_rate"])
            self._integration_time_status.set_value(integration_time)

            rate = output_buffer_size/ integration_time # in spead documentation BYTE per second and not bit!
            rate *= self._config["output_rate_factor"]        # set rate to (100+X)% of expected rate
            self._output_rate_status.set_value(rate / 1E9)

            log.info('Output parameters calculated from configuration:\n\
                total size of data samples:        {} byte\n\
                number_of_packages:  {}\n\
                size of output buffer:      {} byte\n\
                rate ({:.0f}%):        {} Gbps'.format(size_of_samples,
                    number_of_packages, output_buffer_size,
                    self._config["output_rate_factor"]*100, rate / 1E9))

            numa_node = self.__numa_node_pool[i]
            log.debug("Associating {} with numa node {}".format(streamid, numa_node))

            # configure dada buffer
            bufferName = stream_description['dada_key']
            yield self._create_ring_buffer(input_bufferSize, 64, bufferName, numa_node)

            ofname = bufferName[::-1]
            # we write nSlice blocks on each go
            yield self._create_ring_buffer(output_buffer_size, 8, ofname, numa_node)

            # Configure + launch 
            physcpu = numa.getInfo()[numa_node]['cores'][0]
            thread_id = self._config['thread_id'][streamid]
            station_id = self._config['thread_id'][streamid]
            cmd = "taskset -c {physcpu} VLBI --input_key={dada_key} --speadheap_size={heapSize} --thread_id={thread_id} --station_id={station_id} --payload_size={payload_size} --sample_rate={sample_rate} --nbits={bit_depth} -o {ofname} --log_level={log_level} --output_type=dada".format(ofname=ofname, heapSize=self.input_heapSize, numa_node=numa_node, physcpu=physcpu, thread_id=thread_id, station_id=station_id, payload_size=payload_size, log_level=self._config['log_level'], **stream_description)
            log.debug("Command to run: {}".format(cmd))

            cudaDevice = numa.getInfo()[numa_node]['gpus'][0]
            cli = ManagedProcess(cmd, env={"CUDA_VISIBLE_DEVICES": cudaDevice})
            self._subprocessMonitor.add(cli, self._subprocess_error)
            self._subprocesses.append(cli)

            cfg = self._config.copy()
            cfg.update(stream_description)

            ip_range = []
            port = set()
            for key in self._config["output_data_streams"]:
                if streamid in key:
                    ip_range.append(self._config["output_data_streams"][key]['ip'])
                    port.add(self._config["output_data_streams"][key]['port'])
            if len(port)!=1:
                raise FailReply("Output data for one plarization has to be on the same port! ")

            if self._config["output_type"] == 'network':
                physcpu = ",".join(numa.getInfo()[numa_node]['cores'][1:2])
                fastest_nic, nic_params = numa.getFastestNic(numa_node)
                log.info("Sending data for {} on NIC {} [ {} ] @ {} Mbit/s".format(streamid, fastest_nic, nic_params['ip'], nic_params['speed']))

                cmd = "taskset -c {physcpu} vdif_send --input_key {ofname} --if_ip {ibv_if} --dest_ip {mcast_dest} --port {port_tx} --max_rate {rate}".format(ofname=ofname, 
                        physcpu=physcpu, ibv_if=nic_params['ip'], mcast_dest=" ".join(ip_range), port_tx=port.pop(), rate=rate)
                log.debug("Command to run: {}".format(cmd))

            elif self._config["output_type"] == 'disk':
                ofpath = os.path.join(cfg["output_directory"], ofname)
                log.debug("Writing output to {}".format(ofpath))
                if not os.path.isdir(ofpath):
                    os.makedirs(ofpath)
                cmd = "dada_dbdisk -k {ofname} -D {ofpath} -W".format(ofname=ofname, ofpath=ofpath, **cfg)
            else:
                log.warning("Selected null output. Not sending data!")
                cmd = "dada_dbnull -z -k {}".format(ofname)

            log.debug("Command to run: {}".format(cmd))
            mks = ManagedProcess(cmd, env={"CUDA_VISIBLE_DEVICES": cudaDevice})
            self._subprocessMonitor.add(mks, self._subprocess_error)
            self._subprocesses.append(mks)

        self._subprocessMonitor.start()


    @state_change(target="streaming", allowed=["configured"], intermediate="capture_starting")
    @coroutine
    def capture_start(self, config_json=""):
        """
        @brief start streaming output
        """
        log.info("Starting EDD backend")
        try:
            for i, streamid in enumerate(self._config['input_data_streams']):
                stream_description = self._config['input_data_streams'][streamid]
                mkrecvheader_file = tempfile.NamedTemporaryFile(delete=False)
                log.debug("Creating mkrec header file: {}".format(mkrecvheader_file.name))
                mkrecvheader_file.write(mkrecv_header)
                # DADA may need this
                # ToDo: Check for input stream definitions
                mkrecvheader_file.write("NBIT {}\n".format(stream_description["bit_depth"]))
                mkrecvheader_file.write("HEAP_SIZE {}\n".format(self.input_heapSize))

                mkrecvheader_file.write("\n#OTHER PARAMETERS\n")
                mkrecvheader_file.write("samples_per_block {}\n".format(self._config["samples_per_block"]))

                mkrecvheader_file.write("\n#PARAMETERS ADDED AUTOMATICALLY BY MKRECV\n")
                mkrecvheader_file.close()

                cfg = self._config.copy()
                cfg.update(stream_description)
                if not self._config['dummy_input']:
                    numa_node = self.__numa_node_pool[i]
                    fastest_nic, nic_params = numa.getFastestNic(numa_node)
                    log.info("Receiving data for {} on NIC {} [ {} ] @ {} Mbit/s".format(streamid, fastest_nic, nic_params['ip'], nic_params['speed']))
                    physcpu = ",".join(numa.getInfo()[numa_node]['cores'][2:7])
                    if self._config['idx1_modulo'] == 'auto':
                        idx1modulo = 10*cfg["samples_per_block"] / stream_description["samples_per_heap"]
                    else:
                        idx1modulo = self._config['idx1_modulo']

                    cmd = "taskset -c {physcpu} mkrecv_rnt --quiet --header {mkrecv_header} --idx1-step {samples_per_heap} --heap-size {input_heap_size} --idx1-modulo {idx1modulo} \
                    --dada-key {dada_key} --sync-epoch {sync_time} --sample-clock {sample_rate} \
                    --ibv-if {ibv_if} --port {port} {ip}".format(mkrecv_header=mkrecvheader_file.name, physcpu=physcpu,ibv_if=nic_params['ip'], input_heap_size=self.input_heapSize, idx1modulo=idx1modulo,
                            **cfg )
                    mk = ManagedProcess(cmd, stdout_handler=self._polarization_sensors[streamid]["mkrecv_sensors"].stdout_handler)
                else:
                    log.warning("Creating Dummy input instead of listening to network!")
                    cmd = "dada_junkdb -c 1 -R 1000 -t 3600 -k {dada_key} {mkrecv_header}".format(mkrecv_header=mkrecvheader_file.name,
                            **cfg )

                    mk = ManagedProcess(cmd)

                self.mkrec_cmd.append(mk)
                self._subprocessMonitor.add(mk, self._subprocess_error)

        except Exception as E:
            log.error("Error starting pipeline: {}".format(E))
            raise E
        else:
            self.__watchdogs = []
            for i, k in enumerate(self._config['input_data_streams']):
                wd = SensorWatchdog(self._polarization_sensors[streamid]["input-buffer-total-write"],
                        10 * self._integration_time_status.value(),
                        self.watchdog_error)
                wd.start()
                self.__watchdogs.append(wd)


    @state_change(target="idle", allowed=["streaming"], intermediate="capture_stopping")
    @coroutine
    def capture_stop(self):
        """
        @brief Stop streaming of data
        """
        log.info("Stoping EDD backend")
        for wd in self.__watchdogs:
            wd.stop_event.set()
            yield
        if self._subprocessMonitor is not None:
            self._subprocessMonitor.stop()
            yield

        # stop mkrec process
        log.debug("Stopping mkrecv processes ...")
        for proc in self.mkrec_cmd:
            proc.terminate()
            yield
        # This will terminate also the gated spectromenter automatically

        yield self.deconfigure()


    @state_change(target="idle", intermediate="deconfiguring", error='panic')
    @coroutine
    def deconfigure(self):
        """
        @brief deconfigure the pipeline.
        """
        log.info("Deconfiguring EDD backend")
        if self.previous_state == 'streaming':
            yield self.capture_stop()

        if self._subprocessMonitor is not None:
            yield self._subprocessMonitor.stop()
        for proc in self._subprocesses:
            yield proc.terminate()

        self.mkrec_cmd = []

        log.debug("Destroying dada buffers")
        for k in self._dada_buffers:
            k['monitor'].stop()
            cmd = "dada_db -d -k {0}".format(k['key'])
            log.debug("Running command: {0}".format(cmd))
            yield command_watcher(cmd)

        self._dada_buffers = []

    @coroutine
    def populate_data_store(self, host, port):
        """@brief Populate the data store"""
        log.debug("Populate data store @ {}:{}".format(host, port))
        dataStore =  EDDDataStore(host, port)
        log.debug("Adding output formats to known data formats")

        descr = {"description":"VDIF data stream",
                "ip": None,
                "port": None,
                }
        dataStore.addDataFormatDefinition("VDIF:1", descr)



if __name__ == "__main__":
    launchPipelineServer(VLBIPipeline)
