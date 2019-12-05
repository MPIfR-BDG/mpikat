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
from mpikat.effelsberg.edd.pipeline.EDDPipeline import EDDPipeline, launchPipelineServer
import mpikat.utils.numa as numa

from tornado.gen import coroutine
from katcp import Sensor, AsyncReply, FailReply

import os
import time
import logging
import signal
from optparse import OptionParser
import coloredlogs
import json
import tempfile

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.GatedSpectrometerPipeline")
log.setLevel('DEBUG')

# DADA BUFFERS TO BE USED 
DADABUFFERS = ["dada", "dadc"]


#class DadaBuffer:
#    def __init__(self, nblocks, blocksize):
#        """
#        Interface to a dadabuffer with given number of blocks and blocksize.
#        The dada buffer is created with an automatically choosen key starting with
#        dada, dadb, dadc, ....
#        The buffer is deleted on destruction of the object
#        """
#
#        self.__key = getFreeBufferKey()
#        self.nblocks = nblocks
#        self.blocksize = blcoksize
#    
#    def allocate(self):
#
#
#
#    @classmethod
#    def getFreeBufferKey(cls):
#        key = int('dada', 16)
#        while key < key+1024:
#            p = subprocess.Popen(['dada_dbmeminfo', '-k', hex(key)[2:]], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
#            if p.wait():
#                return key
#            key += 1
#        raise RuntimeError("No dada key available!")
#
#
#    @property
#    def key(self):
#        return hex(self.__key)[:2]
#




DEFAULT_CONFIG = {
	    "id": "GatedSpectrometer",                          # default cfgs for master controler. Needs to get a unique ID -- TODO, from ansible
    	"type": "GatedSpectrometer",
        "supported_input_formats": {"MPIFR_EDD_Packetizer": [1]},      # supproted input formats name:version
	    "address": "dummy",                                 # ToDo, address is not a configuration option but found automatically
        "samples_per_block": 256 * 1024 * 1024,             # 256 Mega sampels per buffer block to allow high res  spectra - the
                                                            # theoretical  mazimum is thus  128 M Channels.   This option  allows
                                                            # to tweak  the execution on  low-mem GPUs or  ig the GPU is  shared 
                                                            # with other  codes
        "input_data_streams":
        {
            "polarization_0" :
            {
                "source": "",                               # name of the source for automatic setting of paramters
                "format": "MPIFR_EDD_Packetizer",
                "format_version": 1,
                "ip": "225.0.0.162+3",
                "port": "7148",
                "bit_depth" : 12,
                "sample_rate" : 2600000000,
                "sync_time" : 1562662573.0,
                "samples_per_heap": 4096,                     # this needs to be consistent with the mkrecv configuration
            },
             "polarization_1" :
            {
                "source": "",                               # name of the source for automatic setting of paramters, e.g.: "packetizer1:h_polarization
                "format": "MPIFR_EDD_Packetizer",
                "format_version": 1,
                "ip": "225.0.0.1r6+3",
                "port": "7148",
                "bit_depth" : 12,
                "sample_rate" : 2600000000,
                "sync_time" : 1562662573.0,
                "samples_per_heap": 4096,                           # this needs to be consistent with the mkrecv configuration
            }
        },
        "output_data_streams":
        {
            "polarization_0" :
            {
                "format": "MPIFR_EDD_GatedSpectrometer",
                "format_version": 1,
                "ip": "225.0.0.172 225.0.0.173",        #two destinations gate on/off
                "port": "7152",
            },
             "polarization_1" :
            {
                "format": "MPIFR_EDD_GatedSpectrometer",
                "format_version": 1,
                "ip": "225.0.0.184 225.0.0.185",        #two destinations, one for on, one for off
                "port": "7152",
            }
        },

        "fft_length": 1024 * 1024 * 2 * 8,
        "naccumulate": 32,
        "output_bit_depth": 32,

        "input_level": 100,
        "output_level": 100,

        "output_directory": "/mnt",                         # ToDo: Should be a output data stream def.
        "output_type": 'network',                           # ['network', 'disk', 'null']  ToDo: Should be a output data stream def.
        "dummy_input": False,                               # Use dummy input instead of mkrecv process. Should be input data stream option.
        "log_level": "debug",

        "output_rate_factor": 1.10,                         # True output date rate is multiplied by this factor for sending.
    }

NON_EXPERT_KEYS = ["fft_length", "naccumulate", "output_bit_depth"]

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
# Modulo to create  a delay to syncronize multiple mkrecv instances - should be
# a prime of heaps to wait
IDX1_MODULO         118751 # approx 0.2 s @ 2.6GHz  # other primes: 149993, 349403, 454199, 799529, 1044149

# Add side item to buffer
SCI_LIST            2
"""

# static configuration for mksend. all items that can be configured are passed
# via cmdline
mksend_header = """
HEADER          DADA
HDR_VERSION     1.0
HDR_SIZE        4096
DADA_VERSION    1.0

# MKSEND CONFIG
NETWORK_MODE  1
PACKET_SIZE 8400
IBV_VECTOR   -1          # IBV forced into polling mode
IBV_MAX_POLL 10

SYNC_TIME           unset  # Default value from mksend manual
SAMPLE_CLOCK        unset  # Default value from mksend manual
SAMPLE_CLOCK_START  0      # Default value from mksend manual
UTC_START           unset  # Default value from mksend manual

#number of heaps with the same time stamp.
HEAP_COUNT 1
HEAP_ID_OFFSET  1
HEAP_ID_STEP    13

NSCI            1
NITEMS          9
ITEM1_ID        5632    # timestamp, slowest index

ITEM2_ID        5633    # polarization

ITEM3_ID        5634    # noise diode status
ITEM3_LIST      0,1
ITEM3_INDEX     2

ITEM4_ID        5635    # fft_length

ITEM5_ID        5636
ITEM5_SCI       1       # number of input heaps with ndiode on/off

ITEM6_ID        5637    # sync_time

ITEM7_ID        5638    # sampling rate
ITEM8_ID        5639    # naccumulate

ITEM9_ID        5640    # payload item (empty step, list, index and sci)
"""


@register_pipeline("GatedSpectrometerPipeline")
class GatedSpectrometerPipeline(EDDPipeline):
    """@brief gated spectrometer pipeline class."""
    VERSION_INFO = ("mpikat-edd-api", 0, 1)
    BUILD_INFO = ("mpikat-edd-implementation", 0, 1, "rc1")


    def __init__(self, ip, port):
        """@brief initialize the pipeline."""
        EDDPipeline.__init__(self, ip, port)
        self.__numa_node_pool = []
        self.mkrec_cmd = []
        self._dada_buffers = []


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
        pass
        for streamid, stream_description in self._config["input_data_streams"].iteritems():
            if status['key'] == stream_description['dada_key']:
                self._polarization_sensors[streamid]["input-buffer-total-write"].set_value(status['written'])
                self._polarization_sensors[streamid]["input-buffer-fill-level"].set_value(status['fraction-full'])
        for streamid, stream_description in self._config["input_data_streams"].iteritems():
            if status['key'] == stream_description['dada_key'][::-1]:
                self._polarization_sensors[streamid]["output-buffer-fill-level"].set_value(status['fraction-full'])
                self._polarization_sensors[streamid]["output-buffer-total-read"].set_value(status['read'])


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

        #ToDo: Check that all input data streams have the same format, or allow different formats
        for i, streamid in enumerate(self._config['input_data_streams']):
            # calculate input buffer parameters
            stream_description = self._config['input_data_streams'][streamid]
            stream_description["dada_key"] = DADABUFFERS[i]
            self.add_input_stream_sensor(streamid)
            self.input_heapSize =  stream_description["samples_per_heap"] * stream_description['bit_depth'] / 8

            nHeaps = self._config["samples_per_block"] / stream_description["samples_per_heap"]
            input_bufferSize = nHeaps * (self.input_heapSize + 64 / 8)
            log.info('Input dada parameters created from configuration:\n\
                    heap size:        {} byte\n\
                    heaps per block:  {}\n\
                    buffer size:      {} byte'.format(self.input_heapSize, nHeaps, input_bufferSize))

            # calculate output buffer parameters
            nSlices = max(self._config["samples_per_block"] / self._config['fft_length'] /  self._config['naccumulate'], 1)
            nChannels = self._config['fft_length'] / 2 + 1
            # on / off spectrum  + one side channel item per spectrum
            output_bufferSize = nSlices * (2 * nChannels * self._config['output_bit_depth'] / 8 + 2 * 8)

            output_heapSize = nChannels * self._config['output_bit_depth'] / 8
            integrationTime = self._config['fft_length'] * self._config['naccumulate']  / float(stream_description["sample_rate"])
            self._integration_time_status.set_value(integrationTime)
            rate = output_heapSize / integrationTime # in spead documentation BYTE per second and not bit!
            rate *= self._config["output_rate_factor"]        # set rate to (100+X)% of expected rate
            self._output_rate_status.set_value(rate / 1E9)

            log.info('Output parameters calculated from configuration:\n\
                    spectra per block:  {} \n\
                    nChannels:          {} \n\
                    buffer size:        {} byte \n\
                    integrationTime :   {} s \n\
                    heap size:          {} byte\n\
                    rate ({:.0f}%):        {} Gbps'.format(nSlices, nChannels, output_bufferSize, integrationTime, output_heapSize, self._config["output_rate_factor"]*100, rate / 1E9))
            self._subprocessMonitor = SubprocessMonitor()

            numa_node = self.__numa_node_pool[i] 
            log.debug("Associating {} with numa node {}".format(streamid, numa_node))

            # configure dada buffer
            bufferName = stream_description['dada_key']
            yield self._create_ring_buffer(input_bufferSize, 64, bufferName, numa_node)

            ofname = bufferName[::-1]
            # we write nSlice blocks on each go
            yield self._create_ring_buffer(output_bufferSize, 8 * nSlices, ofname, numa_node)

            # Configure + launch gated spectrometer
            # here should be a smarter system to parse the options from the
            # controller to the program without redundant typing of options
            physcpu = numa.getInfo()[numa_node]['cores'][0]
            cmd = "taskset -c {physcpu} gated_spectrometer --nsidechannelitems=1 --input_key={dada_key} --speadheap_size={heapSize} --selected_sidechannel=0 --nbits={bit_depth} --fft_length={fft_length} --naccumulate={naccumulate} --input_level={input_level} --output_bit_depth={output_bit_depth} --output_level={output_level} -o {ofname} --log_level={log_level} --output_type=dada".format(dada_key=bufferName, ofname=ofname, heapSize=self.input_heapSize, numa_node=numa_node, physcpu=physcpu, bit_depth=stream_description['bit_depth'], **self._config)
            log.debug("Command to run: {}".format(cmd))


            cudaDevice =numa.getInfo()[numa_node]['gpus'][0]
            gated_cli = ManagedProcess(cmd, env={"CUDA_VISIBLE_DEVICES": cudaDevice})
            self._subprocessMonitor.add(gated_cli, self._subprocess_error)
            self._subprocesses.append(gated_cli)

            cfg = self._config.copy()
            cfg.update(stream_description)

            if self._config["output_type"] == 'network':
                mksend_header_file = tempfile.NamedTemporaryFile(delete=False)
                mksend_header_file.write(mksend_header)
                mksend_header_file.close()

                nhops = len(self._config["output_data_streams"][streamid]['ip'].split())

                timestep = cfg["fft_length"] * cfg["naccumulate"]
                physcpu = ",".join(numa.getInfo()[numa_node]['cores'][1:2])
                #select network interface
                fastest_nic, nic_params = nume.getFastestNic(numa_node)

                log.info("Sending data for {} on NIC {} [ {} ] @ {} Mbit/s".format(streamid, fastest_nic, nic_params['ip'], nic_params['speed']))
                cmd = "taskset -c {physcpu} mksend --header {mksend_header} --heap-id-start {heap_id_start} --dada-key {ofname} --ibv-if {ibv_if} --port {port_tx} --sync-epoch {sync_time} --sample-clock {sample_rate} --item1-step {timestep} --item2-list {polarization} --item4-list {fft_length} --item6-list {sync_time} --item7-list {sample_rate} --item8-list {naccumulate} --rate {rate} --heap-size {heap_size} --nhops {nhops} {mcast_dest}".format(mksend_header=mksend_header_file.name, heap_id_start=i , timestep=timestep,
                        ofname=ofname, polarization=i, nChannels=nChannels, physcpu=physcpu, integrationTime=integrationTime,
                        rate=rate, nhops=nhops, heap_size=output_heapSize, ibv_if=nic_params['ip'],
                        mcast_dest=self._config["output_data_streams"][streamid]['ip'],
                        port_tx=self._config["output_data_streams"][streamid]['port'], **cfg)
                log.debug("Command to run: {}".format(cmd))

            elif self._config["output_type"] == 'disk':
		ofpath = os.path.join(cfg["output_directory"], ofname)
                log.debug("Writing output to {}".format(ofpath))
                if not os.path.isdir(ofpath):
                    os.mkdir(ofpath)
                cmd = "dada_dbdisk -k {ofname} -D {ofpath} -W".format(ofname=ofname, ofpath=ofpath, **cfg)
            else:
                log.warning("Selected null output. Not sending data!")
                cmd = "dada_dbnull -z -k {}".format(ofname)

            log.debug("Command to run: {}".format(cmd))
            mks = ManagedProcess(cmd)
            self._subprocessMonitor.add(mks, self._subprocess_error)
            self._subprocesses.append(mks)

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
                    fastest_nic, nic_params = nume.getFastestNic(numa_node)
                    log.info("Receiving data for {} on NIC {} [ {} ] @ {} Mbit/s".format(streamid, fastest_nic, nic_params['ip'], nic_params['speed']))
                    physcpu = ",".join(numa.getInfo()[numa_node]['cores'][2:7])
                    cmd = "taskset -c {physcpu} mkrecv_nt --quiet --header {mkrecv_header} --idx1-step {samples_per_heap} --dada-key {dada_key} \
                    --sync-epoch {sync_time} --sample-clock {sample_rate} \
                    --ibv-if {ibv_if} --port {port} {ip}".format(mkrecv_header=mkrecvheader_file.name, physcpu=physcpu,ibv_if=nic_params['ip'], 
                            **cfg )
                    mk = ManagedProcess(cmd, stdout_handler=self._polarization_sensors[streamid]["mkrecv_sensors"].stdout_handler)
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
            self.__watchdogs = []
            for i, k in enumerate(self._config['input_data_streams']):
                wd = SensorWatchdog(self._polarization_sensors[streamid]["input-buffer-total-write"],
                        10 * self._integration_time_status.value(),
                        self.watchdog_error)
                wd.start()
                self.__watchdogs.append(wd)


    @coroutine
    def capture_stop(self):
        """@brief stop the dada_junkdb and dspsr instances."""
        log.info("Stoping EDD backend")
        if self.state != 'running':
            log.warning("pipleine state is not in state = running but in state {}".format(self.state))
            # return
        log.debug("Stopping")
        for wd in self.__watchdogs:
            wd.stop_event.set()
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


if __name__ == "__main__":
    launchPipelineServer(GatedSpectrometerPipeline)
