import logging
import json
from subprocess import Popen, PIPE
from tornado.gen import coroutine
from tornado.ioloop import IOLoop, PeriodicCallback
from katpoint import Target
from katcp import Sensor
from mpikat.meerkat.apsuse.apsuse_mkrecv_config import (
    make_mkrecv_header, MkrecvStdoutHandler)
from mpikat.utils.process_tools import process_watcher, ManagedProcess
from mpikat.utils.db_monitor import DbMonitor
from mpikat.utils.unix_socket import UDSClient

AVAILABLE_CAPTURE_MEMORY = 3221225472  * 10
MAX_DADA_BLOCK_SIZE = 3221225472
log = logging.getLogger("mpkat.apsuse_capture")


class ApsCapture(object):
    def __init__(self, capture_interface, control_socket,
                 mkrecv_config_filename, mkrecv_cpu_set,
                 apsuse_cpu_set, sensor_prefix, dada_key):
        self._capture_interface = capture_interface
        self._control_socket = control_socket
        self._mkrecv_config_filename = mkrecv_config_filename
        self._mkrecv_cpu_set = mkrecv_cpu_set
        self._apsuse_cpu_set = apsuse_cpu_set
        self._sensor_prefix = sensor_prefix
        self._dada_input_key = dada_key
        self._mkrecv_proc = None
        self._apsuse_proc = None
        self._ingress_buffer_monitor = None
        self._internal_beam_mapping = {}
        self._sensors = []
        self._capturing = False
        self.setup_sensors()

    def add_sensor(self, sensor):
        sensor.name = "{}-{}".format(self._sensor_prefix, sensor.name)
        self._sensors.append(sensor)

    def setup_sensors(self):
        self._config_sensor = Sensor.string(
            "configuration",
            description="The current configuration of the capture instance",
            default="",
            initial_status=Sensor.UNKNOWN
            )
        self.add_sensor(self._config_sensor)

        self._mkrecv_header_sensor = Sensor.string(
            "mkrecv-capture-header",
            description="The MKRECV/DADA header used for configuring capture with MKRECV",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._mkrecv_header_sensor)

        self._apsuse_args_sensor = Sensor.string(
            "apsuse-arguments",
            description="The command line arguments used to invoke apsuse",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._apsuse_args_sensor)

        self._mkrecv_heap_loss = Sensor.float(
            "fbf-heap-loss",
            description=("The percentage of FBFUSE heaps lost "
                         "(within MKRECV statistics window)"),
            default=0.0,
            initial_status=Sensor.UNKNOWN,
            unit="%")
        self.add_sensor(self._mkrecv_heap_loss)

        self._ingress_buffer_percentage = Sensor.float(
            "ingress-buffer-fill-level",
            description=("The percentage fill level for the capture"
                         "buffer between MKRECV and APSUSE"),
            default=0.0,
            initial_status=Sensor.UNKNOWN,
            unit="%")
        self.add_sensor(self._ingress_buffer_percentage)

    @coroutine
    def _make_db(self, key, block_size, nblocks, timeout=120):
        try:
            yield self._destroy_db(key, timeout=20)
        except Exception as error:
            log.debug("Could not clean previous buffer (key={}): {}".format(
                key, str(error)))
        log.debug(("Building DADA buffer: key={}, block_size={}, "
                   "nblocks={}").format(key, block_size, nblocks))


        cmdline = ["sleep", "2"]

        #cmdline = map(str, ["dada_db", "-k", key, "-b", block_size, "-n",
        #                    nblocks, "-l", "-p"])
        proc = Popen(
            cmdline, stdout=PIPE,
            stderr=PIPE, shell=False,
            close_fds=True)
        yield process_watcher(
            proc, name="make_db({})".format(key),
            timeout=timeout)

    @coroutine
    def _destroy_db(self, key, timeout=20.0):
        log.debug("Destroying DADA buffer with key={}".format(key))
        #cmdline = map(str, ["dada_db", "-k", key, "-d"])
        cmdline = ["sleep", "1"]
        proc = Popen(
            cmdline, stdout=PIPE,
            stderr=PIPE, shell=False,
            close_fds=True)
        yield process_watcher(
            proc, name="destroy_db({})".format(key),
            timeout=timeout)

    @coroutine
    def capture_start(self, config):
        log.info("Preparing apsuse capture instance")
        log.info("Config: {}".format(config))
        nbeams = len(config['beam-ids'])
        npartitions = config['nchans'] / config['nchans-per-heap']
        # desired beams here is a list of beam IDs, e.g. [1,2,3,4,5]
        heap_group_size = config['heap-size'] * nbeams * npartitions
        ngroups_data = int(MAX_DADA_BLOCK_SIZE / heap_group_size)
        ngroups_data = 2**((ngroups_data-1).bit_length())
        nheap_groups = ngroups_data / heap_group_size

        # Make DADA buffer and start watchers
        log.info("Creating capture buffer")
        capture_block_size = ngroups_data * heap_group_size
        capture_block_count = int(AVAILABLE_CAPTURE_MEMORY / capture_block_size)
        log.debug("Creating dada buffer for input with key '{}'".format(
            "%s" % self._dada_input_key))
        yield self._make_db(self._dada_input_key, capture_block_size,
            capture_block_count)
        self._config_sensor.set_value(json.dumps(config))
        idx = 0
        for beam in config['beam-ids']:
            self._internal_beam_mapping[beam] = idx
            idx =+ 1

        """
        Config takes the following form:
        {
            "mcast-groups": ["239.11.1.0"],
            "mcast-port": 7147,
            "stream-indices":[12,13,14,15,16,17],
            "beam-ids": ["cfbf00012", "cfbf00013", ...]
            "nchans": 1024,
            "nchans-per-heap": 16,
            "bandwidth": 856e6,
            "centre-frequency": 1284e6,
            "sampling-interval": 306e-6,
            "heap-size": 8192,
            "filesize": 10e9,
            "sync-epoch": 1232352352.0,
            "sample-clock": 1712000000.0,
            "idx1-step": 268435456,
            "base-output-dir"
        }
        """

        # Start APSUSE processing code
        apsuse_cmdline = [
            #"taskset", "-c", self._apsuse_cpu_set,
            "apsuse",
            "--input_key", self._dada_input_key,
            "--ngroups", nheap_groups,
            "--nbeams", nbeams,
            "--nchannels", config['nchans-per-heap'],
            "--nsamples", config['heap-size']/config['nchans-per-heap'],
            "--nfreq", npartitions,
            "--size", int(config['filesize']),
            "--socket", self._control_socket,
            "--dir", config["base-output-dir"],
            "--log_level", "debug"]
        log.debug(" ".join(map(str, apsuse_cmdline)))
        apsuse_cmdline = ["sleep", "1000"]
        self._apsuse_proc = ManagedProcess(
            apsuse_cmdline, stdout_handler=log.debug, stderr_handler=log.error)
        self._apsuse_args_sensor.set_value(" ".join(map(str, apsuse_cmdline)))

        # Start MKRECV capture code
        mkrecv_config = {
                'dada_mode': 4,
                'dada_key': self._dada_input_key,
                'bandwidth': config['bandwidth'],
                'centre_frequency': config['centre-frequency'],
                'nchannels': config["nchans"],
                'sampling_interval': config['sampling-interval'],
                'sync_epoch': config['sync-epoch'],
                'sample_clock': config['sample-clock'],
                'mcast_sources': config['mcast-groups'],
                'mcast_port': str(config['mcast-port']),
                'interface': self._capture_interface,
                'timestamp_step': config['idx1-step'],
                'timestamp_modulus': 1,
                'beam_ids_csv': ",".join(map(str, config['stream-indices'])),
                'freq_ids_csv': "0:{}:{}".format(config['nchans'], config['nchans-per-heap']),
                'ngroups_data': ngroups_data,
                'heap_size': config['heap-size']
            }
        mkrecv_header = make_mkrecv_header(
            mkrecv_config, outfile=self._mkrecv_config_filename)
        log.info("Determined MKRECV configuration:\n{}".format(
            mkrecv_header))
        self._mkrecv_header_sensor.set_value(mkrecv_header)

        def update_heap_loss_sensor(curr, total, avg, window):
            self._mkrecv_heap_loss.set_value(100.0 - avg)

        mkrecv_sensor_updater = MkrecvStdoutHandler(
            callback=update_heap_loss_sensor)

        def mkrecv_aggregated_output_handler(line):
            log.debug(line)
            mkrecv_sensor_updater(line)

        #self._mkrecv_proc = ManagedProcess(
        #    ["mkrecv_nt", "--header",
        #     self._mkrecv_config_filename, "--quiet"],
        #    stdout_handler=mkrecv_aggregated_output_handler,
        #    stderr_handler=log.error)

        self._mkrecv_proc = ManagedProcess(
            ["sleep", "1000"])

        def exit_check_callback():
            if not self._mkrecv_proc.is_alive():
                log.error("mkrecv_nt exited unexpectedly")
                self.ioloop.add_callback(self.capture_stop)
            elif not self._apsuse_proc.is_alive():
                log.error("apsuse pipeline exited unexpectedly")
                self.ioloop.add_callback(self.capture_stop)
            self._capture_monitor.stop()

        self._capture_monitor = PeriodicCallback(exit_check_callback, 1000)
        self._capture_monitor.start()

        """
        self._ingress_buffer_monitor = DbMonitor(
            self._dada_input_key,
            callback=lambda params:
            self._ingress_buffer_percentage.set_value(params["fraction-full"]))
        self._ingress_buffer_monitor.start()
        """

        self._capturing = True

    def target_start(self, beam_info):
        # Send target information to apsuse pipeline
        # and trigger file writing

        # First build message containing beam information
        # in JSON form:
        #
        # {
        # "command":"start",
        # "beam_parameters": [
        #     {id: "cfbf00000", name: "PSRJ1823+3410", "ra": "00:00:00.00", "dec": "00:00:00.00"},
        #     {id: "cfbf00002", name: "SBGS0000", "ra": "00:00:00.00", "dec": "00:00:00.00"},
        #     {id: "cfbf00006", name: "SBGS0000", "ra": "00:00:00.00", "dec": "00:00:00.00"},
        #     {id: "cfbf00008", name: "SBGS0000", "ra": "00:00:00.00", "dec": "00:00:00.00"},
        #     {id: "cfbf00010", name: "SBGS0000", "ra": "00:00:00.00", "dec": "00:00:00.00"}
        # ]
        # }
        #
        # Here the "idx" parameter refers to the internal index of the beam, e.g. if the
        # apsuse executable is handling 6 beams these are numbered 0-5 regardless of their
        # global index. It is thus necessary to track the mapping between internal and
        # external indices for these beams.
        #
        beam_params = []
        message_dict = {"command": "start", "beam_parameters": beam_params}
        for beam, target_str in beam_info.items():
            if beam in self._internal_beam_mapping:
                idx = self._internal_beam_mapping[beam]
                target = Target(target_str)
                ra, dec = map(str, target.radec())
                beam_params.append({
                        "idx": idx,
                        "name": beam,
                        "source": target.name,
                        "ra": ra,
                        "dec": dec
                    })
        client = UDSClient(self._control_socket)
        client.send(json.dumps(message_dict))
        response_str = client.recv(timeout=3)
        try:
            response = json.loads(response_str)["response"]
        except Exception:
            log.exception("Unable to parse JSON returned from apsuse application")
        else:
            if response != "success":
                raise Exception("Failed to start APSUSE recording")
        finally:
            client.close()

    def target_stop(self):
        # Trigger end of file writing
        # First build JSON message to trigger end.
        # Message has the form:
        # {
        #     "command": "stop"
        # }
        message = {"command": "stop"}
        client = UDSClient(self._control_socket)
        client.send(json.dumps(message))
        response_str = client.recv(timeout=3)
        try:
            response = json.loads(response_str)["response"]
        except Exception:
            log.exception("Unable to parse JSON returned from apsuse application")
        else:
            if response != "success":
                raise Exception("Failed to stop APSUSE recording")
        finally:
            client.close()

    @coroutine
    def capture_stop(self):
        self._capturing = False
        self._internal_beam_mapping = {}
        log.info("Stopping capture")
        self._capture_monitor.stop()
        self._ingress_buffer_monitor.stop()
        log.info("Stopping MKRECV instance")
        self._mkrecv_proc.terminate()
        log.info("Stopping PSRDADA_CPP instance")
        self._apsuse_proc.terminate()
        log.info("Destroying DADA buffers")
        try:
            yield self._destroy_db(self._dada_input_key, timeout=7.0)
        except Exception as error:
            log.warning("Error raised on DB destroy: {}".format(str(error)))


if __name__ == "__main__":
    import coloredlogs
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s -"
             "%(filename)s:%(lineno)s] %(message)s"),
        level="DEBUG",
        logger=logger)
    logger.setLevel("DEBUG")
    logging.getLogger('katcp').setLevel(logging.ERROR)

    import time
    ioloop = IOLoop.current()
    coherent_capture = ApsCapture(
        "10.100.24.1", "/tmp/coherent_capture.sock",
        "/tmp/coherent_mkrecv.cfg", "0-8", "8-16",
        "coherent", "dada", "/DATA/APSUSE_COMMISSIONING/")
    incoherent_capture = ApsCapture(
        "10.100.24.1", "/tmp/incoherent_capture.sock",
        "/tmp/incoherent_mkrecv.cfg", "3", "4",
        "incoherent", "caca", "/DATA/APSUSE_COMMISSIONING/")

    coherent_config = {
        "mcast-groups": "239.11.1.1+15",
        "mcast-port": 7147,
        "stream-indices": range(96),
        "beam-ids": ["cfbf{:05d}".format(i) for i in range(96)],
        "nchans": 1024,
        "nchans-per-heap": 16,
        "bandwidth": 856e6,
        "centre-frequency": 1284e6,
        "sampling-interval": 306e-6,
        "heap-size": 8192,
        "filesize": 10e9,
        "sync-epoch": 1232352352.0,
        "sample-clock": 1712000000.0,
        "idx1-step": 268435456
    }

    incoherent_config = {
        "mcast-groups": ["239.11.1.0"],
        "mcast-port": 7147,
        "stream-indices": [0],
        "beam-ids": ['ifbf00000'],
        "nchans": 1024,
        "nchans-per-heap": 16,
        "bandwidth": 856e6,
        "centre-frequency": 1284e6,
        "sampling-interval": 306e-6,
        "heap-size": 8192,
        "filesize": 10e9,
        "sync-epoch": 1232352352.0,
        "sample-clock": 1712000000.0,
        "idx1-step": 268435456
    }

    beam_params = []
    for ii in range(1):
        beam_params.append({'cfbf{:05d}'.format(ii): 'source0,radec,00:00:00.00,00:00:00'})

    ioloop.run_sync(lambda: coherent_capture.capture_start(coherent_config))
    #ioloop.run_sync(lambda: incoherent_capture.capture_start(incoherent_config))
    #coherent_capture.target_start(beam_params)
    #incoherent_capture.target_start(beam_params)
    time.sleep(2000)
    #coherent_capture.target_stop()
    #incoherent_capture.target_stop()
    coherent_capture.capture_stop()
    #incoherent_capture.capture_stop()




