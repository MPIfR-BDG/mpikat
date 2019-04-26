import json
import logging
import os
import time
import coloredlogs 
from optparse import OptionParser
from tornado.gen import coroutine
from tornado.ioloop import IOLoop
from katpoint import Antenna
from katcp import Sensor, AsyncDeviceServer
from katcp import KATCPClientResource
from mpikat.meerkat.fbfuse.fbfuse_worker_server import FbfWorkerServer
from mpikat.meerkat.fbfuse import (
    BeamManager,
    DelayConfigurationServer)
from mpikat.core.utils import parse_csv_antennas
from mpikat.core.ip_manager import ip_range_from_stream
from mpikat.meerkat.test.utils import ANTENNAS

class ManualWorkerController(object):
    def __init__(self, cap_ip, numa_node, exec_mode, ioloop):
        self._cap_ip = cap_ip
        self._numa_node = numa_node
        self._exec_mode = exec_mode
        self._ioloop = ioloop
        self._worker_server = None
        self._beam_manager = None
        self._delay_config_server = None

    @coroutine
    def start(self):
        self._worker_server = FbfWorkerServer(
            "127.0.0.1", 0, self._cap_ip,
            self._numa_node, self._exec_mode)
        yield self._worker_server.start()

    @coroutine
    def setup(self, subarray_size, antennas_csv, nbeams, tot_nchans, feng_groups, chan0_idx, worker_idx):
        cbc_antennas_names = parse_csv_antennas(antennas_csv)
        cbc_antennas = [Antenna(ANTENNAS[name]) for name in cbc_antennas_names]
        self._beam_manager = BeamManager(nbeams, cbc_antennas)
        self._delay_config_server = DelayConfigurationServer(
            "127.0.0.1", 0, self._beam_manager)
        self._delay_config_server.start()
        antennas_json = self._delay_config_server._antennas_sensor.value()
        antennas = json.loads(antennas_json)
        coherent_beams = ["cfbf{:05d}".format(ii) for ii in range(nbeams)]
        coherent_beams_csv = ",".join(coherent_beams)
        feng_antenna_map = {antenna: ii for ii, antenna in enumerate(antennas)}
        coherent_beam_antennas = antennas
        incoherent_beam_antennas = antennas
        nantennas = len(antennas)
        nchans_per_group = tot_nchans / subarray_size / 4
        nchans = ip_range_from_stream(feng_groups).count * nchans_per_group

        chan0_freq = 1240e6
        chan_bw = 856e6 / tot_nchans

        mcast_to_beam_map = {
            "spead://239.11.1.150:7148": "ifbf00001"
        }
        for ii in range(8):
            mcast_to_beam_map["spead://239.11.1.{}:7148".format(ii)] = ",".join(coherent_beams[4*ii:4*(ii+1)])

        feng_config = {
            "bandwidth": 856e6,
            "centre-frequency": 1200e6,
            "sideband": "upper",
            "feng-antenna-map": feng_antenna_map,
            "sync-epoch": 1554907897.0,
            "nchans": tot_nchans
        }
        coherent_beam_config = {
            "tscrunch": 16,
            "fscrunch": 1,
            "antennas": ",".join(coherent_beam_antennas)
        }
        incoherent_beam_config = {
            "tscrunch": 16,
            "fscrunch": 1,
            "antennas": ",".join(incoherent_beam_antennas)
        }

        worker_client = KATCPClientResource(dict(
            name="worker-server-client",
            address=self._worker_server.bind_address,
            controlled=True))
        yield worker_client.start()
        yield worker_client.until_synced()

        print "preparing"
        response = yield worker_client.req.prepare(
            feng_groups, nchans_per_group, chan0_idx, chan0_freq,
            chan_bw, nbeams, json.dumps(mcast_to_beam_map),
            json.dumps(feng_config),
            json.dumps(coherent_beam_config),
            json.dumps(incoherent_beam_config),
            *self._delay_config_server.bind_address, timeout=300.0)
        if not response.reply.reply_ok():
            raise Exception("Error on prepare: {}".format(response.reply.arguments))
        else:
            print "prepare done"

        yield worker_client.req.capture_start()

	#time.sleep(20)

        #print "ATTEMPTING CAPTURE STOP" 
	#yield worker_client.req.capture_stop()
	#print "CAPTURE stopped"

if __name__ == "__main__":
    import os
    os.system("taskset -c 15 -p {}".format(os.getpid()))
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('', '--dc-ip', dest='dc_ip', type=str,
                      help='Host interface to bind to')
    parser.add_option('', '--dc-port', dest='dc_port', type=int,
                      help='Port number to bind to')
    parser.add_option('', '--worker-ip', dest='worker_ip', type=str,
                      help='Host interface to bind to')
    parser.add_option('', '--worker-port', dest='worker_port', type=int,
                      help='Port number to bind to')
    (opts, args) = parser.parse_args()
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level="INFO",
        logger=logger)
    logging.getLogger('mpikat.fbfuse_delay_buffer_controller').setLevel("INFO")
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = IOLoop.current()
    controller = ManualWorkerController(os.environ['FBF_CAPTURE_IP'], 1, "full", ioloop)
    
    @coroutine
    def run():
        yield controller.start()
        yield controller.setup(4, "m011,m044,m056,m058", 32, 4096, "spead://239.8.0.12+3:7148", 3072, 3)
    ioloop.add_callback(run)
    ioloop.start()
