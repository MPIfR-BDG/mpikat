"""
Copyright (c) 2018 Ewan Barr <ebarr@mpifr-bonn.mpg.de>

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

import unittest
import logging
import json
from tornado.gen import sleep
from tornado.testing import gen_test
from katpoint import Antenna, Target
from mpikat.meerkat.fbfuse import FbfWorkerServer, DelayConfigurationServer, BeamManager
from mpikat.meerkat.fbfuse.fbfuse_worker_server import determine_feng_capture_order
from mpikat.meerkat.test.utils import ANTENNAS
from mpikat.test.utils import AsyncServerTester

root_logger = logging.getLogger('mpikat')
root_logger.setLevel(logging.DEBUG)


class TestFbfWorkerServer(AsyncServerTester):

    def setUp(self):
        super(TestFbfWorkerServer, self).setUp()
        self.server = FbfWorkerServer(
            '127.0.0.1', 0, '127.0.0.1', exec_mode="dry-run")
        self.server.start()

    def tearDown(self):
        super(TestFbfWorkerServer, self).tearDown()

    @gen_test
    def test_init(self):
        yield self._check_sensor_value('device-status', 'ok')
        yield self._check_sensor_value('state', self.server.IDLE)
        yield self._check_sensor_value(
            'delay-engine-server', '', expected_status='unknown')
        yield self._check_sensor_value(
            'antenna-capture-order', '', expected_status='unknown')
        yield self._check_sensor_value(
            'mkrecv-capture-header', '', expected_status='unknown')

    @gen_test(timeout=100000)
    def test_dryrun(self):
        nbeams = 32
        antennas = ["m%03d" % ii for ii in range(16)]
        feng_antenna_map = {antenna: ii for ii, antenna in enumerate(antennas)}
        coherent_beam_antennas = antennas
        incoherent_beam_antennas = antennas
        nantennas = len(antennas)
        beam_manager = BeamManager(
            nbeams, [Antenna(ANTENNAS[i]) for i in coherent_beam_antennas])
        delay_config_server = DelayConfigurationServer(
            "127.0.0.1", 0, beam_manager)
        delay_config_server.start()
        dc_ip, dc_port = delay_config_server.bind_address
        for _ in range(nbeams):
            beam_manager.add_beam(Target("source0, radec, 123.1, -30.3"))

        coherent_beams_csv = ",".join(
            [beam.idx for beam in beam_manager.get_beams()])
        tot_nchans = 4096
        feng_groups = "spead://239.11.1.150+3:7147"
        nchans_per_group = tot_nchans / nantennas / 4
        chan0_idx = 0
        chan0_freq = 1240e6
        chan_bw = 856e6 / tot_nchans
        mcast_to_beam_map = {
            "spead://239.11.2.150:7147": coherent_beams_csv,
            "spead://239.11.2.151:7147": "ifbf00001"
        }
        feng_config = {
            "bandwidth": 856e6,
            "centre-frequency": 1200e6,
            "sideband": "upper",
            "feng-antenna-map": feng_antenna_map,
            "sync-epoch": 12353524243.0,
            "nchans": 4096
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
        yield self._send_request_expect_ok(
            'prepare', feng_groups, nchans_per_group, chan0_idx, chan0_freq,
            chan_bw, nbeams, json.dumps(mcast_to_beam_map),
            json.dumps(feng_config),
            json.dumps(coherent_beam_config),
            json.dumps(incoherent_beam_config),
            dc_ip, dc_port)
        yield self._check_sensor_value('device-status', 'ok')
        yield self._send_request_expect_ok('capture-start')
        yield sleep(10)
        yield self._send_request_expect_ok('capture-stop')
        self.server._delay_buffer_controller.stop()


class TestCaptureOrdering(unittest.TestCase):
    def test_same_antennas(self):
        antennas = ["m%03d" % ii for ii in range(16)]
        feng_antenna_map = {antenna: ii for ii, antenna in enumerate(antennas)}
        coherent_beam_antennas = antennas
        incoherent_beam_antennas = antennas
        coherent_beam_config = {
            "antennas": ",".join(coherent_beam_antennas)
        }
        incoherent_beam_config = {
            "antennas": ",".join(incoherent_beam_antennas)
        }
        info = determine_feng_capture_order(
                feng_antenna_map, coherent_beam_config,
                incoherent_beam_config)
        self.assertEqual(info['coherent_span'], info['incoherent_span'])
        self.assertEqual(info['unused_span'][0], info['unused_span'][1])
        for a, b in zip(info['order'], range(len(antennas))):
            self.assertEqual(a, b)


    def test_different_antennas_all_used(self):
        antennas = ["m%03d" % ii for ii in range(16)]
        feng_antenna_map = {antenna: ii for ii, antenna in enumerate(antennas)}
        coherent_beam_antennas = antennas[:-1]
        incoherent_beam_antennas = antennas[1:]
        coherent_beam_config = {
            "antennas": ",".join(coherent_beam_antennas)
        }
        incoherent_beam_config = {
            "antennas": ",".join(incoherent_beam_antennas)
        }

        info = determine_feng_capture_order(
            feng_antenna_map, coherent_beam_config,
            incoherent_beam_config)
        self.assertEqual(set(info['order'][info['coherent_span'][0]:info['coherent_span'][1]]),
                         set(feng_antenna_map[i] for i in coherent_beam_antennas))
        self.assertEqual(set(info['order'][info['incoherent_span'][0]:info['incoherent_span'][1]]),
                         set(feng_antenna_map[i] for i in incoherent_beam_antennas))
        self.assertEqual(info['unused_span'][0], info['unused_span'][1])

    def test_different_antennas_not_all_used(self):
        antennas = ["m%03d" % ii for ii in range(16)]
        feng_antenna_map = {antenna: ii for ii, antenna in enumerate(antennas)}
        coherent_beam_antennas = antennas[4:9]
        incoherent_beam_antennas = antennas[3:6]
        coherent_beam_config = {
            "antennas": ",".join(coherent_beam_antennas)
        }
        incoherent_beam_config = {
            "antennas": ",".join(incoherent_beam_antennas)
        }

        info = determine_feng_capture_order(
            feng_antenna_map, coherent_beam_config,
            incoherent_beam_config)
        self.assertEqual(set(info['order'][info['coherent_span'][0]:info['coherent_span'][1]]),
                         set(feng_antenna_map[i] for i in coherent_beam_antennas))
        self.assertEqual(set(info['order'][info['incoherent_span'][0]:info['incoherent_span'][1]]),
                         set(feng_antenna_map[i] for i in incoherent_beam_antennas))
        unused_antennas = list(set(info['order'][info['unused_span'][0]:info['unused_span'][1]]))
        for antenna in unused_antennas:
            self.assertNotIn(antenna, set(feng_antenna_map[i] for i in coherent_beam_antennas))
            self.assertNotIn(antenna, set(feng_antenna_map[i] for i in incoherent_beam_antennas))


if __name__ == '__main__':
    unittest.main(buffer=True)
