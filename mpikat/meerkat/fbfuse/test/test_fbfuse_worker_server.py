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
import mock
import signal
import logging
import time
import sys
import importlib
import re
import ipaddress
import json
from StringIO import StringIO
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return, sleep
from tornado.testing import AsyncTestCase, gen_test
from katpoint import Antenna, Target
from katcp import AsyncReply, AsyncDeviceServer
from katcp.testutils import mock_req, handle_mock_req
from katpoint import Antenna
from mpikat.meerkat.fbfuse import FbfWorkerServer, FbfMasterController, DelayConfigurationServer, BeamManager
from mpikat.meerkat.katportalclient_wrapper import KatportalClientWrapper
from mpikat.meerkat.test.utils import MockFbfConfigurationAuthority, ANTENNAS, MockKatportalClientWrapper
from mpikat.test.utils import AsyncServerTester
from mpikat.core.ip_manager import ContiguousIpRange, ip_range_from_stream

root_logger = logging.getLogger('')
root_logger.setLevel(logging.CRITICAL)


class TestFbfWorkerServer(AsyncServerTester):
    def setUp(self):
        super(TestFbfWorkerServer, self).setUp()
        self.server = FbfWorkerServer('127.0.0.1', 0, dummy=False)
        self.server.start()

    def tearDown(self):
        super(TestFbfWorkerServer, self).tearDown()

    @gen_test
    def test_init(self):
        yield self._check_sensor_value('device-status', 'ok')
        yield self._check_sensor_value('state', self.server.IDLE)
        yield self._check_sensor_value('delay-engine-server', '', expected_status='unknown')
        yield self._check_sensor_value('antenna-capture-order', '', expected_status='unknown')
        yield self._check_sensor_value('mkrecv-header', '', expected_status='unknown')

    @gen_test(timeout=100000)
    def test_prepare(self):
        nbeams = 32
        antennas = ["m%03d"%ii for ii in range(16)]
        feng_antenna_map = {antenna:ii for ii,antenna in enumerate(antennas)}
        coherent_beam_antennas = antennas[:-1]
        incoherent_beam_antennas = antennas[1:]
        nantennas = len(antennas)
        beam_manager = BeamManager(nbeams, [Antenna(ANTENNAS[i]) for i in coherent_beam_antennas])
        delay_config_server = DelayConfigurationServer("127.0.0.1", 0, beam_manager)
        delay_config_server.start()
        dc_ip, dc_port = delay_config_server.bind_address
        for _ in range(nbeams):
            beam_manager.add_beam(Target("source0, radec, 123.1, -30.3"))

        coherent_beams_csv = ",".join([beam.idx for beam in beam_manager.get_beams()])
        tot_nchans = 4096
        feng_groups = "spead://239.11.1.150+3:7147"
        nchans_per_group = tot_nchans / nantennas / 4
        chan0_idx = 0
        chan0_freq = 1240e6
        chan_bw = 856e6 / tot_nchans
        mcast_to_beam_map = {
            "spead://239.11.2.150:7147":coherent_beams_csv,
            "spead://239.11.2.151:7147":"ifbf00001"
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
            "tscrunch":16,
            "fscrunch":1,
            "antennas":",".join(coherent_beam_antennas)
        }
        incoherent_beam_config = {
            "tscrunch":16,
            "fscrunch":1,
            "antennas":",".join(incoherent_beam_antennas)
        }
        yield self._send_request_expect_ok('prepare', feng_groups, nchans_per_group, chan0_idx, chan0_freq,
                        chan_bw, json.dumps(mcast_to_beam_map), json.dumps(feng_config),
                        json.dumps(coherent_beam_config), json.dumps(incoherent_beam_config), dc_ip, dc_port)
        yield sleep(10)
        self.server._delay_buffer_controller.stop()


if __name__ == '__main__':
    unittest.main(buffer=True)





