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

import logging
import os
import unittest
import socket
import struct
from tornado.testing import AsyncTestCase, gen_test
from tornado.gen import sleep
from katcp import KATCPClientResource
from katpoint import Antenna, Target
from mpikat.meerkat.fbfuse import DelayConfigurationServer, BeamManager
from mpikat.meerkat.fbfuse.fbfuse_delay_buffer_controller import DelayBufferController, CONTROL_SOCKET_ADDR

root_logger = logging.getLogger('mpikat')
root_logger.setLevel(logging.DEBUG)

DEFAULT_ANTENNAS_FILE = os.path.join(
    os.path.dirname(__file__), 'data', 'default_antenna.csv')
with open(DEFAULT_ANTENNAS_FILE, "r") as f:
    DEFAULT_ANTENNAS = f.read().strip().splitlines()
KATPOINT_ANTENNAS = [Antenna(i) for i in DEFAULT_ANTENNAS]


class TestDelayConfigurationServer(AsyncTestCase):
    def setUp(self):
        super(TestDelayConfigurationServer, self).setUp()

    def tearDown(self):
        super(TestDelayConfigurationServer, self).tearDown()

    @gen_test
    def test_startup(self):
        bm = BeamManager(4, KATPOINT_ANTENNAS)
        de = DelayConfigurationServer("127.0.0.1", 0, bm)
        de.start()
        bm.add_beam(Target('test_target0,radec,12:00:00,01:00:00'))
        bm.add_beam(Target('test_target1,radec,12:00:00,01:00:00'))
        bm.add_beam(Target('test_target2,radec,12:00:00,01:00:00'))
        bm.add_beam(Target('test_target3,radec,12:00:00,01:00:00'))

class TestDelayBufferController(AsyncTestCase):
    def setUp(self):
        super(TestDelayBufferController, self).setUp()
        self._beam_manager = BeamManager(32, KATPOINT_ANTENNAS)
        self._delay_config_server = DelayConfigurationServer(
            "127.0.0.1", 0, self._beam_manager)
        self._delay_config_server.start()
        self._delay_client = KATCPClientResource(dict(
            name="delay-configuration-client",
            address=self._delay_config_server.bind_address,
            controlled=True))
        self._delay_client.start()

    def tearDown(self):
        super(TestDelayBufferController, self).tearDown()
        self._delay_config_server.stop()

    @gen_test(timeout=20)
    def test_online_mode(self):
        beam_ids = ["cfbf{:05d}".format(i) for i in range(32)]
        for beam_id in beam_ids:
            self._beam_manager.add_beam(
                Target('{},radec,12:00:00,01:00:00'.format(beam_id)))
        antenna_ids = ["m{:03d}".format(i) for i in range(7, 7+4)]
        controller = DelayBufferController(
            self._delay_client, beam_ids, antenna_ids, 1, offline=False)
        yield controller.start()
        yield sleep(5)
        controller.stop()

    @gen_test(timeout=20)
    def test_offline_mode(self):
        def update_delay_via_socket():
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(CONTROL_SOCKET_ADDR)
            sock.sendall(struct.pack("d", 1554051922.649372))
            response = struct.unpack("b", sock.recv(1))[0]
            if response == 0:
                self.fail("Could not update delays")

        beam_ids = ["cfbf{:05d}".format(i) for i in range(32)]
        for beam_id in beam_ids:
            self._beam_manager.add_beam(
                Target('{},radec,12:00:00,01:00:00'.format(beam_id)))
        antenna_ids = ["m{:03d}".format(i) for i in range(7, 7+4)]
        controller = DelayBufferController(
            self._delay_client, beam_ids, antenna_ids, 1, offline=True)
        yield controller.start()
        update_delay_via_socket()
        controller.stop()

if __name__ == '__main__':
    unittest.main(buffer=True)
