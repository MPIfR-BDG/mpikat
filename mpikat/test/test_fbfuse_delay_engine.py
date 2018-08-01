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
from tornado.testing import AsyncTestCase, gen_test
from katpoint import Antenna, Target
from mpikat import DelayEngine, BeamManager

root_logger = logging.getLogger('')
root_logger.setLevel(logging.CRITICAL)

DEFAULT_ANTENNAS_FILE = os.path.join(os.path.dirname(__file__), 'data', 'default_antenna.csv')
with open(DEFAULT_ANTENNAS_FILE, "r") as f:
    DEFAULT_ANTENNAS = f.read().strip().splitlines()
KATPOINT_ANTENNAS = [Antenna(i) for i in DEFAULT_ANTENNAS]

class TestDelayEngine(AsyncTestCase):
    def setUp(self):
        super(TestDelayEngine, self).setUp()

    def tearDown(self):
        super(TestDelayEngine, self).tearDown()

    @gen_test
    def test_delay_engine_startup(self):
        bm = BeamManager(4, KATPOINT_ANTENNAS)
        de = DelayEngine("127.0.0.1", 0, bm)
        de.start()
        bm.add_beam(Target('test_target0,radec,12:00:00,01:00:00'))
        bm.add_beam(Target('test_target0,radec,12:00:00,01:00:00'))
        bm.add_beam(Target('test_target0,radec,12:00:00,01:00:00'))
        bm.add_beam(Target('test_target0,radec,12:00:00,01:00:00'))
        de.update_delays()

if __name__ == '__main__':
    unittest.main(buffer=True)
