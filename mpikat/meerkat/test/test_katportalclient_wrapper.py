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
import logging
import re
from urllib2 import urlopen, URLError
from StringIO import StringIO
from tornado.gen import coroutine, Return
from tornado.testing import AsyncTestCase, gen_test
from katpoint import Antenna
from katportalclient import SensorNotFoundError, SensorLookupError
from mpikat.meerkat.katportalclient_wrapper import KatportalClientWrapper

root_logger = logging.getLogger('')
root_logger.setLevel(logging.CRITICAL)

PORTAL = "http://monctl.devnmk.camlab.kat.ac.za/api/client/1"

class TestKatPortalClientWrapper(AsyncTestCase):
    def setUp(self):
        super(TestKatPortalClientWrapper, self).setUp()
        try:
            urlopen(PORTAL)
        except URLError:
            raise unittest.SkipTest("No route to {}".format(PORTAL))
        self.kpc = KatportalClientWrapper(PORTAL)

    def tearDown(self):
        super(TestKatPortalClientWrapper, self).tearDown()

    @gen_test(timeout=10)
    def test_get_observer_string(self):
        try:
            value = yield self.kpc.get_observer_string('m001')
        except:
            raise unittest.SkipTest("m001 not available on current portal instance")
        try:
            Antenna(value)
        except Exception as error:
            self.fail("Could not convert antenna string to katpoint Antenna instance,"
                " failed with error {}".format(str(error)))

    @gen_test(timeout=10)
    def test_get_observer_string_invalid_antenna(self):
        try:
            value = yield self.kpc.get_observer_string('IAmNotAValidAntennaName')
        except SensorLookupError:
            pass

    @gen_test(timeout=10)
    def test_get_bandwidth(self):
        value = yield self.kpc.get_bandwidth('i0.antenna-channelised-voltage')

    @gen_test(timeout=10)
    def test_get_cfreq(self):
        value = yield self.kpc.get_cfreq('i0.antenna-channelised-voltage')

    @gen_test(timeout=10)
    def test_get_sideband(self):
        value = yield self.kpc.get_sideband('i0.antenna-channelised-voltage')
        self.assertIn(value, ['upper','lower'])

    @gen_test(timeout=10)
    def test_get_reference_itrf(self):
        value = yield self.kpc.get_itrf_reference()

    @gen_test(timeout=10)
    def test_get_sync_epoch(self):
        value = yield self.kpc.get_sync_epoch()


if __name__ == '__main__':
    unittest.main(buffer=True)