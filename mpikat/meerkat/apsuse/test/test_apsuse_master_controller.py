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
from urllib2 import urlopen, URLError
from StringIO import StringIO
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return, sleep
from tornado.testing import gen_test
from katcp import AsyncReply
from katcp.testutils import mock_req, handle_mock_req
from mpikat.meerkat.apsuse import ApsMasterController, ApsProductController
from mpikat.test.utils import AsyncServerTester
from mpikat.meerkat.test.utils import MockFbfKatportalMonitor

root_logger = logging.getLogger('')
root_logger.setLevel(logging.CRITICAL)

class TestApsMasterController(AsyncServerTester):
    DEFAULT_STREAMS = ('{"cam.http": {"camdata": "http://10.8.67.235/api/client/1"}}')

    def setUp(self):
        super(TestApsMasterController, self).setUp()
        self.server = ApsMasterController('127.0.0.1', 0, dummy=False)
        self.server._katportal_wrapper_type = MockFbfKatportalMonitor
        self.server.start()

    def tearDown(self):
        super(TestApsMasterController, self).tearDown()
        self.server = None

    @gen_test
    def test_product_lookup_errors(self):
        #Test that calls that require products fail if not configured
        yield self._send_request_expect_fail('capture-start', 'test')
        yield self._send_request_expect_fail('capture-stop', 'test')
        yield self._send_request_expect_fail('deconfigure', 'test')

    @gen_test
    def test_configure_start_stop_deconfigure(self):
        #Patching isn't working here for some reason (maybe pathing?)
        #hack solution is to manually switch to the Mock for the portal
        #client. TODO: Fix the structure of the code so that this can be
        #patched properly
        product_name = 'test_product'
        proxy_name = 'APSUSE_test'
        product_state_sensor = '{}.state'.format(product_name)
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_STREAMS, proxy_name)
        yield self._check_sensor_value('products', product_name)
        yield self._check_sensor_value(product_state_sensor, ApsProductController.READY)
        product = self.server._products[product_name]
        yield self._send_request_expect_ok('capture-start', product_name)
        yield self._check_sensor_value(product_state_sensor, ApsProductController.CAPTURING)
        yield self._send_request_expect_ok('capture-stop', product_name)
        yield self._send_request_expect_ok('deconfigure', product_name)
        self.assertEqual(self.server._products, {})
        has_sensor = yield self._check_sensor_exists(product_state_sensor)
        self.assertFalse(has_sensor)

    @gen_test
    def test_configure_same_product(self):
        product_name = 'test_product'
        proxy_name = 'APSUSE_test'
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_STREAMS, proxy_name)
        yield self._send_request_expect_fail('configure', product_name, self.DEFAULT_STREAMS, proxy_name)

    @gen_test
    def test_configure_bad_streams(self):
        yield self._send_request_expect_fail('configure', 'test_product', '{}', 'test_proxy')

    @gen_test
    def test_capture_start_while_stopping(self):
        product_name = 'test_product'
        proxy_name = 'APSUSE_test'
        product_state_sensor = '{}.state'.format(product_name)
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_STREAMS, proxy_name)
        product = self.server._products[product_name]
        product._state_sensor.set_value(ApsProductController.STOPPING)
        yield self._send_request_expect_fail('capture-start', product_name)

if __name__ == '__main__':
    unittest.main(buffer=True)

