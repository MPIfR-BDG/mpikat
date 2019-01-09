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
import socket
import json
from urllib2 import urlopen, URLError
from StringIO import StringIO
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return, sleep
from tornado.testing import gen_test
from katpoint import Antenna, Target
from katcp import AsyncReply
from katcp.testutils import mock_req, handle_mock_req
import mpikat
from mpikat.scpi_client import AsyncScpiClient, ScpiFailedRequest
from mpikat.paf_master_controller import PafMasterController, PAF_PRODUCT_ID, SCPI_BASE_ID
from mpikat.paf_product_controller import PafProductController
from mpikat.paf_worker_wrapper import PafWorkerWrapper
from mpikat.test.utils import AsyncServerTester

root_logger = logging.getLogger('')
root_logger.setLevel(logging.DEBUG)

SCPI_TEST_PORT = 5001

class TestPafMasterController(AsyncServerTester):
    def setUp(self):
        super(TestPafMasterController, self).setUp()
        self.server = PafMasterController('127.0.0.1', 0, '127.0.0.1', SCPI_TEST_PORT)
        self.server.start()
        self.ioloop = IOLoop.current()

    def tearDown(self):
        super(TestPafMasterController, self).tearDown()
        self.server.stop()
        self.server = None

    @gen_test
    def test_product_lookup_errors(self):
        #Test that calls that require products fail if not configured
        yield self._send_request_expect_fail('capture-start', 'test')
        yield self._send_request_expect_fail('capture-stop', 'test')
        yield self._send_request_expect_fail('deconfigure', 'test', '{}')
       
    @gen_test
    def test_scpi_control_not_set(self):
        yield self._send_request_expect_ok('set-control-mode', 'KATCP')
        client = AsyncScpiClient('', SCPI_TEST_PORT, self.ioloop)
        try:
            yield client.send('{}:configure'.format(SCPI_BASE_ID))
        except socket.timeout:
            pass
        except Exception as error:
            self.fail("Expected socket.timeout error but received '{}'".format(str(error)))

    @gen_test
    def test_good_scpi_configure(self):
        yield self._send_request_expect_ok('set-control-mode', 'SCPI')
        client = AsyncScpiClient('127.0.0.1', SCPI_TEST_PORT, self.ioloop)
        try:
            yield client.send('{}:setfrequency 1400000000'.format(SCPI_BASE_ID))
            yield client.send('{}:setnbands 33'.format(SCPI_BASE_ID))
            yield client.send('{}:setbandoffset 7'.format(SCPI_BASE_ID))
            yield client.send('{}:setnbeams 36'.format(SCPI_BASE_ID))
            yield client.send('{}:setmode SpSearch'.format(SCPI_BASE_ID))
            yield client.send('{}:setwritefil 0'.format(SCPI_BASE_ID))
            yield client.send('{}:configure'.format(SCPI_BASE_ID))
        except Exception as error:
            self.fail("Exception on SCPI requests: {}".format(str(error)))
        yield self._check_sensor_value('{}.state'.format(PAF_PRODUCT_ID), 'ready')

    @gen_test
    def test_bad_scpi_configure(self):
        yield self._send_request_expect_ok('set-control-mode', 'SCPI')
        client = AsyncScpiClient('127.0.0.1', SCPI_TEST_PORT, self.ioloop)
        try:
            yield client.send('{}:configure'.format(SCPI_BASE_ID))
        except ScpiFailedRequest:
            pass
        else:
            self.fail("Expected failure from configure call")

    @gen_test
    def test_good_katcp_configure(self):
        config = {
            "frequency": 1400000000.0, 
            "nbands": 33, 
            "mode": "SpSearch", 
            "nbeams": 36, 
            "band_offset": 0, 
            "write_filterbank": False
            }
        yield self._send_request_expect_ok('set-control-mode', 'KATCP')
        yield self._send_request_expect_ok('configure', json.dumps(config))
        yield self._check_sensor_value('{}.state'.format(PAF_PRODUCT_ID), 'ready')

    @gen_test
    def test_bad_katcp_configure(self):
        config = {
            "frequency": 1400000000.0, 
            "write_filterbank": False
            }
        yield self._send_request_expect_ok('set-control-mode', 'KATCP')
        yield self._send_request_expect_fail('configure', json.dumps(config))

if __name__ == '__main__':
    #logger = logging.getLogger('scpi')
    #coloredlogs.install(
    #    fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
    #    level=opts.log_level.upper(),
    #    logger=logger)
    unittest.main(buffer=True)

