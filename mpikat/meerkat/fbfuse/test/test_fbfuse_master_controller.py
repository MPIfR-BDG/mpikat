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
from katpoint import Antenna, Target
from katcp import AsyncReply
from katcp.testutils import mock_req, handle_mock_req
from mpikat.test.utils import AsyncServerTester
from mpikat.core.ip_manager import ContiguousIpRange, ip_range_from_stream
from mpikat.meerkat.fbfuse import FbfMasterController, FbfProductController, FbfWorkerWrapper
from mpikat.meerkat.katportalclient_wrapper import KatportalClientWrapper
from mpikat.meerkat.test.utils import MockFbfConfigurationAuthority, MockKatportalClientWrapper

root_logger = logging.getLogger('')
root_logger.setLevel(logging.CRITICAL)

class TestFbfMasterController(AsyncServerTester):
    DEFAULT_STREAMS = ('{"cam.http": {"camdata": "http://10.8.67.235/api/client/1"}, '
        '"cbf.antenna_channelised_voltage": {"i0.antenna-channelised-voltage": '
        '"spead://239.2.1.150+15:7148"}}')
    DEFAULT_NCHANS = 4096
    DEFAULT_ANTENNAS = 'm007,m008'

    def setUp(self):
        super(TestFbfMasterController, self).setUp()
        self.server = FbfMasterController('127.0.0.1', 0, dummy=True)
        self.server._katportal_wrapper_type = MockKatportalClientWrapper
        self.server.start()

    def tearDown(self):
        super(TestFbfMasterController, self).tearDown()
        self.server = None

    def _add_n_servers(self, n):
        base_ip = ipaddress.ip_address(u'192.168.1.150')
        for ii in range(n):
            self.server._server_pool.add(str(base_ip+ii), 5000)

    #@coroutine
    #def _configure_helper(self, product_name, antennas, nchans, streams_json, proxy_name):
    #    req = mock_req('configure', product_name, antennas, nchans, streams_json, proxy_name)
    #    reply,informs = yield handle_mock_req(self.server, req)
    #    raise Return((reply, informs))

    @gen_test
    def test_product_lookup_errors(self):
        #Test that calls that require products fail if not configured
        yield self._send_request_expect_fail('capture-start', 'test')
        yield self._send_request_expect_fail('capture-stop', 'test')
        yield self._send_request_expect_fail('provision-beams', 'test', 'random_schedule_block_id')
        yield self._send_request_expect_fail('reset-beams', 'test')
        yield self._send_request_expect_fail('deconfigure', 'test')
        yield self._send_request_expect_fail('set-default-target-configuration', 'test', '')
        yield self._send_request_expect_fail('set-default-sb-configuration', 'test', '')
        yield self._send_request_expect_fail('add-beam', 'test', '')
        yield self._send_request_expect_fail('add-tiling', 'test', '', 0, 0, 0, 0)

    @gen_test
    def test_configure_start_stop_deconfigure(self):
        #Patching isn't working here for some reason (maybe pathing?)
        #hack solution is to manually switch to the Mock for the portal
        #client. TODO: Fix the structure of the code so that this can be
        #patched properly
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        product_state_sensor = '{}.state'.format(product_name)
        self._add_n_servers(64)
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._check_sensor_value('products', product_name)
        yield self._check_sensor_value(product_state_sensor, FbfProductController.IDLE)
        yield self._send_request_expect_ok('provision-beams', product_name, 'random_schedule_block_id')
        # after provision beams we need to wait on the system to get into a ready state
        product = self.server._products[product_name]
        while True:
            yield sleep(0.5)
            if product.ready: break
        yield self._check_sensor_value(product_state_sensor, FbfProductController.READY)
        yield self._send_request_expect_ok('capture-start', product_name)
        yield self._check_sensor_value(product_state_sensor, FbfProductController.CAPTURING)
        yield self._send_request_expect_ok('capture-stop', product_name)
        yield self._send_request_expect_ok('deconfigure', product_name)
        self.assertEqual(self.server._products, {})
        has_sensor = yield self._check_sensor_exists(product_state_sensor)
        self.assertFalse(has_sensor)

    @gen_test
    def test_configure_same_product(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._send_request_expect_fail('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)

    @gen_test
    def test_configure_no_antennas(self):
        yield self._send_request_expect_fail('configure', 'test_product', '', self.DEFAULT_NCHANS,
            self.DEFAULT_STREAMS, 'FBFUSE_test')

    @gen_test
    def test_configure_bad_antennas(self):
        yield self._send_request_expect_fail('configure', 'test_product', 'NotAnAntenna',
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, 'FBFUSE_test')

    @gen_test
    def test_configure_bad_n_channels(self):
        yield self._send_request_expect_fail('configure', 'test_product', self.DEFAULT_ANTENNAS,
            4097, self.DEFAULT_STREAMS, 'FBFUSE_test')

    @gen_test
    def test_configure_bad_streams(self):
        yield self._send_request_expect_fail('configure', 'test_product', self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, '{}', 'FBFUSE_test')

    @gen_test
    def test_capture_start_while_stopping(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        product_state_sensor = '{}.state'.format(product_name)
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        product = self.server._products[product_name]
        product._state_sensor.set_value(FbfProductController.STOPPING)
        yield self._send_request_expect_fail('capture-start', product_name)

    @gen_test
    def test_register_deregister_worker_servers(self):
        hostname = '127.0.0.1'
        port = 10000
        yield self._send_request_expect_ok('register-worker-server', hostname, port)
        server = self.server._server_pool.available()[0]
        self.assertEqual(server.hostname, hostname)
        self.assertEqual(server.port, port)
        other = FbfWorkerWrapper(hostname, port)
        self.assertEqual(server, other)
        self.assertIn(other, self.server._server_pool.available())
        reply, informs = yield self._send_request_expect_ok('worker-server-list')
        self.assertEqual(int(reply.arguments[1]), 1)
        self.assertEqual(informs[0].arguments[0], "{} free".format(server))
        #try adding the same server again (should work)
        yield self._send_request_expect_ok('register-worker-server', hostname, port)
        yield self._send_request_expect_ok('deregister-worker-server', hostname, port)
        self.assertEqual(len(self.server._server_pool.available()), 0)

    @gen_test
    def test_deregister_allocated_worker_server(self):
        hostname, port = '127.0.0.1', 60000
        yield self._send_request_expect_ok('register-worker-server', hostname, port)
        server = self.server._server_pool.allocate(1)[-1]
        yield self._send_request_expect_fail('deregister-worker-server', hostname, port)

    @gen_test
    def test_deregister_nonexistant_worker_server(self):
        hostname, port = '192.168.1.150', 60000
        yield self._send_request_expect_ok('deregister-worker-server', hostname, port)

    @gen_test
    def test_set_configuration_authority(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        subarray_antennas = 'm007,m008,m009,m010'
        hostname, port = "127.0.0.1", 60000
        yield self._send_request_expect_ok('configure', product_name, subarray_antennas,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._send_request_expect_ok('set-configuration-authority', product_name, hostname, port)
        address = "{}:{}".format(hostname,port)
        yield self._check_sensor_value('{}.configuration-authority'.format(product_name), address)

    @gen_test
    def test_get_sb_configuration_from_ca(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        hostname = "127.0.0.1"
        sb_id = "default_subarray"
        target = 'test_target,radec,12:00:00,01:00:00'
        sb_config = {
            u'coherent-beams-nbeams':100,
            u'coherent-beams-tscrunch':22,
            u'coherent-beams-fscrunch':2,
            u'coherent-beams-antennas':'m007',
            u'coherent-beams-granularity':6,
            u'incoherent-beam-tscrunch':16,
            u'incoherent-beam-fscrunch':1,
            u'incoherent-beam-antennas':'m008'
            }
        ca_server = MockFbfConfigurationAuthority(hostname, 0)
        ca_server.start()
        ca_server.set_sb_config_return_value(proxy_name, sb_id, sb_config)
        self._add_n_servers(64)
        port = ca_server.bind_address[1]
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._send_request_expect_ok('set-configuration-authority', product_name, hostname, port)
        yield self._send_request_expect_ok('provision-beams', product_name, sb_id)
        product = self.server._products[product_name]
        while True:
            yield sleep(0.5)
            if product.ready: break
        # Here we need to check if the proxy sensors have been updated
        yield self._check_sensor_value("{}.coherent-beam-count".format(product_name), sb_config['coherent-beams-nbeams'], tolerance=0.05)
        yield self._check_sensor_value("{}.coherent-beam-tscrunch".format(product_name), sb_config['coherent-beams-tscrunch'])
        yield self._check_sensor_value("{}.coherent-beam-fscrunch".format(product_name), sb_config['coherent-beams-fscrunch'])
        yield self._check_sensor_value("{}.coherent-beam-antennas".format(product_name), 'm007')
        yield self._check_sensor_value("{}.incoherent-beam-tscrunch".format(product_name), sb_config['incoherent-beam-tscrunch'])
        yield self._check_sensor_value("{}.incoherent-beam-fscrunch".format(product_name), sb_config['incoherent-beam-fscrunch'])
        yield self._check_sensor_value("{}.incoherent-beam-antennas".format(product_name), 'm008')
        expected_ibc_mcast_group = ContiguousIpRange(str(self.server._ip_pool._ip_range.base_ip),
            self.server._ip_pool._ip_range.port, 1)
        yield self._check_sensor_value("{}.incoherent-beam-multicast-group".format(product_name),
            expected_ibc_mcast_group.format_katcp())
        _, ngroups = yield self._get_sensor_reading("{}.coherent-beam-ngroups".format(product_name))
        expected_cbc_mcast_groups = ContiguousIpRange(str(self.server._ip_pool._ip_range.base_ip+1),
            self.server._ip_pool._ip_range.port, ngroups)
        yield self._check_sensor_value("{}.coherent-beam-multicast-groups".format(product_name),
            expected_cbc_mcast_groups.format_katcp())
        yield self._send_request_expect_ok('capture-start', product_name)

    @gen_test
    def test_get_target_configuration_from_ca(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        hostname = "127.0.0.1"
        sb_id = "default_subarray" #TODO replace this when the sb_id is actually provided to FBF
        targets = ['test_target0,radec,12:00:00,01:00:00',
                   'test_target1,radec,13:00:00,02:00:00']
        ca_server = MockFbfConfigurationAuthority(hostname, 0)
        ca_server.start()
        ca_server.set_sb_config_return_value(proxy_name, sb_id, {})
        ca_server.set_target_config_return_value(proxy_name, targets[0], {'beams':targets})
        port = ca_server.bind_address[1]
        self._add_n_servers(64)
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._send_request_expect_ok('set-configuration-authority', product_name, hostname, port)
        yield self._send_request_expect_ok('provision-beams', product_name, 'random_schedule_block_id')
        product = self.server._products[product_name]
        while True:
            yield sleep(0.5)
            if product.ready: break
        yield self._send_request_expect_ok('capture-start', product_name)
        yield self._send_request_expect_ok('target-start', product_name, targets[0])
        yield self._check_sensor_value('{}.coherent-beam-cfbf00000'.format(product_name),
            Target(targets[0]).format_katcp())
        yield self._check_sensor_value('{}.coherent-beam-cfbf00001'.format(product_name),
            Target(targets[1]).format_katcp())
        new_targets = ['test_target2,radec,14:00:00,03:00:00',
                       'test_target3,radec,15:00:00,04:00:00']
        ca_server.update_target_config(proxy_name, {'beams':new_targets})
        # Need to give some time for the update callback to hit the top of the
        # event loop and change the beam configuration sensors.
        yield sleep(1)
        yield self._check_sensor_value('{}.coherent-beam-cfbf00000'.format(product_name),
            Target(new_targets[0]).format_katcp())
        yield self._check_sensor_value('{}.coherent-beam-cfbf00001'.format(product_name),
            Target(new_targets[1]).format_katcp())
        yield self._send_request_expect_ok('target-stop', product_name)
        # Put beam configuration back to original:
        ca_server.update_target_config(proxy_name, {'beams':targets})
        yield sleep(1)
        # At this point the sensor values should NOT have updated
        yield self._check_sensor_value('{}.coherent-beam-cfbf00000'.format(product_name),
            Target(new_targets[0]).format_katcp())
        yield self._check_sensor_value('{}.coherent-beam-cfbf00001'.format(product_name),
            Target(new_targets[1]).format_katcp())
        #Not start up a new target-start
        yield self._send_request_expect_ok('target-start', product_name, targets[0])
        yield self._check_sensor_value('{}.coherent-beam-cfbf00000'.format(product_name),
            Target(targets[0]).format_katcp())
        yield self._check_sensor_value('{}.coherent-beam-cfbf00001'.format(product_name),
            Target(targets[1]).format_katcp())


if __name__ == '__main__':
    unittest.main(buffer=True)

