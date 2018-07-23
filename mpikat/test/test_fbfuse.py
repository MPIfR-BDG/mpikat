import unittest
import mock
import signal
import logging
import time
import sys
import importlib
import re
from urllib2 import urlopen, URLError
from StringIO import StringIO
from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return, sleep
from tornado.testing import AsyncTestCase, gen_test
from katpoint import Antenna, Target
from katcp import AsyncReply
from katcp.testutils import mock_req, handle_mock_req
from katportalclient import SensorNotFoundError
from mpikat import fbfuse
from mpikat.fbfuse import (FbfMasterController,
                           FbfProductController,
                           ProductLookupError,
                           KatportalClientWrapper,
                           FbfWorkerWrapper,
                           BeamManager,
                           DelayEngine)
from mpikat.test.utils import MockFbfConfigurationAuthority

root_logger = logging.getLogger('')
root_logger.setLevel(logging.CRITICAL)

PORTAL = "portal.mkat.karoo.kat.ac.za"

class MockKatportalClientWrapper(mock.Mock):
    @coroutine
    def get_observer_string(self, antenna):
        if re.match("^[mM][0-9]{3}$", antenna):
            raise Return("{}, -30:42:39.8, 21:26:38.0, 1035.0, 13.5".format(antenna))
        else:
            raise SensorNotFoundError("No antenna named {}".format(antenna))

def requires_portal(func):
    def wrapped(*args, **kwargs):
        try:
            urlopen("http://{}".format(PORTAL))
        except URLError:
            raise unittest.SkipTest("No route to {}".format(PORTAL))
        else:
            return func(*args, **kwargs)
    return wrapped

class TestFbfMasterController(AsyncTestCase):
    DEFAULT_STREAMS = ('{"cam.http": {"camdata": "http://10.8.67.235/api/client/1"}, '
        '"cbf.antenna_channelised_voltage": {"i0.antenna-channelised-voltage": '
        '"spead://239.2.1.150+15:7148"}}')
    DEFAULT_NCHANS = 4096
    DEFAULT_ANTENNAS = 'm007,m008'

    def setUp(self):
        super(TestFbfMasterController, self).setUp()
        self.server = FbfMasterController('127.0.0.1', 0, dummy=True)
        self.server.start()

    def tearDown(self):
        super(TestFbfMasterController, self).tearDown()

    @coroutine
    def _configure_helper(self, product_name, antennas, nchans, streams_json, proxy_name):
        #Patching isn't working here for some reason (maybe pathing?), the
        #hack solution is to manually switch to the Mock for the portal
        #client. TODO: Fix the structure of the code so that this can be
        #patched properly
        #Test that a valid configure call goes through
        fbfuse.KatportalClientWrapper = MockKatportalClientWrapper
        req = mock_req('configure', product_name, antennas, nchans, streams_json, proxy_name)
        reply,informs = yield handle_mock_req(self.server, req)
        fbfuse.KatportalClientWrapper = KatportalClientWrapper
        raise Return((reply, informs))

    @coroutine
    def _check_sensor_value(self, sensor_name, expected_value, expected_status='nominal'):
        #Test that the products sensor has been updated
        req = mock_req('sensor-value', sensor_name)
        reply,informs = yield handle_mock_req(self.server, req)
        self.assertTrue(reply.reply_ok(), msg=reply)
        status, value = informs[0].arguments[-2:]
        self.assertEqual(status, expected_status)
        self.assertEqual(value, expected_value)

    @coroutine
    def _check_sensor_exists(self, sensor_name):
        #Test that the products sensor has been updated
        req = mock_req('sensor-list', sensor_name)
        reply,informs = yield handle_mock_req(self.server, req)
        raise Return(reply.reply_ok())

    @coroutine
    def _send_request_expect_ok(self, request_name, *args):
        if request_name == 'configure':
            reply, informs = yield self._configure_helper(*args)
        else:
            reply,informs = yield handle_mock_req(self.server, mock_req(request_name, *args))
        self.assertTrue(reply.reply_ok(), msg=reply)
        raise Return((reply, informs))

    @coroutine
    def _send_request_expect_fail(self, request_name, *args):
        if request_name == 'configure':
            reply, informs = yield self._configure_helper(*args)
        else:
            reply,informs = yield handle_mock_req(self.server, mock_req(request_name, *args))
        self.assertFalse(reply.reply_ok(), msg=reply)
        raise Return((reply, informs))

    @gen_test(timeout=10)
    @requires_portal
    def test_katportalclient_wrapper(self):
        kpc = KatportalClientWrapper(PORTAL)
        value = yield kpc.get_observer_string('m009')
        try:
            Antenna(value)
        except Exception as error:
            self.fail("Could not convert antenna string to katpoint Antenna instance,"
                " failed with error {}".format(str(error)))

    @gen_test(timeout=10)
    @requires_portal
    def test_katportalclient_wrapper_invalid_antenna(self):
        kpc = KatportalClientWrapper(PORTAL)
        try:
            value = yield kpc.get_observer_string('IAmNotAValidAntennaName')
        except SensorNotFoundError:
            pass

    @gen_test
    def test_product_lookup_errors(self):
        #Test that calls that require products fail if not configured
        yield self._send_request_expect_fail('capture-start', 'test')
        yield self._send_request_expect_fail('capture-stop', 'test')
        yield self._send_request_expect_fail('provision-beams', 'test')
        yield self._send_request_expect_fail('reset-beams', 'test')
        yield self._send_request_expect_fail('deconfigure', 'test')
        yield self._send_request_expect_fail('set-default-target-configuration', 'test', '')
        yield self._send_request_expect_fail('set-default-sb-configuration', 'test', '')
        yield self._send_request_expect_fail('add-beam', 'test', '')
        yield self._send_request_expect_fail('add-tiling', 'test', '', 0, 0, 0, 0)
        yield self._send_request_expect_fail('configure-coherent-beams', 'test', 0, '', 0, 0)
        yield self._send_request_expect_fail('configure-incoherent-beam', 'test', '', 0, 0)

    @gen_test
    def test_configure_start_stop_deconfigure(self):
        #Patching isn't working here for some reason (maybe pathing?)
        #hack solution is to manually switch to the Mock for the portal
        #client. TODO: Fix the structure of the code so that this can be
        #patched properly
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        product_state_sensor = '{}.state'.format(product_name)
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._check_sensor_value('products', product_name)
        yield self._check_sensor_value(product_state_sensor, FbfProductController.IDLE)
        yield self._send_request_expect_ok('provision-beams', product_name)
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
        server = self.server._server_pool.allocate(1)[0]
        yield self._send_request_expect_fail('deregister-worker-server', hostname, port)

    @gen_test
    def test_deregister_nonexistant_worker_server(self):
        hostname, port = '127.0.0.1', 60000
        yield self._send_request_expect_ok('deregister-worker-server', hostname, port)

    @gen_test
    def test_configure_coherent_beams(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        tscrunch = 6
        fscrunch = 2
        nbeams = 100
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._send_request_expect_ok('configure-coherent-beams', product_name, nbeams,
            self.DEFAULT_ANTENNAS, fscrunch, tscrunch)
        yield self._check_sensor_value("{}.coherent-beam-count".format(product_name), str(nbeams))
        yield self._check_sensor_value("{}.coherent-beam-tscrunch".format(product_name), str(tscrunch))
        yield self._check_sensor_value("{}.coherent-beam-fscrunch".format(product_name), str(fscrunch))
        yield self._check_sensor_value("{}.coherent-beam-antennas".format(product_name), self.DEFAULT_ANTENNAS)

    @gen_test
    def test_configure_incoherent_beam(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        tscrunch = 6
        fscrunch = 2
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._send_request_expect_ok('configure-incoherent-beam', product_name,
            self.DEFAULT_ANTENNAS, fscrunch, tscrunch)
        yield self._check_sensor_value("{}.incoherent-beam-tscrunch".format(product_name), str(tscrunch))
        yield self._check_sensor_value("{}.incoherent-beam-fscrunch".format(product_name), str(fscrunch))
        yield self._check_sensor_value("{}.incoherent-beam-antennas".format(product_name), self.DEFAULT_ANTENNAS)

    @gen_test
    def test_configure_coherent_beams_invalid_antennas(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        subarray_antennas = 'm007,m008,m009,m010'
        yield self._send_request_expect_ok('configure', product_name, subarray_antennas,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        #Test invalid antenna combinations
        yield self._send_request_expect_fail('configure-coherent-beams', product_name, 100,
            'm007,m008,m011', 1, 16)
        yield self._send_request_expect_fail('configure-coherent-beams', product_name, 100,
            'm007,m008,m009,m010,m011', 1, 16)
        yield self._send_request_expect_fail('configure-coherent-beams', product_name, 100,
            '', 1, 16)
        yield self._send_request_expect_fail('configure-coherent-beams', product_name, 100,
            'm007,m007,m008,m009', 1, 16)

    @gen_test
    def test_configure_incoherent_beam_invalid_antennas(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        subarray_antennas = 'm007,m008,m009,m010'
        yield self._send_request_expect_ok('configure', product_name, subarray_antennas,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        #Test invalid antenna combinations
        yield self._send_request_expect_fail('configure-incoherent-beam', product_name,
                    'm007,m008,m011', 1, 16)
        yield self._send_request_expect_fail('configure-incoherent-beam', product_name,
                    'm007,m008,m009,m010,m011', 1, 16)
        yield self._send_request_expect_fail('configure-incoherent-beam', product_name,
                    '', 1, 16)
        yield self._send_request_expect_fail('configure-incoherent-beam', product_name,
                    'm007,m007,m008,m009', 1, 16)

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
        sb_config = {u'coherent-beams':
                    {u'fscrunch': 2,
                     u'nbeams': 100,
                     u'tscrunch': 22},
                  u'incoherent-beam':
                    {u'fscrunch': 32,
                     u'tscrunch': 4}}
        ca_server = MockFbfConfigurationAuthority(hostname, 0)
        ca_server.start()
        ca_server.set_sb_config_return_value(proxy_name, sb_id, sb_config)
        port = ca_server.bind_address[1]
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._send_request_expect_ok('set-configuration-authority', product_name, hostname, port)
        yield self._send_request_expect_ok('provision-beams', product_name)
        product = self.server._products[product_name]
        while True:
            yield sleep(0.5)
            if product.ready: break
        # Here we need to check if the proxy sensors have been updated
        yield self._check_sensor_value("{}.coherent-beam-count".format(product_name), str(sb_config['coherent-beams']['nbeams']))
        yield self._check_sensor_value("{}.coherent-beam-tscrunch".format(product_name), str(sb_config['coherent-beams']['tscrunch']))
        yield self._check_sensor_value("{}.coherent-beam-fscrunch".format(product_name), str(sb_config['coherent-beams']['fscrunch']))
        yield self._check_sensor_value("{}.coherent-beam-antennas".format(product_name), self.DEFAULT_ANTENNAS)
        yield self._check_sensor_value("{}.incoherent-beam-tscrunch".format(product_name), str(sb_config['incoherent-beam']['tscrunch']))
        yield self._check_sensor_value("{}.incoherent-beam-fscrunch".format(product_name), str(sb_config['incoherent-beam']['fscrunch']))
        yield self._check_sensor_value("{}.incoherent-beam-antennas".format(product_name), self.DEFAULT_ANTENNAS)
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
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._send_request_expect_ok('set-configuration-authority', product_name, hostname, port)
        yield self._send_request_expect_ok('provision-beams', product_name)
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


class TestFbfDelayEngine(AsyncTestCase):
    DEFAULT_ANTENNAS = ['m007, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -89.5835 -402.7315 2.3675 5864.851 5864.965, 0:21:15.7 0 -0:00:41.8 0:01:56.1 0:00:30.5 -0:00:19.9 -0:23:44.9 -0:00:31.4, 1.22',
                        'm008, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -93.523 -535.0255 3.0425 5875.701 5876.213, -0:03:22.5 0 -0:00:43.0 -0:01:01.6 0:00:32.9 -0:00:12.9 -0:12:18.4 0:01:03.5, 1.22',
                        'm009, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, 32.357 -371.054 2.7315 5851.041 5851.051, 2:46:15.8 0 -0:04:14.1 -0:09:28.6 -0:00:22.6 -0:00:17.7 -0:02:33.5 -0:01:07.4, 1.22',
                        'm010, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, 88.1005 -511.8735 3.7765 5880.976 5881.857, 0:26:59.5 0 0:01:26.1 -0:00:54.8 0:00:34.2 -0:00:35.0 -0:02:48.1 0:00:38.0, 1.22',
                        'm011, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, 84.0175 -352.08 2.7535 5859.067 5859.093, -1:57:25.9 0 0:00:03.9 0:02:53.3 0:00:28.1 -0:00:15.1 -0:06:51.8 0:01:50.1, 1.22',
                        'm012, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, 140.0245 -368.268 3.0505 5864.229 5864.229, -0:17:21.2 0 -0:01:49.7 -0:00:38.2 0:00:15.2 -0:00:08.5 -0:01:11.4 0:01:57.3, 1.22',
                        'm013, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, 236.7985 -393.4625 3.719 5863.826 5864.44, 0:40:27.9 0 -0:02:35.2 -0:04:58.5 0:00:13.0 0:00:19.2 -0:05:55.6 0:01:09.3, 1.22',
                        'm014, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, 280.676 -285.792 3.144 5868.151 5868.376, 0:51:40.6 0 -0:01:27.5 0:00:58.0 0:00:11.9 0:00:03.8 -0:02:31.1 0:01:55.5, 1.22',
                        'm015, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, 210.6505 -219.1425 2.342 5919.036 5919.155, -0:10:22.6 0 0:01:16.7 -0:00:51.5 0:00:28.6 -0:00:38.2 -0:10:57.2 -0:00:43.5, 1.22',
                        'm016, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, 288.168 -185.868 2.43 5808.71 5808.856, 0:13:42.0 0 -0:02:17.7 0:00:02.0 0:00:04.9 -0:00:12.4 -0:08:00.4 0:02:07.9, 1.22']
    KATPOINT_ANTENNAS = [Antenna(i) for i in DEFAULT_ANTENNAS]
    def setUp(self):
        super(TestFbfDelayEngine, self).setUp()

    def tearDown(self):
        super(TestFbfDelayEngine, self).tearDown()

    @gen_test
    def test_delay_engine_startup(self):
        bm = BeamManager(4, self.KATPOINT_ANTENNAS)
        de = DelayEngine("127.0.0.1", 0, bm)
        de.start()
        bm.add_beam(Target('test_target0,radec,12:00:00,01:00:00'))
        bm.add_beam(Target('test_target0,radec,12:00:00,01:00:00'))
        bm.add_beam(Target('test_target0,radec,12:00:00,01:00:00'))
        bm.add_beam(Target('test_target0,radec,12:00:00,01:00:00'))
        de.update_delays()

if __name__ == '__main__':
    unittest.main(buffer=True)

