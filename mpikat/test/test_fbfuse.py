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
from mpikat.fbfuse import FbfMasterController, FbfProductController, ProductLookupError, KatportalClientWrapper, FbfWorkerWrapper

log = logging.getLogger('mpikat.test')

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

    @gen_test(timeout=30)
    def test_configure_start_stop_deconfigure(self):
        #Patching isn't working here for some reason (maybe pathing?)
        #hack solution is to manually switch to the Mock for the portal
        #client. TODO: Fix the structure of the code so that this can be
        #patched properly
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        product_state_sensor = '{}-state'.format(proxy_name)
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        yield self._check_sensor_value('products', product_name)
        yield self._check_sensor_value(product_state_sensor, FbfProductController.IDLE)
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
    def test_capture_start_during_provisioning(self):
        #Patching isn't working here for some reason (maybe pathing?)
        #hack solution is to manually switch to the Mock for the portal
        #client. TODO: Fix the structure of the code so that this can be
        #patched properly
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        product_state_sensor = '{}-state'.format(proxy_name)
        yield self._send_request_expect_ok('configure', product_name, self.DEFAULT_ANTENNAS,
            self.DEFAULT_NCHANS, self.DEFAULT_STREAMS, proxy_name)
        # provision-beams should return immediately
        yield self._send_request_expect_ok('provision-beams', product_name)
        yield self._send_request_expect_ok('capture-start', product_name)
        yield self._check_sensor_value(product_state_sensor, FbfProductController.CAPTURING)
        #provision-beams will fail on an already capturing instance
        yield self._send_request_expect_fail('provision-beams', product_name)
        #due to the behaviour of provision-beams, capture-start should return
        #ok even if the system is already capturing
        yield self._send_request_expect_ok('capture-start', product_name)

    @gen_test
    def test_capture_start_while_stopping(self):
        product_name = 'test_product'
        proxy_name = 'FBFUSE_test'
        product_state_sensor = '{}-state'.format(proxy_name)
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
        yield self._check_sensor_value("{}-coherent-beam-count".format(proxy_name), str(nbeams))
        yield self._check_sensor_value("{}-coherent-beam-tscrunch".format(proxy_name), str(tscrunch))
        yield self._check_sensor_value("{}-coherent-beam-fscrunch".format(proxy_name), str(fscrunch))
        yield self._check_sensor_value("{}-coherent-beam-antennas".format(proxy_name), self.DEFAULT_ANTENNAS)

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
        yield self._check_sensor_value("{}-incoherent-beam-tscrunch".format(proxy_name), str(tscrunch))
        yield self._check_sensor_value("{}-incoherent-beam-fscrunch".format(proxy_name), str(fscrunch))
        yield self._check_sensor_value("{}-incoherent-beam-antennas".format(proxy_name), self.DEFAULT_ANTENNAS)

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


if __name__ == '__main__':
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    #logging.basicConfig(format=FORMAT, level=logging.DEBUG)
    #log.setLevel(logging.DEBUG)
    #stream = StringIO()
    #handler = logging.StreamHandler(stream=stream)
    #formatter = logging.Formatter(FORMAT)
    #handler.setFormatter(formatter)
    #app_log.addHandler(handler)
    #app_log = logging.getLogger('mpikat.fbfuse')
    #app_log.setLevel(logging.WARNING)
    #for handler in app_log.handlers:
    #    handler.setFormatter(formatter)
    #formatter = logging.Formatter(FORMAT)
    #handler.setFormatter(formatter)
    #app_log.addHandler(handler)
    unittest.main(buffer=True)
    #print stream.read()

