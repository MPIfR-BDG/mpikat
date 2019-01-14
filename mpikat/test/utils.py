import mock
import re
from tornado.gen import coroutine, Return
from tornado.testing import AsyncTestCase
from katcp.testutils import mock_req, handle_mock_req

def type_converter(value):
    try: return int(value)
    except: pass
    try: return float(value)
    except: pass
    return value

class AsyncServerTester(AsyncTestCase):
    def setUp(self):
        super(AsyncServerTester, self).setUp()

    def tearDown(self):
        super(AsyncServerTester, self).tearDown()

    @coroutine
    def _get_sensor_reading(self, sensor_name):
        req = mock_req('sensor-value', sensor_name)
        reply,informs = yield handle_mock_req(self.server, req)
        self.assertTrue(reply.reply_ok(), msg=reply)
        status, value = informs[0].arguments[-2:]
        value = type_converter(value)
        raise Return((status, value))

    @coroutine
    def _check_sensor_value(self, sensor_name, expected_value, expected_status='nominal', tolerance=None):
        #Test that the products sensor has been updated
        status, value = yield self._get_sensor_reading(sensor_name)
        value = type_converter(value)
        self.assertEqual(status, expected_status)
        if not tolerance:
            self.assertEqual(value, expected_value)
        else:
            max_value = value + value*tolerance
            min_value = value - value*tolerance
            self.assertTrue((value<=max_value) and (value>=min_value))

    @coroutine
    def _check_sensor_exists(self, sensor_name):
        #Test that the products sensor has been updated
        req = mock_req('sensor-list', sensor_name)
        reply,informs = yield handle_mock_req(self.server, req)
        raise Return(reply.reply_ok())

    @coroutine
    def _send_request_expect_ok(self, request_name, *args):
        reply,informs = yield handle_mock_req(self.server, mock_req(request_name, *args))
        self.assertTrue(reply.reply_ok(), msg=reply)
        raise Return((reply, informs))

    @coroutine
    def _send_request_expect_fail(self, request_name, *args):
        reply,informs = yield handle_mock_req(self.server, mock_req(request_name, *args))
        self.assertFalse(reply.reply_ok(), msg=reply)
        raise Return((reply, informs))