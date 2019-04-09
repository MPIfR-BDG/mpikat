import mock
import re
from tornado.gen import coroutine, Return
from tornado.testing import AsyncTestCase
from katcp.testutils import mock_req, handle_mock_req
from katpoint import Target
from mpikat.meerkat.fbfuse import BaseFbfConfigurationAuthority
from mpikat.meerkat.test.utils.antennas import ANTENNAS

class SensorNotFoundError(Exception):
    pass

class MockFbfConfigurationAuthority(BaseFbfConfigurationAuthority):
    def __init__(self, host, port):
        super(MockFbfConfigurationAuthority, self).__init__(host, port)
        self.sb_return_values = {}
        self.target_return_values = {}

    def set_sb_config_return_value(self, proxy_id, sb_id, return_value):
        self.sb_return_values[(proxy_id, sb_id)] = return_value

    def set_target_config_return_value(self, proxy_id, target_string, return_value):
        target = Target(target_string)
        self.target_return_values[(proxy_id, target.name)] = return_value

    @coroutine
    def get_target_config(self, proxy_id, target_string):
        target = Target(target_string)
        raise Return(self.target_return_values[(proxy_id, target.name)])

    @coroutine
    def get_sb_config(self, proxy_id, sb_id):
        raise Return(self.sb_return_values[(proxy_id, sb_id)])

class MockKatportalClientWrapper(mock.Mock):
    @coroutine
    def get_observer_string(self, antenna):
        if re.match("^[mM][0-9]{3}$", antenna):
            raise Return("{}, -30:42:39.8, 21:26:38.0, 1035.0, 13.5".format(antenna))
        else:
            raise SensorNotFoundError("No antenna named {}".format(antenna))

    @coroutine
    def get_antenna_feng_id_map(self, instrument_name, antennas):
        ant_feng_map = {antenna:ii for ii,antenna in enumerate(antennas)}
        raise Return(ant_feng_map)

    @coroutine
    def get_bandwidth(self, stream):
        raise Return(856e6)

    @coroutine
    def get_cfreq(self, stream):
        raise Return(1.28e9)

    @coroutine
    def get_sideband(self, stream):
        raise Return("upper")

    @coroutine
    def get_sync_epoch(self):
        raise Return(1532530856)

    @coroutine
    def get_itrf_reference(self):
        raise Return((5109318.841, 2006836.367, -3238921.775))


class MockFbfKatportalMonitor(mock.Mock):
    DEFAULTS = {
        "available-antennas": "m007,m008",
        "phase-reference": "unset, radec, 0.0, 0.0",
        "bandwidth": 856000000.0,
        "nchannels": 4096,
        "centre-frequency": 1270000000.0,
        "coherent-beam-count": 32,
        "coherent-beam-count-per-group": 16,
        "coherent-beam-ngroups": 2,
        "coherent-beam-tscrunch": 16,
        "coherent-beam-fscrunch": 1,
        "coherent-beam-antennas": "m007,m008",
        "coherent-beam-multicast-groups": "spead://239.11.2.150+1:7147",
        "coherent-beam-multicast-group-mapping": "{}",
        "incoherent-beam-count": 1,
        "incoherent-beam-tscrunch": 16,
        "incoherent-beam-fscrunch": 1,
        "incoherent-beam-antennas": "m007,m008",
        "incoherent-beam-multicast-group": "spead://239.11.2.152:7147"
        }

    @coroutine
    def get_sb_config(self):
        raise Return(self.DEFAULTS)

    @coroutine
    def subscribe_to_beams(self):
        pass

    @coroutine
    def unsubscribe_from_beams(self):
        pass