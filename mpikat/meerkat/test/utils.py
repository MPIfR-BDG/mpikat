import mock
import re
import json
from tornado.gen import coroutine, Return, sleep
from tornado.testing import AsyncTestCase
from katcp.testutils import mock_req, handle_mock_req
from katpoint import Target
from mpikat.meerkat.fbfuse import BaseFbfConfigurationAuthority
from mpikat.meerkat.test.antennas import ANTENNAS


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
    def get_observer_strings(self, antennas):
        for antenna in antennas:
            observers = {}
            if re.match("^[mM][0-9]{3}$", antenna):
                observers[antenna] = "{}, -30:42:39.8, 21:26:38.0, 1035.0, 13.5".format(antenna)
            else:
                raise SensorNotFoundError("No antenna named {}".format(antenna))
        raise Return(observers)

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
        raise Return(12312312, 123123123, 12312312)

    @coroutine
    def get_proposal_id(self):
        raise Return("USE-TEST-USE")

    @coroutine
    def get_sb_id(self):
        raise Return("20191100-0001")

    @coroutine
    def get_fbfuse_address(self):
        raise Return(("127.0.0.1", 5000))

    def get_sensor_tracker(self, component, sensor_name):
        return mock.Mock()

    @coroutine
    def get_fbfuse_sb_config(self, product_id):
        config = {'phase-reference': "source,radec,00:00:00,00:00:00",
                  'bandwidth': 856000000.0,
                  'centre-frequency': 1280000000.0,
                  'coherent-beam-antennas': '',
                  'coherent-beam-count': 36,
                  'coherent-beam-count-per-group': 6,
                  'coherent-beam-fscrunch': 1,
                  'coherent-beam-heap-size': 8192,
                  'coherent-beam-idx1-step': 1232131123,
                  'coherent-beam-multicast-group-mapping': json.dumps({
                      'spead://239.11.1.1:7147': [
                          'cfbf00000',
                          'cfbf00001',
                          'cfbf00002',
                          'cfbf00003',
                          'cfbf00004',
                          'cfbf00005'],
                      'spead://239.11.1.2:7147': [
                          'cfbf00006',
                          'cfbf00007',
                          'cfbf00008',
                          'cfbf00009',
                          'cfbf00010',
                          'cfbf00011'],
                      'spead://239.11.1.3:7147': [
                          'cfbf00012',
                          'cfbf00013',
                          'cfbf00014',
                          'cfbf00015',
                          'cfbf00016',
                          'cfbf00017'],
                      'spead://239.11.1.4:7147': [
                          'cfbf00018',
                          'cfbf00019',
                          'cfbf00020',
                          'cfbf00021',
                          'cfbf00022',
                          'cfbf00023'],
                      'spead://239.11.1.5:7147': [
                          'cfbf00024',
                          'cfbf00025',
                          'cfbf00026',
                          'cfbf00027',
                          'cfbf00028',
                          'cfbf00029'],
                      'spead://239.11.1.6:7147': [
                          'cfbf00030',
                          'cfbf00031',
                          'cfbf00032',
                          'cfbf00033',
                          'cfbf00034',
                          'cfbf00035']}),
                  'coherent-beam-multicast-groups': 'spead://239.11.1.1+5:7147',
                  'coherent-beam-multicast-groups-data-rate': 600000000.0,
                  'coherent-beam-ngroups': 6,
                  'coherent-beam-subband-nchans': 16,
                  'coherent-beam-time-resolution': 7.9e-05,
                  'coherent-beam-tscrunch': 16,
                  'incoherent-beam-antennas': '',
                  'incoherent-beam-count': 1,
                  'incoherent-beam-fscrunch': 1,
                  'incoherent-beam-heap-size': 8192,
                  'incoherent-beam-idx1-step': 1232131123,
                  'incoherent-beam-multicast-group': 'spead://239.11.1.0:7147',
                  'incoherent-beam-multicast-group-data-rate': 100000000.0,
                  'incoherent-beam-subband-nchans': 16,
                  'incoherent-beam-time-resolution': 7.9e-05,
                  'incoherent-beam-tscrunch': 16,
                  'nchannels': 4096}
        raise Return(config)

    @coroutine
    def get_fbfuse_proxy_id(self):
        raise Return("fbfuse_1")

    @coroutine
    def get_fbfuse_coherent_beam_positions(self, product_id):
        beams = {}
        for ii in range(128):
            beams["cfbf{:05d}".format(ii)] = "source0,radec,00:00:00,00:00:00"
        return beams

    def get_sensor_tracker(self, component, sensor_name):
        return DummySensorTracker()


class DummySensorTracker(object):
    def __init__(self, *args, **kwargs):
        pass

    @coroutine
    def start(self):
        pass

    @coroutine
    def wait_until(self, state, interrupt):
        yield sleep(10)

