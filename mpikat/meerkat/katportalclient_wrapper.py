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
import json
import logging
import uuid
import time
import datetime
from tornado.gen import coroutine, Return, sleep, TimeoutError
from katportalclient import KATPortalClient
from katcp.resource import KATCPSensorReading
from katcp import Sensor

log = logging.getLogger('mpikat.katportalclient_wrapper')


class KatportalClientWrapper(object):
    def __init__(self, host, callback=None):
        self._host = host
        self._client = KATPortalClient(
            host,
            on_update_callback=callback,
            logger=logging.getLogger('katcp'))

    @coroutine
    def _query(self, component, sensor):
        log.debug("Querying sensor '{}' on component '{}'".format(
            sensor, component))
        sensor_name = yield self._client.sensor_subarray_lookup(
            component=component, sensor=sensor, return_katcp_name=False)
        log.debug("Found sensor name: {}".format(sensor_name))
        sensor_sample = yield self._client.sensor_value(
            sensor_name,
            include_value_ts=False)
        log.debug("Sensor value: {}".format(sensor_sample))
        raise Return(sensor_sample)

    @coroutine
    def get_observer_string(self, antenna):
        sensor_sample = yield self._query(antenna, "observer")
        raise Return(sensor_sample.value)

    @coroutine
    def get_observer_strings(self, antennas):
        query = "^({})_observer".format("|".join(antennas))
        log.debug("Regex query '{}'".format(query))
        sensor_samples = yield self._client.sensor_values(
            query, include_value_ts=False)
        log.debug("Sensor value: {}".format(sensor_samples))
        antennas = {}
        for key, value in sensor_samples.items():
            antennas[key.strip("_observer")] = value.value
        raise Return(antennas)

    @coroutine
    def get_antenna_feng_id_map(self, instrument_name, antennas):
        sensor_sample = yield self._query('cbf', '{}.input-labelling'.format(
            instrument_name))
        labels = eval(sensor_sample.value)
        mapping = {}
        for input_label, input_index, _, _ in labels:
            antenna_name = input_label.strip("vh").lower()
            if antenna_name.startswith("m") and antenna_name in antennas:
                mapping[antenna_name] = input_index//2
        raise Return(mapping)

    @coroutine
    def get_bandwidth(self, stream):
        sensor_sample = yield self._query('sub', 'streams.{}.bandwidth'.format(
            stream))
        raise Return(sensor_sample.value)

    @coroutine
    def get_cfreq(self, stream):
        sensor_sample = yield self._query(
            'sub', 'streams.{}.centre-frequency'.format(stream))
        raise Return(sensor_sample.value)

    @coroutine
    def get_sideband(self, stream):
        sensor_sample = yield self._query(
            'sub', 'streams.{}.sideband'.format(stream))
        raise Return(sensor_sample.value)

    @coroutine
    def get_sync_epoch(self):
        sensor_sample = yield self._query('sub', 'synchronisation-epoch')
        raise Return(sensor_sample.value)

    @coroutine
    def get_itrf_reference(self):
        sensor_sample = yield self._query('sub', 'array-position-itrf')
        x, y, z = [float(i) for i in sensor_sample.value.split(",")]
        raise Return((x, y, z))

    @coroutine
    def get_proposal_id(self):
        sensor_sample = yield self._query('sub', 'script-proposal-id')
        raise Return(sensor_sample.value)

    @coroutine
    def get_sb_id(self):
        sensor_sample = yield self._query('sub', 'experiment-id')
        raise Return(sensor_sample.value)

    @coroutine
    def get_fbfuse_address(self):
        sensor_sample = yield self._query('fbfuse', 'fbfmc-address')
        raise Return(sensor_sample.value)

    @coroutine
    def get_fbfuse_sb_config(self, product_id):
        sensor_list = [
            "available-antennas",
            "phase-reference",
            "bandwidth",
            "nchannels",
            "centre-frequency",
            "coherent-beam-count",
            "coherent-beam-count-per-group",
            "coherent-beam-ngroups",
            "coherent-beam-tscrunch",
            "coherent-beam-fscrunch",
            "coherent-beam-antennas",
            "coherent-beam-multicast-groups",
            "coherent-beam-multicast-group-mapping",
            "coherent-beam-multicast-groups-data-rate",
            "coherent-beam-heap-size",
            "coherent-beam-idx1-step",
            "coherent-beam-subband-nchans",
            "coherent-beam-time-resolution",
            "incoherent-beam-count",
            "incoherent-beam-tscrunch",
            "incoherent-beam-fscrunch",
            "incoherent-beam-antennas",
            "incoherent-beam-multicast-group",
            "incoherent-beam-multicast-group-data-rate",
            "incoherent-beam-heap-size",
            "incoherent-beam-idx1-step",
            "incoherent-beam-subband-nchans",
            "incoherent-beam-time-resolution"
        ]
        fbf_config = {}
        for key in sensor_list:
            try:
                log.debug("Fetching: {}".format(key))
                sensor_sample = yield self._query('fbfuse', 'fbfmc.{}.{}'.format(
                    product_id, key))
                fbf_config[key] = sensor_sample.value
            except Exception as error:
                log.exception("Could not retrieve {} from fbfuse".format(key))
                raise error
        raise Return(fbf_config)

    def get_sensor_tracker(self, component, sensor_name):
        return SensorTracker(self._host, component, sensor_name)


class Interrupt(Exception):
    pass


class SensorTracker(object):
    def __init__(self, host, component, sensor_name):
        log.debug(("Building sensor tracker activity tracker "
                   "on {} for sensor={} and component={}").format(
            host, sensor_name, component))
        self._client = KATPortalClient(
            host,
            on_update_callback=self.event_handler,
            logger=logging.getLogger('katcp'))
        self._namespace = 'namespace_' + str(uuid.uuid4())
        self._sensor_name = sensor_name
        self._component = component
        self._full_sensor_name = None
        self._state = None
        self._has_started = False

    @coroutine
    def start(self):
        if self._has_started:
            return
        log.debug("Starting sensor tracker")
        yield self._client.connect()
        result = yield self._client.subscribe(self._namespace)
        self._full_sensor_name = yield self._client.sensor_subarray_lookup(
            component=self._component, sensor=self._sensor_name,
            return_katcp_name=False)
        log.debug("Tracking sensor: {}".format(self._full_sensor_name))
        result = yield self._client.set_sampling_strategies(
            self._namespace, self._full_sensor_name,
            'event')
        sensor_sample = yield self._client.sensor_value(
            self._full_sensor_name,
            include_value_ts=False)
        self._state = sensor_sample.value
        log.debug("Initial state: {}".format(self._state))
        self._has_started = True

    def event_handler(self, msg_dict):
        status = msg_dict['msg_data']['status']
        if status == "nominal":
            log.debug("Sensor value update: {} -> {}".format(
                self._state, msg_dict['msg_data']['value']))
            self._state = msg_dict['msg_data']['value']

    @coroutine
    def wait_until(self, state, interrupt):
        log.debug("Waiting for state='{}'".format(state))
        while True:
            if self._state == state:
                log.debug("Desired state reached")
                raise Return(self._state)
            else:
                try:
                    log.debug("Waiting on interrupt in wait_until loop")
                    yield interrupt.wait(
                        timeout=datetime.timedelta(seconds=1))
                    log.debug("Moving to next loop iteration")
                except TimeoutError:
                    continue
                else:
                    log.debug("Wait was interrupted")
                    raise Interrupt("Interrupt event was set")


class SubarrayActivity(SensorTracker):
    def __init__(self, host):
        super(SubarrayActivity, self).__init__(
            host, "subarray", "observation_activity")


if __name__ == "__main__":
    import tornado
    host = "http://portal.mkat.karoo.kat.ac.za/api/client/1"
    log = logging.getLogger('mpikat.katportalclient_wrapper')
    log.setLevel(logging.DEBUG)
    ioloop = tornado.ioloop.IOLoop.current()


    client = KatportalClientWrapper(host)

    x = SubarrayActivity(host)

    event = tornado.locks.Event()

    @coroutine
    def setup():
        yield x.start()
        """
        try:
            yield x.wait_until("track", event)
        except Exception as error:
            print "exception for wait_until"
            print str(error)
        else:
            print "Desired state reached"
            """

    @coroutine
    def set_event():
        print "Setting event wait"
        event.set()

    ioloop.add_callback(setup)
    #ioloop.add_callback(set_event)

    ioloop.start()
