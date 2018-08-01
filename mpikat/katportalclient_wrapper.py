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
import logging
from tornado.gen import coroutine, Return
from katportalclient import KATPortalClient

log = logging.getLogger('mpikat.katportalclient_wrapper')

class KatportalClientWrapper(object):
    def __init__(self, host, sub_nr=1):
        self._client = KATPortalClient('http://{host}/api/client/{sub_nr}'.format(
            host=host, sub_nr=sub_nr),
            on_update_callback=None, logger=logging.getLogger('katcp'))

    @coroutine
    def _query(self, component, sensor):
        sensor_name = yield self._client.sensor_subarray_lookup(
            component=component, sensor=sensor, return_katcp_name=False)
        sensor_sample = yield self._client.sensor_value(sensor_name,
            include_value_ts=False)
        raise Return(sensor_sample)

    @coroutine
    def get_observer_string(self, antenna):
        sensor_sample = yield self._query(antenna, "observer")
        raise Return(sensor_sample.value)

    @coroutine
    def get_antenna_feng_id_map(self, instrument_name, antennas):
        sensor_sample = yield self._query('cbf', '{}.input-labelling'.format(instrument_name))
        labels = eval(sensor_sample.value)
        mapping = {}
        for input_label, input_index, _, _ in labels:
            antenna_name = input_label.strip("vh").lower()
            if antenna_name.startswith("m") and antenna_name in antennas:
                mapping[antenna_name] = input_index//2
        print mapping
        raise Return(mapping)

    @coroutine
    def get_bandwidth(self, stream):
        sensor_sample = yield self._query('sub', 'streams.{}.bandwidth'.format(stream))
        raise Return(sensor_sample.value)

    @coroutine
    def get_cfreq(self, stream):
        sensor_sample = yield self._query('sub', 'streams.{}.centre-frequency'.format(stream))
        raise Return(sensor_sample.value)

    @coroutine
    def get_sideband(self, stream):
        sensor_sample = yield self._query('sub', 'streams.{}.sideband'.format(stream))
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