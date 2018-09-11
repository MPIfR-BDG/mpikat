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
from mpikat.fbfuse_config import FbfConfigurationManager, MIN_NBEAMS, FbfConfigurationError

NBEAMS_OVERFLOW_TOLERANCE = 0.05 # 5%

def make_ip_pool_mock(nips):
    ip_pool = mock.Mock()
    ip_pool.largest_free_range.return_value = nips
    return ip_pool

def make_worker_pool_mock(nworkers):
    worker_pool = mock.Mock()
    worker_pool.navailable.return_value = nworkers
    return worker_pool

class TestFbfConfigurationManager(unittest.TestCase):
    def _verify_configuration(self, cm, tscrunch, fscrunch, bandwidth,
        nbeams, nantennas, granularity):
        max_allowable_nbeams = nbeams+NBEAMS_OVERFLOW_TOLERANCE*nbeams
        if max_allowable_nbeams < MIN_NBEAMS:
            max_allowable_nbeams = MIN_NBEAMS
        min_allowable_nbeams = MIN_NBEAMS
        config = cm.get_configuration(tscrunch, fscrunch, nbeams, nantennas, bandwidth, granularity)
        self.assertTrue(config['num_beams'] <= max_allowable_nbeams,
            "Actual number of beams {}".format(config['num_beams']))
        self.assertTrue(config['num_beams'] >= min_allowable_nbeams)
        self.assertTrue(config['num_mcast_groups'] <= cm.nips)
        self.assertTrue(config['num_workers_total'] <= cm.nworkers)
        nbpmg = config['num_beams_per_mcast_group']
        self.assertTrue((nbpmg%granularity==0) or (granularity%nbpmg==0))
        nchans = cm._sanitise_user_nchans(int(bandwidth / cm.total_bandwidth * cm.total_nchans))
        self.assertEqual(config['num_chans'], nchans)

    def test_full_ranges(self):
        cm = FbfConfigurationManager(64, 856e6, 4096, 64, 128)
        bandwidths = [10e6,100e6,1000e6]
        antennas = [1, 4, 13, 16, 26, 32, 33, 56, 64]
        granularities = [1,2,5,6]
        nbeams = [1,3,40,100,2000,100000]
        for bandwidth in bandwidths:
            for antenna in antennas:
                for granularity in granularities:
                    for nbeam in nbeams:
                        self._verify_configuration(cm, 16, 1, bandwidth, nbeam, antenna, granularity)

    def test_invalid_nantennas(self):
        cm = FbfConfigurationManager(64, 856e6, 4096, 64, 128)
        with self.assertRaises(FbfConfigurationError):
            self._verify_configuration(cm, 16, 1, 856e6, 400, 32, 0)

    def test_no_remaining_workers(self):
        cm = FbfConfigurationManager(64, 856e6, 4096, 0, 128)
        with self.assertRaises(FbfConfigurationError):
            self._verify_configuration(cm, 16, 1, 856e6, 400, 32, 1)

    def test_no_remaining_groups(self):
        cm = FbfConfigurationManager(64, 856e6, 4096, 64, 0)
        with self.assertRaises(FbfConfigurationError):
            self._verify_configuration(cm, 16, 1, 856e6, 400, 32, 1)

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    log = logging.getLogger('')
    log.setLevel(logging.WARN)
    unittest.main(buffer=True)