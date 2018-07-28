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
from math import floor, ceil
from mpikat.utils import next_power_of_two

log = logging.getLogger('mpikat.fbfuse_config_manager')

FBF_BEAM_NBITS = 8
MAX_OUTPUT_RATE = 300.0e9 # bits/s -- This is an artifical limit based on the original FBF design
MAX_OUTPUT_RATE_PER_WORKER = 30.0e9 # bits/s
MAX_OUTPUT_RATE_PER_MCAST_GROUP = 7.0e9 # bits/s
MIN_MCAST_GROUPS = 16

class FbfConfigurationError(Exception):
    pass

class FbfConfigurationManager(object):
    def __init__(self, total_nantennas, total_bandwidth, total_nchans, worker_pool, ip_pool):
        self.total_nantennas = total_nantennas
        self.total_bandwidth = total_bandwidth
        self.total_nchans = total_nchans
        self.worker_pool = worker_pool
        self.ip_pool = ip_pool
        self.effective_nantennas = next_power_of_two(self.total_nantennas)
        self.nchans_per_worker = self.total_nchans / self.effective_nantennas

    def _get_minimum_required_workers(self, nchans):
        return int(ceil(nchans / float(self.nchans_per_worker)))

    def _sanitise_user_nchans(self, nchans):
        if nchans < self.nchans_per_worker:
            nchans = self.nchans_per_worker
        elif nchans > self.total_nchans:
            raise FbfConfigurationError("Requested more channels than are available")
        else:
            nchans = self.nchans_per_worker * int(ceil(nchans / float(self.nchans_per_worker)))
        return nchans

    def _max_nbeam_per_worker_by_performance(self, tscrunch, fscrunch, nantennas):
        return 1000

    def _max_nbeam_per_worker_by_data_rate(self, rate_per_beam):
        scale = self.total_nchans/self.nchans_per_worker
        return int(MAX_OUTPUT_RATE_PER_WORKER / rate_per_beam) * scale

    def _valid_nbeams_per_group(self, max_nbeams_per_group, granularity):
        valid = []
        for ii in range(1, max_nbeams_per_group+1):
            if (ii%granularity == 0) or (granularity%ii == 0):
                valid.append(ii)
        if not valid:
            raise Exception("No valid beam counts for the selected granularity")
        else:
            return valid

    def _max_nbeam_by_mcast_and_granularity(self, rate_per_beam, granularity):
        max_beams_per_mcast = int(MAX_OUTPUT_RATE_PER_MCAST_GROUP / rate_per_beam)
        valid_nbeam_per_mcast = self._valid_nbeams_per_group(max_beams_per_mcast, granularity)
        max_beams_per_mcast = max(valid_nbeam_per_mcast)
        return self.ip_pool.largest_free_range() * max_beams_per_mcast

    def get_configuration(self, tscrunch, fscrunch, requested_nbeams, nantennas=None, nchans=None, granularity=1):
        log.debug("Generating FBFUSE configuration")

        # Sanitise the user inputs with nchans and nantennas
        # defaulting to the full subarray and complete band
        if not nantennas:
            nantennas = self.total_nantennas
        if not nchans:
            nchans = self.total_nchans
        else:
            nchans = self._sanitise_user_nchans(nchans)
        log.debug("Requested number of antennas {}".format(nantennas))
        log.debug("Requested number of channels sanitised to {}".format(nchans))

        # Calculate the bandwidth, data rate per beam and total output rate
        # of the instrument.
        bandwidth = self.total_bandwidth * (nchans/float(self.total_nchans))
        log.debug("Corresponding bandwidth {} MHz".format(bandwidth/1e6))
        rate_per_beam = bandwidth / tscrunch / fscrunch * FBF_BEAM_NBITS # bits/s
        log.debug("Data rate per beam: {} Gb/s".format(rate_per_beam/1e9))
        total_output_rate = rate_per_beam * requested_nbeams
        log.debug("Total data rate across all requested beams: {} Gb/s".format(
            total_output_rate/1e9))

        if total_output_rate > MAX_OUTPUT_RATE:
            nbeams_after_total_rate_limit = int(MAX_OUTPUT_RATE/rate_per_beam)
            log.warning("The requested configuration exceeds the maximum FBFUSE "
                "output rate, limiting nbeams to {}".format(nbeams_after_total_rate_limit))
        else:
            nbeams_after_total_rate_limit = requested_nbeams

        if rate_per_beam > MAX_OUTPUT_RATE_PER_MCAST_GROUP:
            raise FbfConfigurationError("Data rate per beam is greater than"
                " the data rate per multicast group")

        min_num_workers = self._get_minimum_required_workers(nchans)
        log.debug("Minimum number of workers required to support "
            "the input data rate: {}".format(min_num_workers))

        num_workers_available = self.worker_pool.navailable()
        if min_num_workers > num_workers_available:
            raise FbfConfigurationError("Requested configuration requires at minimum {} "
                "workers, but only {} available".format(
                min_num_workers, num_workers_available))
        num_worker_sets_available = num_workers_available // min_num_workers
        log.debug("Number of available worker sets: {}".format(num_worker_sets_available))

        a = self._max_nbeam_per_worker_by_performance(tscrunch, fscrunch, nantennas)
        log.debug("Maximum possible nbeams per worker by performance limit: {}".format(a))
        b = self._max_nbeam_per_worker_by_data_rate(rate_per_beam)
        log.debug("Maximum possible nbeams per worker by data rate limit: {}".format(b))
        max_nbeams_per_worker_set = min(a, b)
        log.debug("Maximum nbeams per worker: {}".format(max_nbeams_per_worker_set))
        max_nbeams_over_all_worker_sets = max_nbeams_per_worker_set * num_worker_sets_available

        mcast_beam_limit = self._max_nbeam_by_mcast_and_granularity(rate_per_beam, granularity)
        log.debug("Maximum number of beams from multicast groups and granularity: {}".format(
            mcast_beam_limit))

        nbeams_after_mcast_limit = min(nbeams_after_total_rate_limit, mcast_beam_limit, max_nbeams_over_all_worker_sets)
        log.debug("Maximum number of beams after accounting for multicast limit: {}".format(
            nbeams_after_mcast_limit))

        num_required_worker_sets = int(ceil(nbeams_after_mcast_limit / float(max_nbeams_per_worker_set)))
        log.debug("Number of required worker sets: {}".format(
            num_required_worker_sets))

        num_worker_sets_to_be_used = min(num_required_worker_sets, num_required_worker_sets, num_worker_sets_available)
        log.debug("Number of worker sets to be used: {}".format(
            num_worker_sets_to_be_used))

        num_beams_per_worker_set = nbeams_after_mcast_limit//num_worker_sets_to_be_used
        log.debug("Updated number of beams per worker set: {}".format(
            num_beams_per_worker_set))

        nbeams_after_mcast_limit = num_worker_sets_to_be_used * num_beams_per_worker_set
        log.debug("Updated total output number of beams to: {}".format(
            nbeams_after_mcast_limit))

        max_nbeams_per_group = int(MAX_OUTPUT_RATE_PER_MCAST_GROUP / rate_per_beam)
        valid_nbeam_per_mcast = self._valid_nbeams_per_group(max_nbeams_per_group, granularity)
        log.debug("Valid numbers of beams per multicast group: {}".format(valid_nbeam_per_mcast))

        max_valid_nbeam_per_mcast = max(valid_nbeam_per_mcast)
        max_valid_nbeam_per_mcast_idx = len(valid_nbeam_per_mcast)-1
        log.debug("Max valid numbers of beams per multicast group: {}".format(max_valid_nbeam_per_mcast))

        num_mcast_required_per_worker_set = int(floor(num_beams_per_worker_set / float(max_valid_nbeam_per_mcast)))
        log.debug("Number of multicast groups required per worker set: {}".format(num_mcast_required_per_worker_set))

        num_mcast_required = num_mcast_required_per_worker_set * num_worker_sets_to_be_used
        log.debug("Total number of multicast groups required: {}".format(num_mcast_required))

        while (num_mcast_required < MIN_MCAST_GROUPS):
            log.debug("Too few multicast groups used, trying fewer beams per group")
            max_valid_nbeam_per_mcast_idx -= 1
            max_valid_nbeam_per_mcast = valid_nbeam_per_mcast[max_valid_nbeam_per_mcast_idx]
            log.debug("Testing {} beams per group".format(max_valid_nbeam_per_mcast))
            num_mcast_required = int(ceil(nbeams_after_mcast_limit / max_valid_nbeam_per_mcast + 0.5))
            log.debug("{} groups required".format(num_mcast_required))

        # Final check should be over the number of beams
        final_nbeams = num_mcast_required * max_valid_nbeam_per_mcast
        config = {
            "num_beams":final_nbeams,
            "num_chans":nchans,
            "num_mcast_groups":num_mcast_required,
            "num_beams_per_mcast_group":max_valid_nbeam_per_mcast,
            "num_workers_per_set":min_num_workers,
            "num_worker_sets":num_worker_sets_to_be_used,
        }
        return config



