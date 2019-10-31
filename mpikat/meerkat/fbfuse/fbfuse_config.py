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
from mpikat.core.utils import next_power_of_two, lcm, next_multiple

log = logging.getLogger('mpikat.fbfuse_config_manager')

FBF_BEAM_NBITS = 8
MAX_OUTPUT_RATE = 300.0e9 # bits/s -- This is an artifical limit based on the original FBF design
MAX_OUTPUT_RATE_PER_WORKER = 7.0e9 # bits/s
MAX_OUTPUT_RATE_PER_MCAST_GROUP = 7.0e9 # bits/s
MIN_MCAST_GROUPS = 32
MIN_NBEAMS = 32
MIN_ANTENNAS = 4
NBEAM_GRANULARITY = 32

class FbfConfigurationError(Exception):
    pass


class FbfConfigurationManager(object):
    def __init__(self, total_nantennas, total_bandwidth, total_nchans, nworkers, nips):
        self.total_nantennas = total_nantennas
        self.total_bandwidth = total_bandwidth
        self.total_nchans = total_nchans
        self.nworkers = nworkers
        self.nips = nips
        self.effective_nantennas = next_power_of_two(self.total_nantennas)
        if self.effective_nantennas < MIN_ANTENNAS:
            self.effective_nantennas = MIN_ANTENNAS
        self.nchans_per_worker = self.total_nchans / self.effective_nantennas
        self.nchans_per_group = self.nchans_per_worker // 4
        self.bandwidth_per_group = self.nchans_per_group * self.total_bandwidth / self.total_nchans
        self.bandwidth_per_worker = self.bandwidth_per_group * 4
        self.channel_bandwidth = self.total_bandwidth / self.total_nchans

    def _get_minimum_required_workers(self, nchans):
        return int(ceil(nchans / float(self.nchans_per_worker)))

    def _sanitise_user_nchans(self, nchans):
        if nchans < self.nchans_per_worker:
            nchans = self.nchans_per_worker
        elif nchans > self.total_nchans:
            nchans = self.total_nchans
        else:
            nchans = self.nchans_per_worker * int(ceil(nchans / float(self.nchans_per_worker)))
        return nchans

    def _max_nbeam_per_worker_by_performance(self, tscrunch, fscrunch, nantennas):
        #TODO replace with look up table
        scrunch = tscrunch * fscrunch
        if (scrunch) < 8:
            scale = 1.0/scrunch
        else:
            scale = 1.0
        nbeams = int(700*(self.nchans_per_worker/float(nantennas)) * scale) * 4
        nbeams -= nbeams%32
        return nbeams

    def _max_nbeam_per_worker_by_data_rate(self, rate_per_beam):
        scale = self.total_nchans/self.nchans_per_worker
        return int(MAX_OUTPUT_RATE_PER_WORKER / rate_per_beam) * scale

    def _valid_nbeams_per_group(self, max_nbeams_per_group, granularity, nworker_sets=1):
        valid = []
        for ii in range(1, max_nbeams_per_group+1):
            if ((ii % granularity == 0) or (granularity % ii == 0)) and (ii % nworker_sets == 0):
                valid.append(ii)
        if not valid:
            raise Exception(
                ("No valid beam counts for the selected granularity "
                 "(max per group: {}, granularity: {}, nworker_sets: {})"
                 ).format(max_nbeams_per_group, granularity, nworker_sets))
        else:
            return valid

    def _max_nbeam_by_mcast_and_granularity(self, rate_per_beam, granularity):
        max_beams_per_mcast = int(MAX_OUTPUT_RATE_PER_MCAST_GROUP / rate_per_beam)
        valid_nbeam_per_mcast = self._valid_nbeams_per_group(max_beams_per_mcast, granularity)
        max_beams_per_mcast = max(valid_nbeam_per_mcast)
        max_nbeams = self.nips * max_beams_per_mcast
        return max_nbeams

    def get_configuration(self, tscrunch, fscrunch, requested_nbeams, nantennas=None, bandwidth=None, granularity=1):
        log.info("Generating FBFUSE configuration")

        # Sanitise the user inputs with bandwidth and nantennas
        # defaulting to the full subarray and complete band
        if granularity <= 0:
            raise FbfConfigurationError("granularity must have a positive value")

        if tscrunch <= 0:
            raise FbfConfigurationError("tscrunch must have a positive value")

        if fscrunch <= 0:
            raise FbfConfigurationError("fscrunch must have a positive value")

        if not bandwidth or (bandwidth > self.total_bandwidth):
            bandwidth = self.total_bandwidth
            nchans = self.total_nchans
        else:
            nchans = self._sanitise_user_nchans(
                int(bandwidth/self.total_bandwidth * self.total_nchans))
            bandwidth = self.total_bandwidth * nchans/float(self.total_nchans)
        if not nantennas or nantennas > self.total_nantennas:
            nantennas = self.total_nantennas
        if not nchans or nchans > self.total_nchans:
            nchans = self.total_nchans
        else:
            nchans = self._sanitise_user_nchans(nchans)
        requested_nbeams = max(MIN_NBEAMS, requested_nbeams)
        log.info("Sanitised number of antennas {}".format(nantennas))
        log.info("Sanitised bandwidth to {} MHz".format(bandwidth/1e6))
        log.info("Corresponing number of channels sanitised to {}".format(nchans))
        rate_per_beam = bandwidth / tscrunch / fscrunch * FBF_BEAM_NBITS # bits/s
        log.info("Data rate per beam: {} Gb/s".format(rate_per_beam/1e9))
        total_output_rate = rate_per_beam * requested_nbeams
        log.info("Total data rate across all requested beams: {} Gb/s".format(
            total_output_rate/1e9))

        if total_output_rate > MAX_OUTPUT_RATE:
            nbeams_after_total_rate_limit = int(MAX_OUTPUT_RATE/rate_per_beam)
            log.info(
                "The requested configuration exceeds the maximum FBFUSE "
                "output rate, limiting nbeams to {}".format(
                    nbeams_after_total_rate_limit))
        else:
            nbeams_after_total_rate_limit = requested_nbeams

        if rate_per_beam > MAX_OUTPUT_RATE_PER_MCAST_GROUP:
            raise FbfConfigurationError(
                "Data rate per beam is greater than"
                " the data rate per multicast group")

        min_num_workers = self._get_minimum_required_workers(nchans)
        log.info(
            "Minimum number of workers required to support "
            "the input data rate: {}".format(min_num_workers))

        num_workers_available = min_num_workers

        # Note: This code may be reinstanted if worker sets are ever supported
        """
        num_workers_available = self.nworkers
        if min_num_workers > num_workers_available:
            raise FbfConfigurationError("Requested configuration requires at minimum {} "
                "workers, but only {} available".format(
                min_num_workers, num_workers_available))
        """
        num_worker_sets_available = num_workers_available // min_num_workers
        log.info("Number of available worker sets: {}".format(num_worker_sets_available))

        a = self._max_nbeam_per_worker_by_performance(tscrunch, fscrunch, nantennas)
        log.info("Maximum possible nbeams per worker by performance limit: {}".format(a))
        b = self._max_nbeam_per_worker_by_data_rate(rate_per_beam)

        log.info("Maximum possible nbeams per worker by data rate limit: {}".format(b))
        max_nbeams_per_worker_set = min(a, b)

        log.info("Maximum nbeams per worker: {}".format(max_nbeams_per_worker_set))
        max_nbeams_over_all_worker_sets = max_nbeams_per_worker_set * num_worker_sets_available

        mcast_beam_limit = self._max_nbeam_by_mcast_and_granularity(rate_per_beam, granularity)
        log.info("Maximum number of beams from multicast groups and granularity: {}".format(
            mcast_beam_limit))
        if mcast_beam_limit < MIN_NBEAMS:
            raise FbfConfigurationError("Multicast beam limit ({}) less than minimum generatable beams ({})".format(
                mcast_beam_limit, MIN_NBEAMS))

        nbeams_after_mcast_limit = min(nbeams_after_total_rate_limit, mcast_beam_limit, max_nbeams_over_all_worker_sets)
        log.info("Maximum number of beams after accounting for multicast limit: {}".format(
            nbeams_after_mcast_limit))

        num_required_worker_sets = int(ceil(nbeams_after_mcast_limit / float(max_nbeams_per_worker_set)))
        log.info("Number of required worker sets: {}".format(
            num_required_worker_sets))

        num_worker_sets_to_be_used = min(num_required_worker_sets, num_required_worker_sets, num_worker_sets_available)
        log.info("Number of worker sets to be used: {}".format(
            num_worker_sets_to_be_used))

        num_beams_per_worker_set = nbeams_after_mcast_limit//num_worker_sets_to_be_used
        log.info("Updated number of beams per worker set: {}".format(
            num_beams_per_worker_set))

        nbeams_after_mcast_limit = num_worker_sets_to_be_used * num_beams_per_worker_set
        log.info("Updated total output number of beams to: {}".format(
            nbeams_after_mcast_limit))

        max_nbeams_per_group = int(MAX_OUTPUT_RATE_PER_MCAST_GROUP / rate_per_beam)
        valid_nbeam_per_mcast = self._valid_nbeams_per_group(
            max_nbeams_per_group, granularity,
            nworker_sets=num_worker_sets_to_be_used)
        log.info("Valid numbers of beams per multicast group: {}".format(valid_nbeam_per_mcast))

        max_valid_nbeam_per_mcast = max(valid_nbeam_per_mcast)
        max_valid_nbeam_per_mcast_idx = len(valid_nbeam_per_mcast)-1
        log.info("Max valid numbers of beams per multicast group: {}".format(max_valid_nbeam_per_mcast))

        num_mcast_required_per_worker_set = int(floor(num_beams_per_worker_set / float(max_valid_nbeam_per_mcast)))
        log.info("Number of multicast groups required per worker set: {}".format(num_mcast_required_per_worker_set))

        num_mcast_required = num_mcast_required_per_worker_set * num_worker_sets_to_be_used
        log.info("Total number of multicast groups required: {}".format(num_mcast_required))

        while (num_mcast_required < MIN_MCAST_GROUPS):
            log.debug("Too few multicast groups used, trying fewer beams per group")
            max_valid_nbeam_per_mcast_idx -= 1
            max_valid_nbeam_per_mcast = valid_nbeam_per_mcast[max_valid_nbeam_per_mcast_idx]
            log.debug("Testing {} beams per group".format(max_valid_nbeam_per_mcast))
            num_mcast_required = int(ceil(nbeams_after_mcast_limit / max_valid_nbeam_per_mcast))
            log.debug("{} groups required".format(num_mcast_required))

        # Final check should be over the number of beams
        log.info("Finding common multiples for the total beam and beam per multicast granularity")
        group_corrected_nbeams = num_mcast_required * max_valid_nbeam_per_mcast
        final_nbeams = next_multiple(group_corrected_nbeams, lcm(
            NBEAM_GRANULARITY, max_valid_nbeam_per_mcast))
        final_num_mcast = final_nbeams // max_valid_nbeam_per_mcast
        if final_nbeams % num_worker_sets_to_be_used !=0:
            raise Exception(
                "Error during configuration, expected number of "
                "beams ({}) to be a multiple of number of worker sets ({})".format(
                    final_nbeams, num_worker_sets_to_be_used))
        num_beams_per_worker_set = final_nbeams / num_worker_sets_to_be_used
        config = {
            "num_beams":final_nbeams,
            "num_chans":nchans,
            "num_mcast_groups":final_num_mcast,
            "num_beams_per_mcast_group":max_valid_nbeam_per_mcast,
            "num_workers_per_set":min_num_workers,
            "num_worker_sets":num_worker_sets_to_be_used,
            "num_workers_total":num_worker_sets_to_be_used*min_num_workers,
            "num_beams_per_worker_set": num_beams_per_worker_set,
            "data_rate_per_group": rate_per_beam * max_valid_nbeam_per_mcast,
            "used_bandwidth": bandwidth
        }
        log.info("Final coniguration: {}".format(config))
        return config

