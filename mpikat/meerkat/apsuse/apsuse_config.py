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
from mpikat.core.ip_manager import ip_range_from_stream

log = logging.getLogger('mpikat.apsuse_config_manager')

DATA_RATE_PER_WORKER = 10e9  # bits / s


DUMMY_FBF_CONFIG = {
    "coherent-beam-multicast-groups":"spead://239.11.1.15+15:7147",
    "coherent-beam-multicast-groups-data-rate": 7e9,
    "incoherent-beam-multicast-group": "spead://239.11.1.14:7147",
    "incoherent-beam-multicast-group-data-rate": 150e6,
}


class ApsConfigurationError(Exception):
    pass


class ApsWorkerBandwidthExceeded(Exception):
    pass


class ApsWorkerTotalBandwidthExceeded(Exception):
    pass


class ApsWorkerConfig(object):
    def __init__(self, total_bandwidth=DATA_RATE_PER_WORKER):
        log.debug("Created new apsuse worker config")
        self._total_bandwidth = total_bandwidth
        self._available_bandwidth = self._total_bandwidth
        self._incoherent_groups = []
        self._coherent_groups = []
        self._incoherent_beams = []
        self._coherent_beams = []

    def add_incoherent_group(self, group, bandwidth):
        if bandwidth > self._total_bandwidth:
            log.debug("Adding group would exceed worker bandwidth")
            raise ApsWorkerTotalBandwidthExceeded

        if self._available_bandwidth < bandwidth:
            log.debug("Adding group would exceed worker bandwidth")
            raise ApsWorkerBandwidthExceeded
        else:
            log.debug("Adding group {} to worker".format(group))
            self._incoherent_groups.append(group)
            self._available_bandwidth -= bandwidth

    def add_coherent_group(self, group, bandwidth):
        if self._available_bandwidth < bandwidth:
            log.debug("Adding group would exceed worker bandwidth")
            raise ApsWorkerBandwidthExceeded
        else:
            self._coherent_groups.append((group))
            log.debug("Adding group {} to worker".format(group))
            self._available_bandwidth -= bandwidth

    def data_rate(self):
        return self._total_bandwidth - self._available_bandwidth

    def coherent_groups(self):
        return self._coherent_groups

    def incoherent_groups(self):
        return self._incoherent_groups

    def coherent_beams(self):
        return self._coherent_beams

    def incoherent_beams(self):
        return self._incoherent_beams


def get_required_workers(fbfuse_config):
    workers = []
    current_worker = ApsWorkerConfig()
    # There is a slightly sketchy assumption here that there will only ever
    # be one multicast group for the incoherent beam
    incoherent_range = ip_range_from_stream(fbfuse_config['incoherent-beam-multicast-group'])
    incoherent_mcast_group_rate = fbfuse_config['incoherent-beam-multicast-group-data-rate']

    for group in incoherent_range:
        try:
            current_worker.add_incoherent_group(group, incoherent_mcast_group_rate)
        except (ApsWorkerTotalBandwidthExceeded, ApsWorkerBandwidthExceeded):
            log.error("Incoherent beam mutlicast group ({} Gb/s) size exceeds data rate for one node ({} Gb/s)".format(
                incoherent_mcast_group_rate/1e9, current_worker._total_bandwidth/1e9))
            log.error("Incoherent beam data will not be captured")
            break
    coherent_range = ip_range_from_stream(fbfuse_config['coherent-beam-multicast-groups'])
    coherent_mcast_group_rate = fbfuse_config['coherent-beam-multicast-groups-data-rate']
    for group in coherent_range:
        try:
            current_worker.add_coherent_group(group, coherent_mcast_group_rate)
        except ApsWorkerTotalBandwidthExceeded:
            log.error("Coherent beam mutlicast group ({} Gb/s) size exceeds data rate for one node ({} Gb/s)".format(
                coherent_mcast_group_rate/1e9, current_worker._total_bandwidth/1e9))
            log.error("Coherent beam data will not be captured")
            break
        except ApsWorkerBandwidthExceeded:
            workers.append(current_worker)
            current_worker = ApsWorkerConfig()
            current_worker.add_coherent_group(group, coherent_mcast_group_rate)
    else:
        workers.append(current_worker)

    # Get all beam mappings
    for worker in workers:
        for incoherent_group in worker.incoherent_groups():
            worker._incoherent_beams.append("ifbf00000")
        for coherent_group in worker.coherent_groups():
            spead_formatted = "spead://{}:{}".format(str(coherent_group), coherent_range.port)
            beam_idxs = fbfuse_config['coherent-beam-multicast-group-mapping'][spead_formatted]
            worker._coherent_beams.extend(beam_idxs)

    for ii, worker in enumerate(workers):
        log.debug(("Worker {} config: coherent-groups: {},"
                   " coherent-beams: {}, incoherent-groups: {},"
                   " incoherent-beams: {},").format(
                   ii, map(str, worker.coherent_groups()),
                   map(str, worker.coherent_beams()),
                   map(str, worker.incoherent_groups()),
                   map(str, worker.incoherent_beams())))

    return workers





