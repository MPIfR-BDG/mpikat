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
import struct
import numpy as np

log = logging.getLogger("mpikat.fbfuse_feng_subscription_manager")

NSPINES = 16
NLEAVES = 4
MAX_SUBS_PER_LEAF = 4
HOST_TO_LEAF_MAP = {
    "fbfpn00.mpifr-be.mkat.karoo.kat.ac.za": 0,
    "fbfpn01.mpifr-be.mkat.karoo.kat.ac.za": 0,
    "fbfpn02.mpifr-be.mkat.karoo.kat.ac.za": 0,
    "fbfpn03.mpifr-be.mkat.karoo.kat.ac.za": 0,
    "fbfpn04.mpifr-be.mkat.karoo.kat.ac.za": 0,
    "fbfpn05.mpifr-be.mkat.karoo.kat.ac.za": 0,
    "fbfpn06.mpifr-be.mkat.karoo.kat.ac.za": 0,
    "fbfpn07.mpifr-be.mkat.karoo.kat.ac.za": 0,
    "fbfpn08.mpifr-be.mkat.karoo.kat.ac.za": 1,
    "fbfpn09.mpifr-be.mkat.karoo.kat.ac.za": 1,
    "fbfpn10.mpifr-be.mkat.karoo.kat.ac.za": 1,
    "fbfpn11.mpifr-be.mkat.karoo.kat.ac.za": 1,
    "fbfpn12.mpifr-be.mkat.karoo.kat.ac.za": 1,
    "fbfpn13.mpifr-be.mkat.karoo.kat.ac.za": 1,
    "fbfpn14.mpifr-be.mkat.karoo.kat.ac.za": 1,
    "fbfpn15.mpifr-be.mkat.karoo.kat.ac.za": 1,
    "fbfpn16.mpifr-be.mkat.karoo.kat.ac.za": 2,
    "fbfpn17.mpifr-be.mkat.karoo.kat.ac.za": 2,
    "fbfpn18.mpifr-be.mkat.karoo.kat.ac.za": 2,
    "fbfpn19.mpifr-be.mkat.karoo.kat.ac.za": 2,
    "fbfpn20.mpifr-be.mkat.karoo.kat.ac.za": 2,
    "fbfpn21.mpifr-be.mkat.karoo.kat.ac.za": 2,
    "fbfpn22.mpifr-be.mkat.karoo.kat.ac.za": 2,
    "fbfpn23.mpifr-be.mkat.karoo.kat.ac.za": 2,
    "fbfpn24.mpifr-be.mkat.karoo.kat.ac.za": 3,
    "fbfpn25.mpifr-be.mkat.karoo.kat.ac.za": 3,
    "fbfpn26.mpifr-be.mkat.karoo.kat.ac.za": 3,
    "fbfpn27.mpifr-be.mkat.karoo.kat.ac.za": 3,
    "fbfpn28.mpifr-be.mkat.karoo.kat.ac.za": 3,
    "fbfpn29.mpifr-be.mkat.karoo.kat.ac.za": 3,
    "fbfpn30.mpifr-be.mkat.karoo.kat.ac.za": 3,
    "fbfpn31.mpifr-be.mkat.karoo.kat.ac.za": 3
}


class FengToFbfMapper(object):
    def __init__(self, nspines=NSPINES, nleaves=NLEAVES,
                 max_subs_per_leaf=MAX_SUBS_PER_LEAF,
                 host_to_leaf_map=HOST_TO_LEAF_MAP):
        self._h2l_map = host_to_leaf_map
        self._nspines = nspines
        self._max_subs_per_leaf = max_subs_per_leaf
        self._subscriptions = np.zeros((nspines, nleaves))
        self._subscription_sets = {}

    def validate_ip_ranges(self, ip_ranges):
        log.debug("Validating IP ranges")
        for ip_range in ip_ranges:
            if ip_range.count != 4:
                log.error("Count for IP range was not 4")
                raise Exception(
                    "All stream must span 4 consecutive multicast groups")

    def group_to_spine(self, group):
        subnet = struct.unpack("B"*4, group.packed)[-1]
        return subnet % self._nspines

    def worker_to_leaf(self, worker):
        return self._h2l_map[worker.hostname]

    def validate_workers(self, workers):
        log.debug("Validating worker servers")
        for worker in workers:
            if worker.hostname not in self._h2l_map:
                log.error(("Could not determine leaf switch ID "
                           "for worker server: {}").format(
                           worker.hostname))
                raise Exception(
                    "Worker '{}' does not map to a leaf switch".format(
                        worker))

    def subscribe(self, ordered_ip_ranges, available_workers, subarray_id):
        log.debug("Determining safe F-engine subscriptions")
        available_workers = available_workers[:]
        self.validate_workers(available_workers)
        self.validate_ip_ranges(ordered_ip_ranges)
        if subarray_id in self._subscription_sets:
            raise Exception(
                "Subarray {} already has a subscription mapping".format(
                    subarray_id))
        used_workers = []
        unallocated_ranges = []
        all_indexes = []
        mapping = []
        for ip_range in ordered_ip_ranges:
            log.debug("Attempting to allocate range: {}".format(
                ip_range.format_katcp()))
            for worker in available_workers:
                leaf_idx = self.worker_to_leaf(worker)
                can_subscribe = True
                indexes = []
                for group in ip_range:
                    spine_idx = self.group_to_spine(group)
                    indexes.append((spine_idx, leaf_idx))
                    if self._subscriptions[spine_idx, leaf_idx] >= self._max_subs_per_leaf:
                        can_subscribe = False
                if can_subscribe:
                    for x, y in indexes:
                        self._subscriptions[x, y] += 1
                    mapping.append((worker, ip_range))
                    all_indexes.extend(indexes)
                    available_workers.remove(worker)
                    used_workers.append(worker)
                    log.info("Allocated {} to {}".format(
                        ip_range.format_katcp(), worker))
                    break
                else:
                    continue
            else:
                log.warning("Unable to allocate {}".format(
                    ip_range.format_katcp()))
                unallocated_ranges.append(ip_range)
        self._subscription_sets[subarray_id] = all_indexes
        log.debug(self.render_spine_status())
        return mapping, available_workers, unallocated_ranges

    def unsubscribe(self, subarray_id):
        log.debug("Removing subscriptions from subarray: {}".format(
            subarray_id))
        for x, y in self._subscription_sets[subarray_id]:
            self._subscriptions[x, y] -= 1
        del self._subscription_sets[subarray_id]
        log.debug(self.render_spine_status())

    def render_spine_status(self):
        status = "Subscription count matrix:\n"
        status += "Leaf:     0 | 1 | 2 | 3 \n"
        status += "-----------------------\n"
        for ii, row in enumerate(self._subscriptions):
            status += "Spine {:02d}: {}\n".format(
                ii, " | ".join(map(str, map(int, row))))
        return status


