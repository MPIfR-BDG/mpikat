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
import coloredlogs
import json
import tornado
import signal
import time
import numpy as np
from katcp import Sensor, AsyncDeviceServer
from katcp.kattypes import request, return_reply, Int, Str
from optparse import OptionParser
from tornado.gen import Return, coroutine
from katcp import Sensor, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str, Discrete, Float
from katpoint import Target
from mpikat.core.master_controller import MasterController
from mpikat.core.exceptions import ProductLookupError

from __future__ import print_function, division
import spead2
import spead2.recv
import logging
import argparse
import numpy as np

log = logging.getLogger("mpikat.apsuse_ib_monitor")

class StreamManager(object):
    def __init__(self, stream):
        self._stream = stream
        self._item_group = spead2.ItemGroup()
        self._item_group.add_item(5632, "timestamp", "", (1,), dtype=">I")
        self._item_group.add_item(21845, "beam_idx", "", (1,), dtype=">I")
        self._item_group.add_item(16643, "chan_idx", "", (1,), dtype=">I")
        self._item_group.add_item(21846, "raw_data", "", (8192,), dtype="b")
        self._counts = {}
        self._integrations = {}

    def capture(self, nheaps=100):
        for heap in self._stream:
            self._handler(heap)
            nheaps -= 1
            if nheaps <= 0:
                break
        self._finish()

    def _handler(self, heap):
        items = self._item_group.update(heap)
        log.info("Heap: {}".format(items))
        key = (int(items["beam_idx"].value), int(items["chan_idx"].value))
        power = items["raw_data"].value
        var = power.reshape(8, 1024).std(axis=0).mean(axis=0)
        print(key, var)
        if key not in self._counts:
            self._counts[key] = 1
            self._integrations[key] = power
        else:
            self._counts[key] += 1
            self._integrations[key] += power

    def _finish(self):
        ar = np.zeros(4096)
        for key in sorted(self._counts.keys()):
            total_power = self._integrations[key]
            count = self._counts[key]
            print("beam_idx={}, chan_idx={}, power={}".format(
                key[0], key[1], total_power.sum()/float(count)))
            tp = total_power / float(count)
            ar[key[1]:key[1]+64] = tp.reshape(128, 64).mean(axis=0)
        np.save("bandpass.npy", ar)


def main(mcast_groups, interface, nheaps):
    thread_pool = spead2.ThreadPool(threads=4)
    stream = spead2.recv.Stream(thread_pool, spead2.BUG_COMPAT_PYSPEAD_0_5_2)
    del thread_pool
    pool = spead2.MemoryPool(16384, 26214400, 64, 32)
    stream.set_memory_allocator(pool)
    for group in mcast_groups:
        stream.add_udp_reader(group, 7147, interface_address=interface)
    manager = StreamManager(stream)
    manager.capture(nheaps)



class CaptureProcess(Process):
    def __init__(self, stream):
        self._stream = stream
        self._ip_range = ip_range_from_stream(self._stream)

    def run(self):
        pass



class IncoherentBeamMonitor(AsyncDeviceServer):
    VERSION_INFO = ("mpikat-ib-monitor-api", 0, 1)
    BUILD_INFO = ("mpikat-ib-monitor-implementation", 0, 1, "rc1")

    def __init__(self, ip, port):
        super(IncoherentBeamMonitor, self).__init__(ip, port)

    def start(self):
        super(IncoherentBeamMonitor, self).start()

    def stop(self):
        super(IncoherentBeamMonitor, self).stop()

    def setup_sensors(self):
        self._mc_address_sensor = Sensor.address(
            "fbfuse-master-controller",
            description="The address of the FBFUSE master controller",
            default=("0", 0),
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._mc_address_sensor)

        self._ib_stream_sensor = Sensor.string(
            "ib-stream",
            description="The stream address for the FBFUSE incoherent beam",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ib_stream_sensor)

        self._ib_bandpass_sensor = Sensor.string(
            "ib-bandpass_PNG",
            description="The bandpass of the incoherent beam",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ib_bandpass_sensor)

        self._ib_total_power_sensor = Sensor.string(
            "ib-total-power_PNG",
            description="The total power timeseries of the incoherent beam",
            default="",
            initial_status=Sensor.UNKNOWN)
        self.add_sensor(self._ib_total_power_sensor)

