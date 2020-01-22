#!/usr/bin/env python

from __future__ import print_function, division
import spead2
import spead2.recv
import logging
import numpy as np
import socket
import time
import thread
import multiprocessing
from multiprocessing import Pool, Process
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import math as m
from datetime import datetime

class CaptureData(object):
    
    def resource_allocation(self):
        print("allocating resource..... ")
        thread_pool = spead2.ThreadPool(threads=4)
        x = spead2.recv
        stream = x.Stream(thread_pool, spead2.BUG_COMPAT_PYSPEAD_0_5_2, max_heaps = 64, ring_heaps=64, contiguous_only = False)
        pool = spead2.MemoryPool(16384, (32*4*1024**2)+1024, max_free= 64, initial= 64)
        stream.set_memory_allocator(pool)
        print("Done.")
        return stream
    
    def mcg_subscription(self, stream, ip):
        print("subscribing.... ")
        stream.add_udp_reader(ip, 7152, interface_address="10.10.1.12", max_size = 9200L,buffer_size= 1073741820L)
        print("Done.")

    def plot_data(self, data, name):
        filename = name+"."+"npy"
        np.save(filename, data)
        logX = (np.log10(data)*10)
        fig = plt.figure()
        plt.plot(logX[1:])
        fig_name = name+"."+"png"
        fig.savefig(fig_name, dpi=fig.dpi)
    
    def stream_handler(self,stream, name):
        items = []
        incomplete_heaps = 0
        complete_heaps = 0
        ig = spead2.ItemGroup()
        ig.add_item(5632, "timestamp_count", "", (1,), dtype=">I")
        ig.add_item(5633, "nspectrum", "", (1,), dtype=">I")
        ig.add_item(5634, "ndStatus", "", (1,), dtype=">I")
        ig.add_item(5635, "fft_length", "", (1,), dtype=">I")
        ig.add_item(5636, "nsamples", "", (1,), dtype=">I")
        ig.add_item(5637, "synctime", "", (1,), dtype=">I")
        ig.add_item(5638, "sampling_rate", "", (1,), dtype=">I")
        self._first_heap = True
        num_heaps = 0
        logX = []
        sampling_rate = 2.6e9
        for h in stream:
            print ("num_heaps: ", num_heaps)
            if num_heaps > 10:
                stream.stop()
                self.plot_data(data, name)
                print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
                      " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
                      " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n"
                       "Statistics::stream heaps: {}, incomplete_heaps: {}, packets: {}".format(num_heaps, incomplete_heaps, complete_heaps))
            num_heaps += 1
            if isinstance(h, spead2.recv.IncompleteHeap):
                incomplete_heaps += 1
                continue
            else:
                complete_heaps += 1
            items = ig.update(h)
            count = 0
            for item in items.values():
                count += 1
                print("content: ", count, item.id, item.name, item.value)
                if (item.id == 5635) and (self._first_heap):
                    nchan = int((item.value/2)+1)
                    ig.add_item(5639, "data", "", (nchan,), dtype="<f")
                    self._first_heap = False
                if (item.id == 5639):
                    data = item.value
                setattr(ig, item.name, item.value)
            integtime = (ig.nspectrum*ig.fft_length)/sampling_rate
            nchannels = (ig.fft_length/2)+1
            t = (ig.synctime+((ig.timestamp_count+(ig.nspectrum*ig.fft_length)/2)/sampling_rate))
            timestamp = datetime.fromtimestamp(t).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-2]
            print("integTime: {}, timestamp: {}, nchannels: {}".format(integtime, timestamp, nchannels))
    
    def start(self, ip, fig):
        s = self.resource_allocation()
        self.mcg_subscription(s, ip)
        self.stream_handler(s, fig)
        
    def __call__(self):
        print("starting processes..")
        procs = []
        p = Process(target=self.start, args=(["225.0.0.185", "ndON"]))
        p.start()
        procs.append(p)
        p = Process(target=self.start, args=(["225.0.0.184", "ndOFF"]))
        p.start()
        procs.append(p)
        for p in procs:
            p.join()

x = CaptureData()
x()
