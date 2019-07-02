from __future__ import print_function, division
import spead2
import spead2.recv
import logging
import argparse
import numpy as np

logging.basicConfig(level=logging.ERROR)
log = logging.getLogger()


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run an FBFUSE capture test')
    parser.add_argument('--groups', type=str, nargs='+', required=True,
                        help='List of mutlicast groups to capture')
    parser.add_argument('--interface', type=str, required=True,
                        help='The Ethernet interface to capture on')
    parser.add_argument('--nheaps', type=int, default=100,
                        help='The number of heaps to capture')
    args = parser.parse_args()
    main(args.groups, args.interface, args.nheaps)