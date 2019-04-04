import logging
import posix_ipc
import ctypes as C
import numpy as np
from optparse import OptionParser
from mmap import mmap
from mpikat.meerkat.fbfuse.fbfuse_delay_buffer_controller import delay_model_type

log = logging.getLogger('mpikat.dummy_delay_buffer')


class DummyDelayBufferManager(object):
    def __init__(self, nantennas, nbeams):
        self._nantennas = nantennas
        self._nbeams = nbeams
        self.shared_buffer_key = "delay_buffer"
        self.mutex_semaphore_key = "delay_buffer_mutex"
        self.counting_semaphore_key = "delay_buffer_count"
        self._delay_model = delay_model_type(
            self._nbeams, self._nantennas)()
        self._nreaders = 1

    def destroy(self):
        """
        @brief   Unlink (remove) all posix shared memory sections and semaphores.
        """
        log.debug(
            "Unlinking all relevant posix shared memory segments and semaphores")
        try:
            posix_ipc.unlink_semaphore(self.counting_semaphore_key)
        except posix_ipc.ExistentialError:
            pass
        try:
            posix_ipc.unlink_semaphore(self.mutex_semaphore_key)
        except posix_ipc.ExistentialError:
            pass
        try:
            posix_ipc.unlink_shared_memory(self.shared_buffer_key)
        except posix_ipc.ExistentialError:
            pass

    def make(self):
        log.debug("Creating mutex semaphore, key='{}'".format(
            self.mutex_semaphore_key))
        self._mutex_semaphore = posix_ipc.Semaphore(
            self.mutex_semaphore_key,
            flags=posix_ipc.O_CREX,
            initial_value=self._nreaders)
        log.debug("Creating counting semaphore, key='{}'".format(
            self.counting_semaphore_key))
        self._counting_semaphore = posix_ipc.Semaphore(
            self.counting_semaphore_key,
            flags=posix_ipc.O_CREX,
            initial_value=0)
        log.debug("Creating shared memory, key='{}'".format(
            self.shared_buffer_key))
        self._shared_buffer = posix_ipc.SharedMemory(
            self.shared_buffer_key,
            flags=posix_ipc.O_CREX,
            size=C.sizeof(self._delay_model))
        self._shared_buffer_mmap = mmap(
            self._shared_buffer.fd, self._shared_buffer.size)

    def write(self, epoch, duration, delays):
        self._delay_model.epoch = epoch
        self._delay_model.duration = duration
        self._delay_model.delays[:] = np.array(delays).astype('float32').ravel()
        self._shared_buffer_mmap.seek(0)
        self._shared_buffer_mmap.write(C.string_at(
            C.addressof(self._delay_model), C.sizeof(self._delay_model)))


if __name__ == "__main__":
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-a', '--nantennas', dest='nantennas', type=int,
                      help='The number of antennas in the delay model')
    parser.add_option('-b', '--nbeams', dest='nbeams', type=int,
                      help='The number of beams in the delay model')
    (opts, args) = parser.parse_args()
    manager = DummyDelayBufferManager(opts.nantennas, opts.nbeams)
    manager.destroy()
    manager.make()




