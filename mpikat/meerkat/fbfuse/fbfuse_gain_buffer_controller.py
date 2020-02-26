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
import posix_ipc
import numpy as np
from mmap import mmap

log = logging.getLogger("mpikat.fbfuse_gain_buffer_controller")


class GainBufferController(object):
    def __init__(self, nantennas, nchans, npol):
        self._nantennas = nantennas
        self._nchans = nchans
        self._npol = npol
        self._nelements = self._nantennas * self._nchans * self._npol
        self._nbytes = self._nelements * 8  # 8 bytes for float2 type
        self.shared_buffer_key = "gain_buffer"
        self.mutex_semaphore_key = "gain_buffer_mutex"
        self.counting_semaphore_key = "gain_buffer_count"
        self._nreaders = 1

    def __del__(self):
        self.unlink()

    def unlink(self):
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

    def create(self):
        """
        @brief   Start the gain buffer controller

        @detail  This method will create all necessary posix shared memory segments
                 and semaphores, retreive necessary information from the gain
                 configuration server and start the gain update callback loop.
        """
        log.info("Starting gain buffer controller")
        log.info("Creating IPC buffers and semaphores for gain buffer")
        self.unlink()
        # This semaphore is required to protect access to the shared_buffer
        # so that it is not read and written simultaneously
        # The value is set to two such that two processes can read
        # simultaneously
        log.debug("Creating mutex semaphore, key='{}'".format(
            self.mutex_semaphore_key))
        self._mutex_semaphore = posix_ipc.Semaphore(
            self.mutex_semaphore_key,
            flags=posix_ipc.O_CREX,
            initial_value=self._nreaders)

        # This semaphore is used to notify beamformer instances of a change to the
        # complex gains. Upon any change its value is simply incremented by one.
        # Note: There sem_getvalue does not work on Mac OS X so the value of this
        # semaphore cannot be tested on OS X (this is only a problem for local
        # testing).
        log.debug("Creating counting semaphore, key='{}'".format(
            self.counting_semaphore_key))
        self._counting_semaphore = posix_ipc.Semaphore(
            self.counting_semaphore_key,
            flags=posix_ipc.O_CREX,
            initial_value=0)

        # This is the share memory buffer that contains the gain models for
        # the
        log.debug("Creating shared memory, key='{}'".format(
            self.shared_buffer_key))
        self._shared_buffer = posix_ipc.SharedMemory(
            self.shared_buffer_key,
            flags=posix_ipc.O_CREX,
            size=self._nbytes)

        # For reference one can access this memory from another python process using:
        # shm = posix_ipc.SharedMemory("gain_buffer")
        # data_map = mmap.mmap(shm.fd, shm.size)
        # data = np.frombuffer(data_map, dtype=[("gain_rate","float32"),("gain_offset","float32")])
        # data = data.reshape(nbeams, nantennas)
        self._shared_buffer_mmap = mmap(
            self._shared_buffer.fd, self._shared_buffer.size)
        log.info("Delay buffer controller started")

    def update_gains(self):
        for ii in range(self._nreaders):
            self._mutex_semaphore.acquire()
        try:
            self.write_gains()
            self._counting_semaphore.release()
            log.debug("Incrementing counting semaphore")
        except Exception as error:
            log.exception("Error when updating complex gain model: {}".format(
                str(error)))
        finally:
            for ii in range(self._nreaders):
                self._mutex_semaphore.release()

    def write_gains(self):
        log.debug("Writing gains")
        gains = np.exp(np.zeros(self._nelements)*1j)
        gains = gains.astype("complex64").view("float32")
        self._shared_buffer_mmap.seek(0)
        self._shared_buffer_mmap.write(gains.tobytes())
