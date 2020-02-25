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

log = logging.getLogger("mpikat.fbfuse_autoscaling_trigger")

class AutoscalingTrigger(object):
    def __init__(self, key="autoscaling_trigger"):
        self._sem_key = key
        log.debug("Creating trigger semaphore, key='{}'".format(
            self._sem_key))
        self._unlink()
        self._create()

    def _create(self):
        log.debug("Creating trigger semaphore, key='{}'".format(
            self._sem_key))
        self._sem = posix_ipc.Semaphore(
            self._sem_key,
            flags=posix_ipc.O_CREX,
            initial_value=0)

    def _unlink(self):
        log.debug("Unlinking trigger semaphore, key='{}'".format(
            self._sem_key))
        try:
            posix_ipc.unlink_semaphore(self._sem_key)
        except posix_ipc.ExistentialError:
            pass

    def trigger(self):
        log.info("Triggering autoleveling")
        self._sem.release()

