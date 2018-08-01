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
from katcp import KATCPClientResource
from mpikat.worker_pool import WorkerPool

log = logging.getLogger("mpikat.fbfuse_worker_wrapper")

class FbfWorkerWrapper(object):
    """Wrapper around a client to an FbfWorkerServer
    instance.
    """
    def __init__(self, hostname, port):
        """
        @brief  Create a new wrapper around a client to a worker server

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on
        """
        log.debug("Building client to FbfWorkerServer at {}:{}".format(hostname, port))
        self._client = KATCPClientResource(dict(
            name="worker-server-client",
            address=(hostname, port),
            controlled=True))
        self.hostname = hostname
        self.port = port
        self.priority = 0 # Currently no priority mechanism is implemented
        self._started = False

    def start(self):
        """
        @brief  Start the client to the worker server
        """
        log.debug("Starting client to FbfWorkerServer at {}:{}".format(self.hostname, self.port))
        self._client.start()
        self._started = True

    def __repr__(self):
        return "<{} for {}:{}>".format(self.__class__, self.hostname, self.port)

    def __hash__(self):
        # This has override is required to allow these wrappers
        # to be used with set() objects. The implication is that
        # the combination of hostname and port is unique for a
        # worker server
        return hash((self.hostname, self.port))

    def __eq__(self, other):
        # Also implemented to help with hashing
        # for sets
        return self.__hash__() == hash(other)

    def __del__(self):
        if self._started:
            try:
                self._client.stop()
            except Exception as error:
                log.exception(str(error))


class FbfWorkerPool(WorkerPool):
    def make_wrapper(self, hostname, port):
        return FbfWorkerWrapper(hostname, port)