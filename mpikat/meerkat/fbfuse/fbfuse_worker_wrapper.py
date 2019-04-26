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
from tornado.gen import coroutine, Return
from katcp import KATCPClientResource
from mpikat.core.worker_pool import WorkerPool, WorkerWrapper, WorkerRequestError

log = logging.getLogger("mpikat.fbfuse_worker_wrapper")


class FbfWorkerWrapper(WorkerWrapper):
    """Wrapper around a client to an FbfWorkerServer
    instance.
    """

    def __init__(self, hostname, port):
        """
        @brief  Create a new wrapper around a client to a worker server

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on
        """
        super(FbfWorkerWrapper, self).__init__(hostname, port)

    @coroutine
    def prepare(self, *args, **kwargs):
        response = yield self._client.req.prepare(*args, **kwargs)
        if not response.reply.reply_ok():
            raise WorkerRequestError(response.reply.arguments[1])

class FbfWorkerPool(WorkerPool):

    def make_wrapper(self, hostname, port):
        return FbfWorkerWrapper(hostname, port)
