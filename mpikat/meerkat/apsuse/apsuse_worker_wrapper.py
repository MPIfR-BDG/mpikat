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
import json
from tornado.gen import coroutine
from mpikat.core.worker_pool import WorkerPool, WorkerWrapper, WorkerRequestError

log = logging.getLogger("mpikat.apsuse_worker_wrapper")


class ApsWorkerWrapper(WorkerWrapper):
    """Wrapper around a client to an ApsWorkerServer
    instance.
    """
    def __init__(self, hostname, port):
        """
        @brief  Create a new wrapper around a client to a worker server

        @params hostname The hostname for the worker server
        @params port     The port number that the worker server serves on
        """
        super(ApsWorkerWrapper, self).__init__(hostname, port)

    @coroutine
    def _make_request(self, req, *args, **kwargs):
        response = yield req(*args, **kwargs)
        if not response.reply.reply_ok():
            raise WorkerRequestError(response.reply.arguments[1])

    @coroutine
    def configure(self, config_dict):
        yield self._make_request(
            self._client.req.configure,
            json.dumps(config_dict),
            timeout=120.0)

    @coroutine
    def deconfigure(self):
        yield self._make_request(self._client.req.deconfigure, timeout=60.0)

    @coroutine
    def enable_writers(self, beam_dict, output_dir):
        yield self._make_request(self._client.req.target_start,
            json.dumps(beam_dict), output_dir)

    @coroutine
    def disable_writers(self):
        yield self._make_request(self._client.req.target_stop)


class ApsWorkerPool(WorkerPool):
    def make_wrapper(self, hostname, port):
        return ApsWorkerWrapper(hostname, port)
