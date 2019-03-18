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
import time
from tornado.gen import coroutine, Return
from katcp import Sensor, Message, KATCPClientResource
from mpikat.core.utils import LoggingSensor

log = logging.getLogger("mpikat.paf_fi")


class PafFitsInterfaceClientError(Exception):
    pass

class PafFitsInterfaceClient(object):
    """
    Wrapper class for a KATCP client to a PafFitsInterfaceServer
    """
    def __init__(self, name, address):
        """
        @brief      Construct new instance

        @param      parent            The parent PafFitsInterfaceMasterController instance
        """
        self.log = logging.getLogger("mpikat.paf_fi.{}".format(name))
        self._fits_interface_client = KATCPClientResource(dict(
            name="fits-interface-client",
            address=address,
            controlled=True))
        self._fits_interface_client.start()

    @coroutine
    def _request_helper(self, name, *args, **kwargs):
        if kwargs.pop("presync", None):
            yield self._fits_interface_client.until_synced(2)
        response = yield self._fits_interface_client.req[name](*args)
        if not response.reply.reply_ok():
            self.log.error("Error on {} request: {}".format(name, response.reply.arguments[1]))
            raise PafFitsInterfaceClientError(response.reply.arguments[1])

    @coroutine
    def configure(self, active):
        """
        @brief      Configure the attached FITS writer interface

        @param      active  A true or false value for the fits interface (active or passive).
        """
        yield self._fits_interface_client.until_synced(2)
        yield self._request_helper("configure", active)

    @coroutine
    def capture_start(self):
        """
        @brief      Start the FITS interface capturing data
        """
        yield self._request_helper("start")

    @coroutine
    def capture_stop(self):
        """
        @brief      Stop the FITS interface from capturing data
        """
        yield self._request_helper("stop")
