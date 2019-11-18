
import logging
import time
from tornado.gen import coroutine, sleep, Return
from tornado.ioloop import IOLoop
from katcp import KATCPClientResource
import json 

log = logging.getLogger("mpikat.edd_digpack_client")

class ConfigClient(object):
    def __init__(self, host, port=7147):
        """
        @brief      Class for digitiser packetiser client.

        @param      host   The host IP or name for the desired packetiser KATCP interface
        @param      port   The port number for the desired packetiser KATCP interface
        """
        self._host = host
        self._port = port
        self._client = KATCPClientResource(dict(
            name="config-client",
            address=(self._host, self._port),
            controlled=True))
        self._client.start()

    def stop(self):
        self._client.stop()

    @coroutine
    def _safe_request(self, request_name, *args):
        log.info("Sending packetiser request '{}' with arguments {}".format(request_name, args))
        yield self._client.until_synced()
        response = yield self._client.req[request_name](*args)
        if not response.reply.reply_ok():
            log.error("'{}' request failed with error: {}".format(request_name, response.reply.arguments[1]))
        else:
            log.debug("'{}' request successful".format(request_name))
            raise Return(response)

    @coroutine
    def load_cfg(self, filename):
        """
        @brief      Set the interface address for a packetiser qsfp interface

        @param      intf   The interface specified as a string integer, e.g. '0' or '1'
        @param      ip     The IP address to assign to the interface
        """
        cfg = json.load(open(filename))
        yield self._safe_request("configure", json.dumps(cfg))


if __name__ == "__main__":
    import coloredlogs
    from optparse import OptionParser
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
        help='Host interface to bind to', default="134.104.73.132")
    parser.add_option('-p', '--port', dest='port', type=long,
        help='Port number to bind to', default=7147)
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    ioloop = IOLoop.current()
    client = ConfigClient(opts.host, port=opts.port)
    @coroutine
    def configure():
        try:
            yield client.load_cfg(args[0])
        except Exception as error:
            log.exception("Error during packetiser configuration: {}".format(str(error)))
            raise error
    ioloop.run_sync(configure)



