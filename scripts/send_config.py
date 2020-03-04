#!/usr/bin/env python

import logging
import time
import socket
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
    def send_cfg(self, cfg):
        """
        @brief      Set the interface address for a packetiser qsfp interface

        @param      intf   The interface specified as a string integer, e.g. '0' or '1'
        @param      ip     The IP address to assign to the interface
        """
        yield self._safe_request("configure", json.dumps(cfg))


if __name__ == "__main__":
    import coloredlogs
    from argparse import ArgumentParser 
    parser = ArgumentParser()
    parser.add_argument('-H', '--host', dest='host', type=str,
        help='Host interface to bind to', default="automatic")
    parser.add_argument('-p', '--port', dest='port', type=long,
        help='Port number to bind to', default=1235)

    parser.add_argument('-s', '--select-product', dest='select_product', type=str,
        help='Select specific product', default=None)
    parser.add_argument('configfile', help="Config json to process")

    args = parser.parse_args()
    if args.host == "automatic":
        args.host = socket.gethostbyname(socket.gethostname())
        print("Automatic look up of host IP - found {}".format(args.host))

    logging.getLogger().addHandler(logging.NullHandler())
    ioloop = IOLoop.current()
    client = ConfigClient(args.host, port=args.port)

    cfg = json.load(open(args.configfile))
    if 'packetisers' in cfg:
        cfg['packetizers'] = cfg['packetisers']

    if args.select_product:
        for p in cfg['packetizers'] + cfg['products']:
            if args.select_product == p['id']:
                cfg = p
                break

    print("Configure client ...")
    @coroutine
    def configure():
        try:
            yield client.send_cfg(cfg)
        except Exception as error:
            log.exception("Error during packetiser configuration: {}".format(str(error)))
            raise error
    ioloop.run_sync(configure)


