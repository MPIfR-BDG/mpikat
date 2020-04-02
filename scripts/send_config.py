#!/usr/bin/env python

import logging
import socket
from tornado.gen import coroutine, Return
from tornado.ioloop import IOLoop
import json
import os

log = logging.getLogger("mpikat.cli")

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

    # Fix python import to current directory for
    os.sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from mpikat.effelsberg.edd.edd_server_product_controller import EddServerProductController

    controller = EddServerProductController("MASTER", args.host, args.port)

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
    def wrapper():
        try:
            yield controller.configure(cfg)
        except Exception as E:
            log.error(E)
            raise Return("ERROR: " + str(E))
        else:
            raise Return('OK')

    res = ioloop.run_sync(wrapper)
    print(res)
