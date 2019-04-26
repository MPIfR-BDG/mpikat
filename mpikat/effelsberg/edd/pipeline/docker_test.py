#!/usr/bin/env python
import signal
import tornado
from tornado import gen
import logging
from optparse import OptionParser
from mpikat.effelsberg.edd.pipeline.pipeline_server import PipelineServer
#from reynard.effelsberg.servers import StatusServer,EffCAMServer, JsonStatusServer
#from reynard.effelsberg.servers import 
from mpikat.effelsberg.edd.pipeline.pipeline import PIPELINE_REGISTRY

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.docker_test")

class Config(object):
    VOLUMES = [("root","/"),]
    NODES = [("localhost",1235),]

@gen.coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()

if __name__ == "__main__":

    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('reynard')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(logging.DEBUG)

    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type="string", default="", metavar='HOST',
                      help='listen to HOST (default="" - all hosts)')
    parser.add_option('-p', '--port', dest='port', type=long, default=0, metavar='N',
                      help='attach to port N (default=0)')
    parser.add_option('-s', '--server_type', dest='server_type', type=str, default="UniversalBackendNode",
                      help='server type to start')
    (opts, args) = parser.parse_args()
    log.info("Starting {opts.server_type} instance".format(opts=opts))
    ioloop = tornado.ioloop.IOLoop.current()
    if opts.server_type == "UniversalBackendNode":
        server = UniversalBackendNode(opts.host, opts.port, Config())
    elif opts.server_type == "UniversalBackendInterface":
        server = UniversalBackendInterface(opts.host, opts.port, Config())
    elif opts.server_type == "PipelineServer":
        #server = PipelineServer(opts.host, opts.port, PIPELINE_REGISTRY["PafFrbPipeline"]["class"])
        server = PipelineServer(opts.host, opts.port, PIPELINE_REGISTRY["EddTestPipeline"]["class"])
    elif opts.server_type == "StatusServer":
        server = StatusServer(opts.host, opts.port)
    elif opts.server_type == "JsonStatusServer":
        server = JsonStatusServer(opts.host, opts.port)
    elif opts.server_type == "EffCAMServer":
        server = EffCAMServer(opts.host, opts.port)
    else:
        raise Exception("Unknown pipeline type")
    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, server))
    def start_and_display():
        server.start()
        print "Listening at {0}, Ctrl-C to terminate server".format(server.bind_address)
    ioloop.add_callback(start_and_display)
    ioloop.start()
