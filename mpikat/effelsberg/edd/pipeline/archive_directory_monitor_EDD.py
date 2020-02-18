import logging
import signal
import sys
import shlex
import shutil
import os
import base64
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from subprocess import Popen, PIPE
from katcp import AsyncDeviceServer, Message, Sensor, AsyncReply, KATCPClientResource
from katcp.kattypes import request, return_reply, Str
from tornado.gen import coroutine, sleep, Return
import tornado

log = logging.getLogger(
    "mpikat.effelsberg.edd.pipeline.pipeline")

class SensorServerError(Exception):
    pass

class ArchiveAdder(FileSystemEventHandler):

    def __init__(self, output_dir, png_server):
        super(ArchiveAdder, self).__init__()
        self.output_dir = output_dir
        self.first_file = True
        self.png_server = png_server

    @coroutine
    def _safe_request(self, request_name, *args):
        log.info("Sending packetiser request '{}' with arguments {}".format(
            request_name, args))
        yield self._png_server.until_synced()
        response = yield self._png_server.req[request_name](*args)
        if not response.reply.reply_ok():
            log.error("'{}' request failed with error: {}".format(
                request_name, response.reply.arguments[1]))
            raise SensorServerError(response.reply.arguments[1])
        else:
            log.debug("'{}' request successful".format(request_name))
            raise Return(response)

    def _syscall(self, cmd):
        log.info("Calling: {}".format(cmd))
        proc = Popen(shlex.split(cmd), stdout=PIPE, stderr=PIPE)
        proc.wait()
        if proc.returncode != 0:
            log.error(proc.stderr.read())
        else:
            log.debug("Call success")

    def fscrunch(self, fname):
        self._syscall("pam -F -e fscrunch {}".format(fname))
        return fname.replace(".ar", ".fscrunch")
        
    @coroutine    
    def process(self, fname):
        fscrunch_fname = self.fscrunch(fname)
        if self.first_file:
            log.info("First file in set. Copying to sum.?scrunch.")
            shutil.copy2(fscrunch_fname, "sum.fscrunch")
            shutil.copy2(fname, "sum.tscrunch")
            self.first_file = False
        else:
            self._syscall("psradd -T -inplace sum.tscrunch {}".format(fname))
            self._syscall(
                "psradd -inplace sum.fscrunch {}".format(fscrunch_fname))
            #log.debug("psrplot -p time -D {}/fscrunch.png/png sum.fscrunch".format(self.output_dir))
            #log.debug("psrplot -p freq -D {}/tscrunch.png/png sum.tscrunch".format(self.output_dir))
            #self._syscall("psrplot -p time -D ../combined_data/fscrunch.png/png sum.fscrunch".format(self.output_dir))
            #self._syscall("psrplot -p freq -D ../combined_data/tscrunch.png/png sum.tscrunch".format(self.output_dir))
            #self._syscall("psrplot -p flux -D ../combined_data/profile.png/png sum.fscrunch".format(self.output_dir))
            self._syscall(
                "psrplot -p freq+ -j dedisperse -D ../combined_data/tscrunch.png/png sum.tscrunch")
            #self._syscall("pav -TGpd sum.tscrunch -g ../combined_data/tscrunch.png/png")
            self._syscall(
                "pav -DFTp sum.fscrunch -g ../combined_data/profile.png/png")
            self._syscall(
                "pav -FY sum.fscrunch -g ../combined_data/fscrunch.png/png")
            log.info("removing {}".format(fscrunch_fname))
            os.remove(fscrunch_fname)
            #shutil.copy2("sum.fscrunch", self.output_dir)
            #shutil.copy2("sum.tscrunch", self.output_dir)
            log.info("Accessing archive PNG files")
            
            #imageFile = open("{}/fscrunch.png".format(self.output_dir), "rb")
            #log.info("reading fscrunch.png")
            #fscrunch = base64.b64encode(imageFile.read())
            #yield png_server.until_synced()
            #png_server.req.fscrunch(fscrunch)
            #yield self.png_server.until_synced()
            IMAGE = "iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg=="
            self._safe_request("fscrunch", IMAGE)
            #self.png_server.req.fscrunch("iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg==")
            """
            try:
                with open("{}/tscrunch.png".format(self.output_dir), "rb") as imageFile:
                    log.info("reading tscrunch.png")
                    yield self._png_server.until_synced()
                    self._png_server.req.tscrunch(base64.b64encode(imageFile.read()))
            except Exception as error:
                log.debug(error)
            try:
                with open("{}/profile.png".format(self.output_dir), "rb") as imageFile:
                    log.info("reading profile.png")
                    yield self._png_server.until_synced()
                    self._png_server.req.profile(base64.b64encode(imageFile.read()))
            except Exception as error:
                log.debug(error)
            """
    def on_created(self, event):
        log.info("New file created: {}".format(event.src_path))
        try:
            fname = event.src_path
            log.info(fname.find('.ar.') != -1)
            if fname.find('.ar.') != -1:
                log.info(
                    "Passing archive file {} for processing".format(fname[0:-9]))
                self.process(fname[0:-9])
        except Exception as error:
            log.error(error)


def main(input_dir, output_dir, handler):
    observer = Observer()
    observer.daemon = False
    log.info("Input directory: {}".format(input_dir))
    log.info("Output directory: {}".format(output_dir))
    log.info("Setting up ArchiveAdder handler")
    observer.schedule(handler, input_dir, recursive=False)

    def shutdown(sig, func):
        log.info("Signal handler called on signal: {}".format(sig))
        observer.stop()
        observer.join()
        sys.exit()

    log.info("Setting SIGTERM and SIGINT handler")
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    log.info("Starting directory monitor")
    observer.start()
    log.info("Parent thread entering 1 second polling loop")
    while not observer.stopped_event.wait(1):
        pass

if __name__ == "__main__":
    from argparse import ArgumentParser
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger(
        'mpikat.effelsberg.edd.pipeline.pipeline')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(logging.DEBUG)

    usage = "usage: {prog} [options]".format(prog=sys.argv[0])
    parser = ArgumentParser(usage=usage)
    parser.add_argument("-i", "--input_dir", type=str,
                        help="The directory to monitor for new files",
                        required=True)
    parser.add_argument("-o", "--output_dir", type=str,
                        help="The directory to output results to",
                        required=True)
    parser.add_argument("-m", "--mode", type=str,
                        help="Processing mode to operate in",
                        default="ArchiveAdder")
    parser.add_argument('-H', '--host', dest='host', type=str,
                      help='KATCP png server interface', default="0.0.0.0")
    parser.add_argument('-p', '--port', dest='port', type=long,
                      help='Port number for the KATCP png server', default=10000)

    args = parser.parse_args()

    log.info("Starting connection to KATCP server")
    ioloop = tornado.ioloop.IOLoop.current()
    png_server = KATCPClientResource(dict(
            name="_png_server_client",
            address=(args.host, args.port),
            controlled=True))
    #png_server.start()

#    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
#        shutdown, ioloop, png_server))

    def start_and_display():
        png_server.start()
        log.info(
            "Listening at {0}, Ctrl-C to terminate server".format(args.host))

    ioloop.add_callback(start_and_display)


    if args.mode == "ArchiveAdder":
        handler = ArchiveAdder(args.output_dir, png_server)
    else:
        log.error("Processing mode {} is not supported.".format(args.mode))
        sys.exit(-1)

    main(args.input_dir, args.output_dir, handler)
    ioloop.start()