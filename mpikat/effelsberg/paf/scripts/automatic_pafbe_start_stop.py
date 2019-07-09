import logging
import coloredlogs
import signal
from optparse import OptionParser
from tornado.gen import coroutine, Return
from tornado.locks import Lock
from tornado.ioloop import IOLoop
from katcp import KATCPClientResource

log = logging.getLogger("mpikat.automatic_start_stop")


class PafAutomaticStartStop(object):
    def __init__(self, mc_addr, ioloop, dry_run=False):
        self._mc_addr = mc_addr
        self._mc_client = None
        self._ss_client = None
        self._new_scan = True
        self._observing = False
        self._ioloop = ioloop
        self._dry_run = dry_run
        self._lock = Lock()

    @coroutine
    def _request_helper(self, req, *args, **kwargs):
        response = yield req(*args, **kwargs)
        if not response.reply.reply_ok():
            log.error("Error on request: {}".format(
                response.reply.arguments[1]))
            raise Exception(response.reply.arguments[1])
        raise Return(response)

    @coroutine
    def start(self):
        log.debug("Connecting to master controller...")
        self._mc_client = KATCPClientResource(dict(
            name="master-controller-client",
            address=self._mc_addr,
            controlled=True))
        yield self._mc_client.start()
        log.debug("Connected")
        log.debug("Syncing to master controller...")
        yield self._mc_client.until_synced()
        log.debug("Synced")
        log.debug("Fetching status server address")
        response = yield self._request_helper(
            self._mc_client.req.sensor_value,
            "status-server-address")
        address = response.informs[0].arguments[4]
        ip, port = address.split(":")
        log.debug("Status server address: {}".format(address))
        log.debug("Connecting to status server")
        self._ss_client = KATCPClientResource(dict(
            name="status-server-client",
            address=(ip, int(port)),
            controlled=False))
        yield self._ss_client.start()
        log.debug("Connected")
        log.debug("Syncing with status server")
        yield self._ss_client.until_synced()
        log.debug("Synced")
        log.debug("Registering sensor listeners")
        self._ss_client.sensor.scannum.set_sampling_strategy('event')
        self._ss_client.sensor.scannum.register_listener(
            self._scan_handler)
        self._ss_client.sensor.observing.set_sampling_strategy('event')
        self._ss_client.sensor.observing.register_listener(
            self._observing_handler)
        log.debug("Sensor listners ready")

    def stop(self):
        log.debug("Deregistering sensor listeners")
        self._ss_client.sensor.scannum.set_sampling_strategy('none')
        self._ss_client.sensor.scannum.unregister_listener(
            self._scan_handler)
        self._ss_client.sensor.observing.set_sampling_strategy('none')
        self._ss_client.sensor.observing.unregister_listener(
            self._observing_handler)
        log.debug("Stopping clients")
        self._ss_client.stop()
        self._mc_client.stop()

    def _scan_handler(self, rt, t, status, value):
        log.info("Scan number change, new scan id = {}".format(value))
        self._new_scan = True
        if self._observing:
            self._ioloop.add_callback(self.stop_backend)

    def _observing_handler(self, rt, t, status, value):
        log.info("Observing status changed to: {}".format(value))
        if not value:
            return
        elif self._new_scan:
            self._observing = True
            self._ioloop.add_callback(self.start_backend)
            self._new_scan = False

    @coroutine
    def _with_katcp_mode(self, req, *args, **kwargs):
        log.debug("Setting KATCP control mode")
        yield self._request_helper(
            self._mc_client.req.set_control_mode,
            "katcp",
            timeout=5)
        yield self._request_helper(req, *args, **kwargs)
        log.debug("Setting SCPI control mode")
        yield self._request_helper(
            self._mc_client.req.set_control_mode,
            "scpi",
            timeout=5)

    @coroutine
    def _is_be_configured(self):
        yield self._mc_client.until_synced()
        if "paf0_state" in self._mc_client.sensor.keys():
            state = yield self._mc_client.sensor.paf0_state.get_value()
            log.debug("Backend in '{}' state".format(state))
            raise Return(True)
        else:
            log.warning("Backend is not configured, no start/stop commmands will be sent")
            raise Return(False)

    @coroutine
    def start_backend(self):
        configured = yield self._is_be_configured()
        if not configured:
            return
        log.info("Starting backend")
        if self._dry_run:
            return
        with (yield self._lock.acquire()):
            yield self._with_katcp_mode(
                self._mc_client.req.capture_start,
                timeout=60)

    @coroutine
    def stop_backend(self):
        configured = yield self._is_be_configured()
        if not configured:
            return
        log.info("Stopping backend")
        if self._dry_run:
            self._observing = False
            return
        with (yield self._lock.acquire()):
            yield self._with_katcp_mode(
                self._mc_client.req.capture_stop,
                timeout=60)
            self._observing = False


@coroutine
def on_shutdown(ioloop, controller):
    log.info("Shutting down controller")
    controller.stop()
    ioloop.stop()


if __name__ == "__main__":
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='mc_ip', type=str,
                      help='Host address for master controller')
    parser.add_option('-p', '--port', dest='mc_port', type=int,
                      help='Port number for master controller')
    parser.add_option('-d', '--dry-run', dest='dry_run', action='store_true',
                      help='Do not send start stop commands to the backend')
    parser.add_option('', '--log-level', dest='log_level', type=str,
                      help='Logging level', default="INFO")
    (opts, args) = parser.parse_args()
    logging.getLogger().addHandler(logging.NullHandler())
    logger = logging.getLogger('mpikat')
    logging.getLogger('mpikat.effelsberg.status_server').setLevel(logging.INFO)
    coloredlogs.install(
        fmt="[ %(levelname)s - %(asctime)s - %(name)s - %(filename)s:%(lineno)s] %(message)s",
        level=opts.log_level.upper(),
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = IOLoop.current()
    log.info("Starting PafAutomaticStartStop instance")
    controller = PafAutomaticStartStop(
        (opts.mc_ip, opts.mc_port), ioloop,
        dry_run=opts.dry_run)
    signal.signal(signal.SIGINT,
                  lambda sig, frame: ioloop.add_callback_from_signal(
                      on_shutdown, ioloop, controller))

    @coroutine
    def start_and_display():
        yield controller.start()
        log.info("Started automatic controller")
    ioloop.add_callback(start_and_display)
    ioloop.start()
