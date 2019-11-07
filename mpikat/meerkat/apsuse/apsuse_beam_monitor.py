import logging
import signal
import time
import coloredlogs
from six import string_types
import tornado
from tornado import gen
from katcp import KATCPClientResource
from katcp.resource import KATCPSensorError
import katpoint

log = logging.getLogger('mpikat.apsuse_beam_monitor')


class BeamListener(object):
    """
    beam_id: str
        FBFUSE beam ID being listened to, e.g. 'cfbf00042'
    update_callback: func
        Function to call when a sensor update is received. At the moment
        this is a method of the parent NodeController class. It must take
        two arguments: FBFUSE beam ID (str), and katpoint.Target object with
        the updated coords.
    """
    def __init__(self, beam_id, update_callback):
        assert isinstance(beam_id, string_types)
        assert callable(update_callback)
        self._beam_id = beam_id
        self._update_callback = update_callback

    def __call__(self, rt, t, status, value):
        target = katpoint.Target(value)
        self._update_callback(self._beam_id, target)


class FBFUSEBeamMonitor(KATCPClientResource):
    """
    Class that monitors the beam position sensors directly on FBFUSE
    """
    def __init__(self, host, port, update_callback, product_id='array_1', beam_ids=[]):
        assert isinstance(host, string_types)
        assert isinstance(port, int)
        assert callable(update_callback)
        assert isinstance(product_id, string_types)
        assert isinstance(beam_ids, list)
        resource_spec = dict(
            name='apsuse-beam-monitor',
            address=(host, port),
            controlled=False, # Do NOT expose requests
            auto_reconnect=True,
            auto_reconnect_delay=5.0, # seconds
            )
        super(FBFUSEBeamMonitor, self).__init__(resource_spec)
        self._update_callback = update_callback
        self._product_id = product_id
        self._beam_ids = beam_ids

        # Gets set to True once the sensor sampling strategies have been set
        # successfully
        self._subscribed = False

    def __del__(self):
        self.stop()

    @property
    def beam_ids(self):
        return self._beam_ids

    @property
    def product_id(self):
        return self._product_id

    @property
    def update_callback(self):
        return self._update_callback

    def get_beam_sensor_name(self, beam_id):
        """
        Returns the sensor name associated to given beam_id. Properly handles
        the special case of the incoherent beam 'ifbf00000'
        """
        # NOTE: these are the sensor names on the FBFUSE master controller
        if beam_id.startswith('cfbf'):
            # coherent beams
            sname = "{}_coherent_beam_{}".format(self.product_id, beam_id)
        elif beam_id == 'ifbf00000':
            # incoherent beam
            sname = "{}_phase_reference".format(self.product_id)
        else:
            raise ValueError("{!r} is not a valid FBFUSE beam ID".format(beam_id))
        return sname

    @gen.coroutine
    def subscribe(self):
        for beam_id in self.beam_ids:
            sname = self.get_beam_sensor_name(beam_id)
            sensor = self.sensor[sname]

            # NOTE: The sensor name is NOT passed as an argument to the
            # listener when an update is received. That's why we need
            # a function-like class BeamListener to store the beam ID
            sensor.register_listener(
                BeamListener(beam_id, self.update_callback)
                )

            # NOTE: IMPORTANT !
            # Set the sampling strategy *last*, that way we get an update
            # from the listener immediately on calling
            # set_sampling_strategy(), which is crucial for our purposes
            # at the moment. Otherwise, we run into a problem where if
            # a beam position flips between init and start, we don't get
            # an update for it when we ?capture-start
            yield sensor.set_sampling_strategy('event')
            log.debug("Successfully set sampling strategy for beam ID {!r}, sensor = {!r}".format(beam_id, sname))
        self._subscribed = True

    @gen.coroutine
    def subscribe_with_retries(self, retries=1):
        for __ in range(retries+1):
            try:
                yield self.subscribe()
            # NOTE: a set sensor sampling strategy call that times out raises a
            # KATCPSensorError
            except (gen.TimeoutError, KATCPSensorError):
                log.warning("Set sampling strategies timed out, trying again")
            else:
                break

        if not self._subscribed:
            raise gen.TimeoutError("Failed to set sampling strategies after {} attempt(s)".format(retries + 1))

    @classmethod
    @gen.coroutine
    def create(cls, host, port, update_callback, product_id='array_1', beam_ids=[], retries=1):
        """
        Create and properly set-up a new FBFUSEMonitor instance (i.e. start it
        and set sampling strategies)

        Parameters
        ----------
        host: str
            IP/DNS address of the FBFUSE master controller
        port: int
            Port on which the FBFUSE master controller is running
        update_callback: func
            Function to call when a sensor update is received. At the moment
            this is a method of the parent NodeController class. It must take
            two arguments: FBFUSE beam ID (str), and katpoint.Target object with
            the updated coords.
        product_id: str
        beam_ids: list
            List of FBFUSE beam IDs such as 'cfbf00042'
        retries: int
            Number of times the setting of sensor sampling strategies should be
            attempted again if it times out.
        """
        monitor = cls(host, port, update_callback, beam_ids=beam_ids)

        t = time.time()
        yield monitor.start()
        log.debug("FBFUSEBeamMonitor started, total time elapsed = {:.2f} s".format(time.time() - t))

        yield monitor.until_synced(timeout=15.0)
        log.debug("FBFUSEBeamMonitor synced, total time elapsed = {:.2f} s".format(time.time() - t))

        yield monitor.subscribe_with_retries(retries=retries)
        log.debug("FBFUSEBeamMonitor sampling strategies set, total time elapsed = {:.2f} s".format(time.time() - t))
        raise gen.Return(monitor)


###############################################################################

# FBFUSE address in devnmk: '0.0.0.0', 33813
# Quadruple zero IP means same as FBFUSE MC: 10.8.66.71

def update_callback(beam_id, target):
    log.debug("{!r}: {!s}".format(beam_id, target))


def main():
    @gen.coroutine
    def on_shutdown(ioloop):
        log.info("Shutting down")
        ioloop.stop()

    ioloop = tornado.ioloop.IOLoop.current()

    # Hook up to SIGINT so that ctrl-C results in a clean shutdown
    signal.signal(
        signal.SIGINT,
        lambda sig, frame: ioloop.add_callback_from_signal(on_shutdown, ioloop)
        )

    @gen.coroutine
    def start_and_display():
        beam_ids = ['cfbf{:05d}'.format(ibeam) for ibeam in range(192)]
        monitor = yield FBFUSEBeamMonitor.create('10.98.76.65', 5000, update_callback, beam_ids=beam_ids)
        log.info("Monitor created and set up")

    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == '__main__':
    logger = logging.getLogger('mpikat')
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s - "
             "%(filename)s:%(lineno)s] %(message)s"),
        level="DEBUG",
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    main()