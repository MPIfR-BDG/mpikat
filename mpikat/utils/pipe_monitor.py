import logging
import select
from threading import Thread, Event

log = logging.getLogger('mpikat.pipe_monitor')


class PipeMonitor(Thread):

    def __init__(self, pipe, handler, sentinel=b'', timeout=1):
        """
        @brief   Class for parsing output of subprocess pipes

        @param   pipe   An OS pipe
        """
        Thread.__init__(self)
        self.setDaemon(True)
        self._handler = handler
        self._pipe = pipe
        self._sentinel = sentinel
        self._timeout = timeout
        self._poll = select.poll()
        self._poll.register(self._pipe)
        self._stop_event = Event()

    def run(self):
        for line in iter(self._pipe.readline, self._sentinel):
            try:
                self._handler(line)
            except Exception as error:
                log.warning("Exception raised in pipe handler: '{}' with line '{}'".format(
                    str(error), line))


    def stop(self):
        """
        @brief      Stop the thread
        """
        self._stop_event.set()
