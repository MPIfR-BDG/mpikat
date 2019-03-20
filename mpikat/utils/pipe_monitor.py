import time
import logging
from threading import Thread, Event

log = logging.getLogger('mpikat.pipe_monitor')

DELIMETER = None

# As an example this is what the stdout formatter for MKRECV requires
FORMATS = {
    "STAT": [("slot-size", int), ("heaps-completed", int),
             ("heaps-discarded", int), ("heaps-needed", int),
             ("payload-expected", int), ("payload-received", int)]
}


class PipeMonitor(Thread):

    def __init__(self, stdout, formats, delimiter=None):
        """
        @brief   Wrapper for parsing the stdout from MKRECV processes.

        @param   stdout   Stdout pipe from an MKRECV process.

        @detail  MkrecvMonitor runs as a background thread that blocks on the MKRECV
                 processes stdout pipe
        """
        Thread.__init__(self)
        self.setDaemon(True)
        self._formats = formats
        self._delimeter = delimiter
        self._stdout = stdout
        self._params = {}
        self._watchers = set()
        self._last_update = time.time()
        self._stop_event = Event()

    def add_watcher(self, watcher):
        """
        @brief   Add watcher to the montior

        @param   watcher   A callable to be invoked on a notify_watchers call

        @detail  The watcher should take one argument in the form of
                 a dictionary of parameters
        """
        log.debug("Adding watcher: {}".format(watcher))
        self._watchers.add(watcher)

    def remove_watcher(self, watcher):
        """
        @brief   Remove a watcher from the montior

        @param   watcher   A callable to be invoked on a notify_watchers call
        """
        log.debug("Removing watcher: {}".format(watcher))
        self._watchers.remove(watcher)

    def notify_watchers(self):
        """
        @brief      Notifies all watchers of any updates.
        """
        log.debug("Notifying watchers")
        for watcher in self._watchers:
            watcher(self._params)

    def parse_line(self, line):
        """
        @brief      Parse a line from an MKRECV processes stdout

        @param      line  The line
        """
        line = line.strip()
        log.debug("Parsing line: '{}'".format(line))
        if len(line) == 0:
            log.warning("Zero length line detected")
            return
        split = line.split(self._delimeter)
        key = split[0]
        if key in self._formats:
            log.debug("Using formatter for key: {}".format(key))
            formatter = self._formats[key]
            for (name, parser), value in zip(formatter, split[1:]):
                self._params[name] = parser(value)
            log.info("Parameters: {}".format(self._params))
            self.notify_watchers()
        else:
            log.debug("Invalid key: {}".format(key))

    def run(self):
        for line in iter(self._stdout.readline, b''):
            self._last_update = time.time()
            try:
                self.parse_line(line)
            except Exception:
                log.exception("Unable to parse line: '{}'".format(line))
            if self._stop_event.is_set():
                break

    def stop(self):
        """
        @brief      Stop the thread
        """
        self._stop_event.set()

    def is_blocked(self, idle_time=2.0):
        """
        @brief      Check if there is a prolonged block on the stdout read

        @param      idle_time  The maximum time since an update after which blocking is assumed

        @return     True if blocked, False otherwise.
        """
        return time.time() - self._last_update > idle_time
