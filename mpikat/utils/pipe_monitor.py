import time
import logging
from threading import Thread, Event

log = logging.getLogger('mpikat.pipe_monitor')


class PipeMonitor(Thread):

    def __init__(self, pipe, parser):
        """
        @brief   Class for parsing output of subprocess pipes

        @param   pipe   An OS pipe
        """
        Thread.__init__(self)
        self.setDaemon(True)
        self._parser = parser
        self._pipe = pipe
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
        @brief      Parse a line from a subprocesses pipe

        @param      line  The line
        """
        line = line.strip()
        if len(line) == 0:
            return
        log.debug("Parsing line: '{}'".format(line))
        updates = self._parser(line)
        if not updates:
            log.debug("No parameter updates parsed from line")
            return
        else:
            self._params.update(updates)
            log.info("Parameters: {}".format(self._params))
            self.notify_watchers()

    def run(self):
        for line in iter(self._pipe.readline, b''):
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
        @brief      Check if there is a prolonged block on the pipe read

        @param      idle_time  The maximum time since an update after which blocking is assumed

        @return     True if blocked, False otherwise.
        """
        return time.time() - self._last_update > idle_time
