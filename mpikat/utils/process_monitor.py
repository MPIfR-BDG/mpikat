import logging
from threading import Thread, Event

log = logging.getLogger('mpikat.process_monitor')


class SubprocessMonitor(Thread):

    def __init__(self):
        """
        @brief   Monitors managed subprocesses and passes the process to a specific callback function.

        """
        Thread.__init__(self)
        self.setDaemon(True)
        self._procs = []

        self._stop_event = Event()

    def run(self):
        for proc, cb in self._procs:
            if not proc.is_alive():
                log.debug('Process {} terminated with returncode {}'.format(proc.pid, proc.returncode))
                cb(proc)

    def add(self, proc, cb):
        log.debug('Adding process {} with callback {}'.format(proc.pid, cb))
        self._procs.append([proc, cb])

    def stop(self):
        """
        @brief      Stop the thread
        """
        log.debug('Stopping')
        self._stop_event.set()


