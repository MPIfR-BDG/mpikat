import logging
import select
from threading import Thread, Event
import time

log = logging.getLogger('mpikat.pipe_monitor')


class PipeMonitor(Thread):

    def __init__(self, pipe, handler, sentinel=b'', timeout=1):
        """
        @brief   Class for parsing output of subprocess pipes

        @param   pipe    An OS pipe
        @param   handler handler to be called on the line.
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
        """
        Starts the monitor thread. Will stop on pipe error or pipe close.
        """
        while not self._stop_event.is_set():
            pev = self._poll.poll(self._timeout)[0]
            if pev[1] in [select.POLLIN, select.POLLPRI]:
                for line in iter(self._pipe.readline, self._sentinel):
                    try:
                        self._handler(line)
                    except Exception as error:
                        log.warning("Exception raised in pipe handler: '{}' with line '{}'".format(
                            str(error), line))
            elif pev[1] in [select.POLLHUP]:
                self.stop()
            elif pev[1] == select.POLLOUT:
                pass
            elif pev[1] == select.POLLNVAL:
                log.error("Invalid request descriptor not open")
                self.stop()
            elif pev[1] == select.POLLERR:
                log.error("Error reading data from pipe!")
                self.stop()
            else:
                log.error("Unknown error code")
                self.stop()

    def stop(self):
        """
        @brief   Stop the thread
        """
        self._stop_event.set()


if __name__ == "__main__":
    import subprocess

    def handler(line):
        print line

    proc = subprocess.Popen("echo Foo && sleep 2 && echo bar", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, close_fds=True)
    pm = PipeMonitor(proc.stdout, handler)
    pm.start()
    pm.join()


