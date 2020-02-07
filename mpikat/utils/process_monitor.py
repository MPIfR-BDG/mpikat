import logging
from threading import Thread, Event
import time
import os

log = logging.getLogger('mpikat.process_monitor')


class SubprocessMonitor(Thread):

    def __init__(self, poll_intervall=1):
        """
        @brief   Monitors managed subprocesses and passes the process to a specific callback function if crashed.
        """
        Thread.__init__(self)
        self.setDaemon(True)
        self._procs = []
        self._poll_intervall = poll_intervall

        self._stop_event = Event()

    def run(self):
        while not self._stop_event.is_set():
            for proc, cb in self._procs:
                if not os.path.exists("/proc/{}/stat".format(proc.pid)):
                    log.debug('Process {} terminated with returncode {}'.format(proc.pid, proc.returncode))
                    cb(proc)
                else:
                    stat = open("/proc/{}/stat".format(proc.pid)).readline().split()
                    if stat[2] in ['R', 'S', 'D']:
                        log.debug('Process {} [{}] still alive in state {}'.format(proc.pid, stat[1], stat[2]))
                    else:
                        log.debug('Process {} [{}] terminated with returncode {} - state {}'.format(proc.pid, stat[1], proc.returncode, stat[2]))
                        cb(proc)
            time.sleep(self._poll_intervall)

    def add(self, proc, cb):
        log.debug('Adding process {} with callback {}'.format(proc.pid, cb))
        self._procs.append([proc, cb])

    def stop(self):
        """
        @brief      Stop the thread
        """
        log.debug('Stopping monitoring of {} subprocesses'.format(len(self._procs)))
        self._stop_event.set()


if __name__ == "__main__":
    import mpikat.utils.process_tools as process_tools
    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(logging.DEBUG)
    log.addHandler(streamHandler)
    log.setLevel(logging.DEBUG)
    f = process_tools.ManagedProcess("sleep 5")
    m = SubprocessMonitor()

    def callback(args):
        print m.stop()

    m.add(f, callback)
    m.start()
    m.join()


