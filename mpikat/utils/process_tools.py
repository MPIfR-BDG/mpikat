import time
import logging
from subprocess import Popen, PIPE
from tornado.gen import coroutine, sleep
from mpikat.utils.pipe_monitor import PipeMonitor

log = logging.getLogger('mpikat.process_tools')


class ProcessTimeout(Exception):
    pass


class ProcessException(Exception):
    pass


@coroutine
def process_watcher(process, name=None, timeout=120):
    if name is None:
        name = ""
    else:
        name = "'{}'".format(name)
    log.debug("Watching process: {} {}".format(process.pid, name))
    start = time.time()
    while process.poll() is None:
        yield sleep(0.2)
        if (time.time() - start) > timeout:
            process.kill()
            raise ProcessTimeout
    if process.returncode != 0:
        message = "Process returned non-zero returncode: {} {}".format(
            process.returncode, name)
        log.error(message)
        log.error("Process STDOUT dump {}:\n{}".format(
            name, process.stdout.read()))
        log.error("Process STDERR dump {}:\n{}".format(
            name, process.stderr.read()))
        raise ProcessException(
            "Process returned non-zero returncode: {} {}".format(
                process.returncode, name))
    else:
        log.debug("Process stdout {}:\n{}".format(
            name, process.stdout.read()))
        log.debug("Process stderr {}:\n{}".format(
            name, process.stderr.read()))


class ManagedProcess(object):
    def __init__(self, cmdlineargs, stdout_handler=None, stderr_handler=None):
        self._proc = Popen(cmdlineargs, stdout=PIPE, stderr=PIPE,
                           shell=False, close_fds=True)
        self._stdout_handler = stdout_handler
        self._stderr_handler = stderr_handler
        self.stdout_monitor = None
        self.stderr_monitor = None
        self._start_monitors()

    def is_alive(self):
        return self._proc.poll() is None

    def _start_monitors(self):
        if self._stdout_handler:
            self.stdout_monitor = PipeMonitor(
                self._proc.stdout, self._stdout_handler)
            self.stdout_monitor.start()
        if self._stderr_handler:
            self.stderr_monitor = PipeMonitor(
                self._proc.stderr, self._stderr_handler)
            self.stderr_monitor.start()

    def _stop_monitors(self):
        if self.stdout_monitor:
            self.stdout_monitor.stop()
            self.stdout_monitor.join()
            self.stdout_monitor = None
        if self.stderr_monitor:
            self.stderr_monitor.stop()
            self.stderr_monitor.join()
            self.stderr_monitor = None

    def terminate(self, timeout=5):
        start = time.time()
        self._stop_monitors()
        while self._proc.poll() is None:
            time.sleep(0.5)
            if (time.time() - start) > timeout:
                self._proc.kill()
