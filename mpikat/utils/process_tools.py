import time
import logging
import os
from subprocess import Popen, PIPE
from tornado.gen import coroutine, sleep
from mpikat.utils.pipe_monitor import PipeMonitor

log = logging.getLogger('mpikat.process_tools')


class ProcessTimeout(Exception):
    pass


class ProcessException(Exception):
    pass


@coroutine
def process_watcher(process, name=None, timeout=120, allow_fail=False):
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
    if process.returncode != 0 and not allow_fail:
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


@coroutine
def command_watcher(cmd, env={}, umask=0o000, **kwargs):
    """
    @brief Executes a command and watches the process result. Raises an error on non
    zero returncode (except allow_fail is given), and logs command output to
    debug, respectively eror output.

    @param cmd  string or list of commandline arguments used to create the subprocess
    @param env   Dict of environemnet variables set for the process
    @param umask  Default permissions for files created byh this subprocess. Defaults to globally read and writeable!

    """

    def preexec_fn():
        os.umask(umask)

    log.debug("Executing command: {}".format(cmd))
    if isinstance(cmd, str):
        cmd = cmd.split()

    environ = os.environ.copy()
    environ.update(env)
    if env.keys():
        log.debug("Additional environment variabels set: {}".format(env))

    proc = Popen(map(str, cmd), stdout=PIPE, stderr=PIPE, shell=False,
            env=environ, close_fds=True, preexec_fn=preexec_fn)
    yield process_watcher(proc, name=" ".join(cmd), **kwargs)


class ManagedProcess(object):
    def __init__(self, cmdlineargs, env={}, umask=0o000, stdout_handler=None, stderr_handler=None):
        """
        @brief Manages a long running process and passes stdout and stderr to handlers.

        @param cmdlineargs  string or list of commandline arguments used to create the subprocess
        @param env   Dict of environemnet variables set for the process
        @param umask  Default permissions for files created byh this subprocess. Defaults to globally read and writeable!
        @param stdout_handler Handler for ouptut written to stdout
        @param stderr_handler Handler for ouptut written to stderr
        """
        # cmdlineargs to list of strings
        if isinstance(cmdlineargs, str):
            cmdlineargs = cmdlineargs.split()
        cmdlineargs = map(str, cmdlineargs)
        self._cmdl = " ".join(cmdlineargs)

        def preexec_fn():
            os.umask(umask)

        environ = os.environ.copy()
        environ.update(env)

        self._proc = Popen(cmdlineargs, stdout=PIPE, stderr=PIPE,
                           shell=False, env=environ, close_fds=True, preexec_fn=preexec_fn)
        if stdout_handler:
            self._stdout_handler = stdout_handler
        else:
            self._stdout_handler = lambda line: log.debug(self._cmdl + ":\n   " + line.strip())
        if stderr_handler:
            self._stderr_handler = stderr_handler
        else:
            self._stderr_handler = lambda line: log.error(self._cmdl + ":\n   " + line.strip())
        self.stdout_monitor = None
        self.stderr_monitor = None
        self.eop_monitor = None
        self._start_monitors()

    @property
    def pid(self):
        return self._proc.pid

    @property
    def returncode(self):
        return self._proc.returncode

    def is_alive(self):
        return self._proc.poll() is None

    def _start_monitors(self):
        self.stdout_monitor = PipeMonitor(
            self._proc.stdout, self._stdout_handler)
        self.stdout_monitor.start()
        self.stderr_monitor = PipeMonitor(
            self._proc.stderr, self._stderr_handler)
        self.stderr_monitor.start()

    def _stop_monitors(self):
        if self.stdout_monitor is not None:
            self.stdout_monitor.stop()
            self.stdout_monitor.join(3)
            self.stdout_monitor = None
        if self.stderr_monitor is not None:
            self.stderr_monitor.stop()
            self.stderr_monitor.join(3)
            self.stderr_monitor = None

    def terminate(self, timeout=5):
        start = time.time()
        self._stop_monitors()
        log.debug("Trying to terminate process {} ...".format(self._cmdl))
        if self._proc is None:
            log.warning(" process already terminated!".format())
            return

        while self._proc.poll() is None:
            time.sleep(0.5)
            if (time.time() - start) > timeout:
                log.debug("Reached timeout - Killing process")
                self._proc.kill()
                break
        self._proc = None  # delete to avoid zombie process
