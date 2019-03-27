import time
import logging
from tornado.gen import coroutine, sleep

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
        name = "(name: {})".format(name)
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