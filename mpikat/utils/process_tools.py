import time
import logging
from tornado.gen import coroutine, sleep

log = logging.getLogger('mpikat.process_tools')


class ProcessTimeout(Exception):
    pass


class ProcessException(Exception):
    pass


@coroutine
def process_watcher(process, timeout=120):
    log.debug("Watching process: {}".format(process.pid))
    start = time.time()
    while process.poll() is None:
        yield sleep(0.2)
        if (time.time() - start) > timeout:
            process.kill()
            raise ProcessTimeout
    if process.returncode != 0:
        message = "Process returned non-zero returncode: {}".format(
            process.returncode)
        log.error(message)
        log.error("Process STDOUT dump:\n{}".format(process.stdout.read()))
        log.error("Process STDERR dump:\n{}".format(process.stderr.read()))
        raise ProcessException(
            "Process returned non-zero returncode: {}".format(
                process.returncode))
    else:
        log.debug("Process stdout:\n{}".format(process.stdout.read()))
        log.debug("Process stderr:\n{}".format(process.stderr.read()))