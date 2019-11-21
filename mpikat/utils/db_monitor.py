import logging
import time
from subprocess import Popen, PIPE
from mpikat.utils.pipe_monitor import PipeMonitor

log = logging.getLogger('mpikat.db_monitor')


class DbMonitor(object):
    def __init__(self, key, callback=None):
        self._key = key
        self._callback = callback
        self._dbmon_proc = None
        self._mon_thread = None

    def _stdout_parser(self, line):
        line = line.strip()
        try:
            values = map(int, line.split())
            free, full, clear, written, read = values[5:]
            fraction = float(full)/(full + free)
            params = {
                "fraction-full": fraction,
                "written": written,
                "read": read
                }
            log.debug("{} params: {}".format(self._key, params))
            if self._callback:
                self._callback(params)
            return params
        except Exception:
            return None

    def start(self):
        self._dbmon_proc = Popen(
            ["dada_dbmonitor", "-k", self._key],
            stdout=PIPE, stderr=PIPE, shell=False,
            close_fds=True)
        self._mon_thread = PipeMonitor(
            self._dbmon_proc.stderr,
            self._stdout_parser)
        self._mon_thread.start()

    def stop(self):
        self._dbmon_proc.kill()
        self._mon_thread.join()


if __name__ == "__main__":
    import sys
    logging.basicConfig()
    log.setLevel(logging.DEBUG)
    mon = DbMonitor(sys.argv[1])
    mon.start()
    time.sleep(10)
    mon.stop()
