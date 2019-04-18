import logging
import time
from subprocess import Popen, PIPE
from mpikat.utils.pipe_monitor import PipeMonitor

log = logging.getLogger('mpikat.db_monitor')

class DbMonitor(object):
    def __init__(self, key, callback):
        self._key = key
        self._dbmon_proc = None
        self._mon_thread = None
        self._callback = callback

    def _stdout_parser(self, line):
        line = line.strip()
        try:
            values = map(int, line.split())
            free, full, clear, written, read = values[5:]
            fraction = float(full)/(full + free)
            params = {
                "key": self._key,
                "fraction-full": fraction,
                "written": written,
                "read": read
                }
        except Exception as error:
            log.warning("Unable to parse line {} with error".format(line, str(error)))
            return None
        self._callback(params)

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
        self._mon_thread.stop()
        self._mon_thread.join()
        self._dbmon_proc.terminate()


if __name__ == "__main__":
    import sys
    logging.basicConfig()
    log.setLevel(logging.DEBUG)
    mon = DbMonitor(sys.argv[1])
    mon.start()
    time.sleep(10)
    mon.stop()
