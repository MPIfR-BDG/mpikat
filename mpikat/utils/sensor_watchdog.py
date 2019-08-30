import threading
import time

class SensorWatchdog(threading.Thread):
    """
    Watchdog thread that checks if the execution stalls without getting
    noticed.  If time between changes of the value of a sensor surpasses  the
    timeout value, the callbaxck function is called.

    Usage examnple:
        wd = SensorWatchdog(self._polarization_sensors[k]["input-buffer-total-write"],
                10 * self._integration_time_status.value(),
                self.watchdog_error)

        # The input buffer should not silently  stall (was? an mkrecv problem).

    """
    def __init__(self, sensor, timeout, callback):
        threading.Thread.__init__(self)
        self.__timeout = timeout
        self.__sensor = sensor
        self.__callback = callback
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.wait(timeout=self.__timeout):
            timestamp, status, value = self.__sensor.read()
            if (time.time() - timestamp) > self.__timeout:
                self.__callback()
                self.stop_event.set()


