from katcp import Sensor
import logging

log = logging.getLogger('mpikat.mkrecv_stdout_parser')

MKRECV_STDOUT_KEYS = { "STAT": [("slot-size", int), ("heaps-completed", int),
             ("heaps-discarded", int), ("heaps-needed", int),
             ("payload-expected", int), ("payload-received", int),
             ("global-heaps-completed", int),
             ("global-heaps-discarded", int), ("global-heaps-needed", int),
             ("global-payload-expected", int), ("global-payload-received", int)]
}

def mkrecv_stdout_parser(line):
    log.debug(line)
    tokens = line.split()
    params = {}
    if tokens[0] in MKRECV_STDOUT_KEYS:
        params = {}
        parser = MKRECV_STDOUT_KEYS[tokens[0]]
        for ii, (key, dtype) in enumerate(parser):
            params[key] = dtype(tokens[ii+1])
    return params


class MkrecvSensors:
    def __init__(self, name_suffix):
        """
        List of sensors and handler to be connected to a mkrecv process
        """
        self.sensors = {}

        self.sensors['global_payload_frac'] = Sensor.float(
                    "global-payload-received-fraction-{}".format(name_suffix),
                    description="Ratio of received and expected payload.",
                    params=[0, 1]
                    )

    def stdout_handler(self, line):
        data = mkrecv_stdout_parser(line)
        self.sensors["global_payload_frac"].set_value(float(data["global-heaps-received"]) / float(data["global-payloaf-expected"]))






