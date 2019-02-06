import signal
import logging
import tempfile
import coloredlogs
import tornado
import datetime
from tornado import gen
import os
import time
import shutil
from datetime import datetime
from subprocess import check_output, PIPE, Popen
from katcp import AsyncDeviceServer, Sensor, ProtocolFlags, AsyncReply
from katcp.kattypes import request, return_reply, Int, Str, Discrete, Float
from mpikat.effelsberg.edd.pipeline.dada import render_dada_header, make_dada_key_string
import shlex
import threading
log = logging.getLogger("mpikat.effelsberg.edd.pipeline.pipeline")
log.setLevel('DEBUG')

RUN = True

PIPELINES = {}

PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "error"]

CONFIG = {
    "base_output_dir": os.getcwd(),
    "dspsr_params":
    {
        "args": "-cpu 2,3 -L 10 -r -F 256:D -cuda 0,0 -minram 1024"
    },
    "dada_db_params":
    {
        "args": "-n 8 -b 1280000000 -p -l",
        "key": "dada"
    },
    "dada_header_params":
    {
        "filesize": 32000000000,
        "telescope": "Effelsberg",
        "instrument": "asterix",
        "frequency_mhz": 1370,
        "receiver_name": "P200-3",
        "bandwidth": 320,
        "tsamp": 0.00156250,
        "nbit": 8,
        "ndim": 1,
        "npol": 2,
        "nchan": 1,
        "resolution": 1,
        "dsb": 1
    }

}

sensors = {"ra": 123, "dec": -10, "source-name": "J1939+2134",
           "scannum": 0, "subscannum": 1, "timestamp": str(datetime.now().time())}

DESCRIPTION = """
This pipeline captures data from the network and passes it to a dada
ring buffer for processing by DSPSR
""".lstrip()


def register_pipeline(name):
    def _register(cls):
        PIPELINES[name] = cls
        return cls
    return _register


class ExecuteCommand(object):

    def __init__(self, command, resident=False):
        self._command = command
        self._resident = resident
        self.stdout_callbacks = set()
        self.error_callbacks = set()

        self._process = None
        self._executable_command = None
        self._monitor_thread = None
        self._stdout = None
        self._error = False
        #self._stopevent = threading.Event( )

        self._finish_event = threading.Event()
        # print self._command

        if not self._resident:  # For the command which stops immediately, we need to set the event before hand
            self._finish_event.set()

        # log.info(self._command)
        self._executable_command = shlex.split(self._command)
        #self._executable_command = self._command
        # log.info(self._executable_command)

        if RUN:
            try:
                self._process = Popen(self._executable_command,
                                      stdout=PIPE,
                                      stderr=PIPE,
                                      bufsize=1,
                                      # shell=True,
                                      universal_newlines=True)
                #)
                # print "self_process = {}".format(self._process.poll())
            except Exception as error:
                log.exception("Error while launching command: {}".format(
                    self._executable_command))
                self.error = True
            if self._process == None:
                self._error = True
            self._monitor_thread = threading.Thread(
                target=self._execution_monitor)
            self._monitor_thread.start()

    def __del__(self):
        class_name = self.__class__.__name__

    def set_finish_event(self):
        if not self._finish_event.isSet():
            self._finish_event.set()

    def finish(self):
        if RUN:
            self._process.terminate()

    def stdout_notify(self):
        for callback in self.stdout_callbacks:
            callback(self._stdout, self)

    @property
    def stdout(self):
        return self._stdout

    @stdout.setter
    def stdout(self, value):
        self._stdout = value
        self.stdout_notify()

    def error_notify(self):
        for callback in self.error_callbacks:
            callback(self._error, self)

    @property
    def error(self):
        return self._error

    @error.setter
    def error(self, value):
        self._error = value
        self.error_notify()

    def _execution_monitor(self):
        # Monitor the execution and also the stdout for the outside useage
        if RUN:
            while self._process.poll() == None:
                # print "trying to assign the stdout"
                stdout = self._process.stderr.readline().rstrip("\n")
                if stdout != b"":
                    self.stdout = stdout
                    # print self.stdout, self._command
            if not self._finish_event.isSet():
                # For the command which runs for a while, if it stops before
                # the event is set, the command does not successfully finish
                stderr = self._process.stderr.read()
                log.error(
                    "Process exited unexpectedly with return code: {}".format(self._process.returncode))
                self.error = True


@register_pipeline("DspsrPipeline")
class Mkrecv2Db2Dspsr(object):
    """@brief dspsr pipeline class."""

    def __del__(self):
        class_name = self.__class__.__name__

    def notify(self):
        """@brief callback function."""
        for callback in self.callbacks:
            callback(self._state, self)

    @property
    def state(self):
        """@brief property of the pipeline state."""
        return self._state

    @state.setter
    def state(self, value):
        self._state = value
        self.notify()

    def __init__(self):
        """@brief initialize the pipeline."""
        self.callbacks = set()
        self._state = "idle"
        self._volumes = ["/tmp/:/scratch/"]
        self._dada_key = None
        self._config = None
        self._dspsr = None
        self._mkrecv_ingest_proc = None

    def _decode_capture_stdout(self, stdout, callback):
        log.debug('{}'.format(str(stdout)))

    @gen.coroutine
    def configure(self):
        """@brief destroy any ring buffer and create new ring buffer."""
        self._config = CONFIG
        self._dada_key = CONFIG["dada_db_params"]["key"]
        try:
            self.deconfigure()
        except Exception:
            pass
        cmd = "dada_db -k {key} {args}".format(**
                                               self._config["dada_db_params"])
        log.debug("Running command: {0}".format(cmd))
        self._create_ring_buffer = ExecuteCommand(cmd, resident=False)
        self._create_ring_buffer.stdout_callbacks.add(
            self._decode_capture_stdout)
        self._create_ring_buffer._process.wait()
        self.state = "ready"

    @gen.coroutine
    def start(self):
        """@brief start the dspsr instance then turn on dada_junkdb instance."""
        self.state = "running"
        header = self._config["dada_header_params"]
        header["ra"] = sensors["ra"]
        header["dec"] = sensors["dec"]
        source_name = sensors["source-name"]
        try:
            source_name = source_name.split("_")[0]
        except Exception:
            pass
        header["source_name"] = source_name
        header["obs_id"] = "{0}_{1}".format(
            sensors["scannum"], sensors["subscannum"])
        tstr = sensors["timestamp"].replace(":", "-")  # to fix docker bug
        out_path = os.path.join("/beegfs/jason/", source_name, tstr)
        log.debug("Creating directories")
        cmd = "mkdir -p {}".format(out_path)
        log.debug("Command to run: {}".format(cmd))
        log.debug("Current working directory: {}".format(os.getcwd()))
        self._create_workdir = ExecuteCommand(cmd, resident=False)
        self._create_workdir.stdout_callbacks.add(
            self._decode_capture_stdout)
        self._create_workdir._process.wait()
        #process = safe_popen(cmd, stdout=PIPE)
        # process.wait()
        os.chdir(out_path)
        log.debug("Change to workdir: {}".format(os.getcwd()))
        dada_header_file = tempfile.NamedTemporaryFile(
            mode="w",
            prefix="edd_dada_header_",
            suffix=".txt",
            dir=os.getcwd(),
            delete=False)
        log.debug(
            "Writing dada header file to {0}".format(
                dada_header_file.name))
        header_string = render_dada_header(header)
        dada_header_file.write(header_string)
        #log.debug("Header file contains:\n{0}".format(header_string))
        dada_key_file = tempfile.NamedTemporaryFile(
            mode="w",
            prefix="dada_keyfile_",
            suffix=".key",
            dir=os.getcwd(),
            delete=False)
        log.debug("Writing dada key file to {0}".format(dada_key_file.name))
        key_string = make_dada_key_string(self._dada_key)
        dada_key_file.write(make_dada_key_string(self._dada_key))
        log.debug("Dada key file contains:\n{0}".format(key_string))
        dada_header_file.close()
        dada_key_file.close()
        cmd = "dspsr {args} -N {source_name} {keyfile}".format(
            args=self._config["dspsr_params"]["args"],
            source_name=source_name,
            keyfile=dada_key_file.name)
        log.debug("Running command: {0}".format(cmd))
        self._dspsr = ExecuteCommand(cmd, resident=True)
        self._dspsr.stdout_callbacks.add(
            self._decode_capture_stdout)

        ###################
        # Start up MKRECV
        ###################
        # if RUN is True:
        #self._mkrecv_ingest_proc = Popen(["mkrecv","--config",self._mkrecv_config_filename], stdout=PIPE, stderr=PIPE)

        cmd = "dada_junkdb -k {0} -b 320000000000 -r 1024 -g {1}".format(
            self._dada_key,
            dada_header_file.name)
        log.debug("running command: {}".format(cmd))
        self._dada_junkdb = ExecuteCommand(cmd, resident=True)
        self._dada_junkdb.stdout_callbacks.add(
            self._decode_capture_stdout)

    @gen.coroutine
    def stop(self):
        """@brief stop the dada_junkdb and dspsr instances."""
        log.debug("Stopping")
        self._timeout = 10

        self._dada_junkdb.set_finish_event()
        yield self._dada_junkdb.finish()

        log.debug(
            "Waiting {} seconds for dada_junkdb to terminate...".format(self._timeout))
        now = time.time()
        while time.time() - now < self._timeout:
            retval = self._dada_junkdb._process.poll()
            if retval is not None:
                log.info("Returned a return value of {}".format(retval))
                break
            else:
                yield time.sleep(0.5)
        else:
            log.warning("Failed to terminate dada_junkdb in alloted time")
            log.info("Killing process")
            self._dspsr.kill()

        self._dspsr.set_finish_event()
        yield self._dspsr.finish()

        log.debug(
            "Waiting {} seconds for DSPSR to terminate...".format(self._timeout))
        now = time.time()
        while time.time() - now < self._timeout:
            retval = self._dspsr._process.poll()
            if retval is not None:
                log.info("Returned a return value of {}".format(retval))
                break
            else:
                yield time.sleep(0.5)
        else:
            log.warning("Failed to terminate DSPSR in alloted time")
            log.info("Killing process")
            self._dspsr.kill()
        self.state = "ready"

    def deconfigure(self):
        """@brief deconfigure the dspsr pipeline."""
        self.state = "idle"
        log.debug("Destroying dada buffer")
        cmd = "dada_db -d -k {0}".format(self._dada_key)
        log.debug("Running command: {0}".format(cmd))
        self._destory_ring_buffer = ExecuteCommand(cmd, resident=False)
        self._destory_ring_buffer.stdout_callbacks.add(
            self._decode_capture_stdout)
        self._destory_ring_buffer._process.wait()

        #log.debug("Sending SIGTERM to MKRECV process")
        #    self._mkrecv_ingest_proc.terminate()
        #    self._mkrecv_timeout = 10.0
        #    log.debug("Waiting {} seconds for MKRECV to terminate...".format(self._mkrecv_timeout))
        #    now = time.time()
        #    while time.time()-now < self._mkrecv_timeout:
        #        retval = self._mkrecv_ingest_proc.poll()
        #        if retval is not None:
        #            log.info("MKRECV returned a return value of {}".format(retval))
        #            break
        #        else:
        #            yield sleep(0.5)
        #    else:
        #        log.warning("MKRECV failed to terminate in alloted time")
        #        log.info("Killing MKRECV process")
        #        self._mkrecv_ingest_proc.kill()


def main():
    logging.info("Starting pipeline instance")
    server = Mkrecv2Db2Dspsr()
    server.configure()
    server.start()
    server.stop()
    server.deconfigure()

if __name__ == "__main__":
    main()
