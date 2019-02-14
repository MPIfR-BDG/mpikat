"""
This pipeline captures data from the network and passes it to a dada ring buffer for processing by DSPSR.
"""
import logging
import signal
import tempfile
import sys
import shutil
from tornado import gen
import os
import time
from astropy.time import Time
from subprocess import PIPE, Popen
from mpikat.effelsberg.edd.pipeline.dada import render_dada_header, make_dada_key_string
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import shlex
import threading
import base64
from katcp import Sensor
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
        "args": "-cpu 2,3 -L 10 -r -F 128:D -cuda 0,0 -minram 1024"
    },
    "dada_db_params":
    {
        "args": "-n 64 -b 67108864 -p -l",
        "key": "dada"
    },
    "dada_header_params":
    {
        "filesize": 32000000000,
        "telescope": "Effelsberg",
        "instrument": "EDD",
        "frequency_mhz": 1370,
        "receiver_name": "P200-3",
        "bandwidth": 162.5,
        "tsamp": 0.04923076923076923,
        "nbit": 8,
        "ndim": 2,
        "npol": 2,
        "nchan": 8,
        "resolution": 1,
        "dsb": 1
    }

}

sensors = {"ra": 123, "dec": -10, "source-name": "J1939+2134",
           "scannum": 0, "subscannum": 1}


def register_pipeline(name):
    def _register(cls):
        PIPELINES[name] = cls
        return cls
    return _register


class ExecuteCommand(object):

    def __init__(self, command, outpath=None, resident=False):
        self._command = command
        self._resident = resident
        self._outpath = outpath
        self.stdout_callbacks = set()
        self.stderr_callbacks = set()
        self.error_callbacks = set()
        self.fscrunch_callbacks = set()
        self.tscrunch_callbacks = set()
        self._monitor_threads = []
        self._process = None
        self._executable_command = None
        self._monitor_thread = None
        self._stdout = None
        self._stderr = None
        self._error = False
        self._finish_event = threading.Event()

        if not self._resident:
            self._finish_event.set()

        self._executable_command = shlex.split(self._command)

        if RUN:
            try:
                self._process = Popen(self._executable_command,
                                      stdout=PIPE,
                                      stderr=PIPE,
                                      bufsize=1,
                                      # shell=True,
                                      universal_newlines=True)
            except Exception as error:
                log.exception("Error while launching command: {}".format(
                    self._executable_command))
                self.error = True
            if self._process == None:
                self._error = True
            self._monitor_thread = threading.Thread(
                target=self._execution_monitor)
            self._stderr_monitor_thread = threading.Thread(
                target=self._stderr_monitor)
            self._monitor_thread.start()
            self._stderr_monitor_thread.start()
            if self._outpath is not None:
                self._png_monitor_thread = threading.Thread(
                    target=self._png_monitor)
                self._png_monitor_thread.start()

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

    def stderr_notify(self):
        for callback in self.stderr_callbacks:
            callback(self._stderr, self)

    @property
    def stderr(self):
        return self._stderr

    @stderr.setter
    def stderr(self, value):
        self._stderr = value
        self.stderr_notify()

    def fscrunch_notify(self):
        for callback in self.fscrunch_callbacks:
            callback(self._fscrunch, self)

    @property
    def fscrunch(self):
        return self._fscrunch

    @fscrunch.setter
    def fscrunch(self, value):
        self._fscrunch = value
        self.fscrunch_notify()

    def tscrunch_notify(self):
        for callback in self.tscrunch_callbacks:
            callback(self._tscrunch, self)

    @property
    def tscrunch(self):
        return self._tscrunch

    @tscrunch.setter
    def tscrunch(self, value):
        self._tscrunch = value
        self.tscrunch_notify()

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
                stdout = self._process.stdout.readline().rstrip("\n\r")
                if stdout != b"":
                    if (not stdout.startswith("heap")) & (not stdout.startswith("mark")) & (not stdout.startswith("[")) & (not stdout.startswith("-> parallel")) & (not stdout.startswith("-> sequential")):
                        self.stdout = stdout
                    # print self.stdout, self._command

            if not self._finish_event.isSet():
                # For the command which runs for a while, if it stops before
                # the event is set, the command does not successfully finish
                stdout = self._process.stdout.read()
                log.error(
                    "Process exited unexpectedly with return code: {}".format(self._process.returncode))
                log.error("exited unexpectedly, stdout = {}".format(stdout))
                log.error("exited unexpectedly, cmd = {}".format(self._command))
                self.error = True

    def _stderr_monitor(self):
        if RUN:
            while self._process.poll() == None:
                stderr = self._process.stderr.readline().rstrip("\n\r")
                if stderr != b"":
                    self.stderr = stderr
            if not self._finish_event.isSet():
                # For the command which runs for a while, if it stops before
                # the event is set, the command does not successfully finish
                stderr = self._process.stderr.read()
                log.error(
                    "Process exited unexpectedly with return code: {}".format(self._process.returncode))
                log.error("exited unexpectedly, stderr = {}".format(stderr))
                log.error("exited unexpectedly, cmd = {}".format(self._command))
                self.error = True

    def _png_monitor(self):
        if RUN:
            #time.sleep(30)
            while self._process.poll() == None:
                try:
                    with open("{}/fscrunch.png".format(self._outpath), "rb") as imageFile:
                        # print "trying to access
                        # {}/fscrunch.png".format(self._outpath)
                        self.fscrunch = base64.b64encode(imageFile.read())
                    # print png
                except:
                    pass

                try:
                    with open("{}/tscrunch.png".format(self._outpath), "rb") as imageFile:
                        # print "trying to access
                        # {}/fscrunch.png".format(self._outpath)
                        self.tscrunch = base64.b64encode(imageFile.read())
                except:
                    pass
                time.sleep(2)


@register_pipeline("DspsrPipelineP0")
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
        tstr = Time.now().isot.replace(":", "-")  # to fix docker bug
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

        cmd = "dada_junkdb -k {0} -b 262144000000 -r 1024 -g {1}".format(
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


@register_pipeline("DspsrPipelineSrxdev")
class Db2Dbnull(object):
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
        self._sensors = []
        self._volumes = ["/tmp/:/scratch/"]
        self._dada_key = None
        self._config = None
        self._dspsr = None
        self._mkrecv_ingest_proc = None
        self.setup_sensors()

    def setup_sensors(self):
        """
        @brief Setup monitoring sensors
        """
        self._tscrunch = Sensor.string(
            "tscrunch_PNG",
            description="tscrunch png",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._tscrunch)

        self._fscrunch = Sensor.string(
            "fscrunch_PNG",
            description="fscrunch png",
            default=0,
            initial_status=Sensor.UNKNOWN)
        self.sensors.append(self._fscrunch)

    @property
    def sensors(self):
        return self._sensors

    def _decode_capture_stdout(self, stdout, callback):
        log.debug('{}'.format(str(stdout)))

    def _handle_execution_returncode(self, returncode, callback):
        log.debug(returncode)

    def _handle_execution_stderr(self, stderr, callback):
        log.info(stderr)

    def _add_tscrunch_to_sensor(self, png_blob, callback):
        self._tscrunch.set_value(png_blob)

    def _add_fscrunch_to_sensor(self, png_blob, callback):
        self._fscrunch.set_value(png_blob)

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
        self._create_ring_buffer = ExecuteCommand(
            cmd, outpath=None, resident=False)
        self._create_ring_buffer.stdout_callbacks.add(
            self._decode_capture_stdout)
#        self._create_ring_buffer.stderr_callbacks.add(
#            self._handle_execution_stderr)
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
        tstr = Time.now().isot.replace(":", "-")  # to fix docker bug
        in_path = os.path.join("/data/jason/", source_name, tstr, "raw_data")
        out_path = os.path.join(
            "/data/jason/", source_name, tstr, "combined_data")
        self.out_path = out_path
        log.debug("Creating directories")
        cmd = "mkdir -p {}".format(in_path)
        log.debug("Command to run: {}".format(cmd))
        log.debug("Current working directory: {}".format(os.getcwd()))
        self._create_workdir_in_path = ExecuteCommand(
            cmd,  outpath=None, resident=False)
        self._create_workdir_in_path.stdout_callbacks.add(
            self._decode_capture_stdout)
        self._create_workdir_in_path._process.wait()
        cmd = "mkdir -p {}".format(out_path)
        log.debug("Command to run: {}".format(cmd))
        self._create_workdir_out_path = ExecuteCommand(
            cmd,  outpath=None, resident=False)
        self._create_workdir_out_path.stdout_callbacks.add(
            self._decode_capture_stdout)
        self._create_workdir_out_path._process.wait()
        os.chdir(in_path)
        log.debug("Change to workdir: {}".format(os.getcwd()))
        log.debug("Current working directory: {}".format(os.getcwd()))
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
        self._dspsr = ExecuteCommand(cmd,  outpath=None, resident=True)
        self._dspsr.stdout_callbacks.add(
            self._decode_capture_stdout)
        self._dspsr.stderr_callbacks.add(
            self._handle_execution_stderr)
        cmd = "mkrecv_nt --header {} --dada-mode 4".format(
            dada_header_file.name)
        log.debug("Running command: {0}".format(cmd))
        self._mkrecv_ingest_proc = ExecuteCommand(
            cmd,  outpath=None, resident=True)
        self._mkrecv_ingest_proc.stdout_callbacks.add(
            self._decode_capture_stdout)

        cmd = "python /home/psr/software/mpikat/mpikat/effelsberg/edd/pipeline/archive_directory_monitor.py -i {} -o {}".format(
            in_path, out_path)
        log.debug("Running command: {0}".format(cmd))
        self._archive_directory_monitor = ExecuteCommand(
            cmd, outpath=out_path, resident=True)
        self._archive_directory_monitor.stdout_callbacks.add(
            self._decode_capture_stdout)
        self._archive_directory_monitor.fscrunch_callbacks.add(
            self._add_fscrunch_to_sensor)
        self._archive_directory_monitor.tscrunch_callbacks.add(
            self._add_tscrunch_to_sensor)

    @gen.coroutine
    def stop(self):
        """@brief stop the dada_junkdb and dspsr instances."""
        log.debug("Stopping")
        self._timeout = 10
        # if self._mkrecv_ingest_proc
        self._mkrecv_ingest_proc.set_finish_event()
        yield self._mkrecv_ingest_proc.finish()
        log.debug(
            "Waiting {} seconds for _mkrecv_ingest_proc to terminate...".format(self._timeout))
        now = time.time()
        while time.time() - now < self._timeout:
            retval = self._mkrecv_ingest_proc._process.poll()
            if retval is not None:
                log.info("Returned a return value of {}".format(retval))
                break
            else:
                yield time.sleep(0.5)
        else:
            log.warning(
                "Failed to terminate _mkrecv_ingest_proc in alloted time")
            log.info("Killing process")
            self._mkrecv_ingest_proc.kill()

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

        self._archive_directory_monitor.set_finish_event()
        yield self._archive_directory_monitor.finish()

        log.debug(
            "Waiting {} seconds for _archive_directory_monitor to terminate...".format(self._timeout))
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
            self._archive_directory_monitor.kill()
        self.state = "ready"

    def deconfigure(self):
        """@brief deconfigure the dspsr pipeline."""
        self.state = "idle"
        log.debug("Destroying dada buffer")
        cmd = "dada_db -d -k {0}".format(self._dada_key)
        log.debug("Running command: {0}".format(cmd))
        self._destory_ring_buffer = ExecuteCommand(
            cmd, outpath=None, resident=False)
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


@register_pipeline("DspsrPipelineUptotrix")
class Mkrecv2Db(object):
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
        out_path = os.path.join(
            "/media/scratch/mkrecv_testground/", source_name, tstr)
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
        cmd = "dspsr -cpu 0-4 -L 10 -r -F 256:D -minram 1024 -N {source_name} {keyfile}".format(
            #args=self._config["dspsr_params"]["-cpu 2,3 -L 10 -r -F 256:D -minram 1024"],
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
