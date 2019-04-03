"""
Copyright (c) 2019 Jason Wu <jwu@mpifr-bonn.mpg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import logging
import time 
from subprocess import PIPE, Popen
import threading
import shlex

log = logging.getLogger("mpikat.effelsberg.edd.pipeline.ExecuteCommand")


RUN = True 

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
            self._monitor_thread.join()
            self._stderr_monitor_thread.join()
            if self._outpath is not None:
                self._png_monitor_thread.join()

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
            time.sleep(30)
            while self._process.poll() == None:
                try:
                    with open("{}/fscrunch.png".format(self._outpath), "rb") as imageFile:
                        self.fscrunch = base64.b64encode(imageFile.read())
                except Exception as error:
                    raise PulsarPipelineError(str(error))
                try:
                    with open("{}/tscrunch.png".format(self._outpath), "rb") as imageFile:
                        self.tscrunch = base64.b64encode(imageFile.read())
                except Exception as error:
                    raise PulsarPipelineError(str(error))
                time.sleep(5)

