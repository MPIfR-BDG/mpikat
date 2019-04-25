"""
Wrappers for generic compute pipelines on a node
"""
import logging
import os
import glob
import binascii
import docker
import urllib2
import json
from threading import Thread, Event, Lock

log = logging.getLogger("reynard.pipelines")

PIPELINE_STATES = ["idle", "configuring", "ready",
                   "starting", "running", "stopping",
                   "deconfiguring", "failed"]


class Enum(object):
    def __init__(self, vals):
        for ii, val in enumerate(vals):
            self.__setattr__(val.upper(), val)


IDLE, READY, RUNNING, FAILED, COMPLETED = range(5)
STATES = {
    IDLE: "Idle",
    READY: "Ready",
    RUNNING: "Running",
    FAILED: "Failed",
    COMPLETED: "Completed"
}

NVIDA_DOCKER_PLUGIN_HOST = "localhost:3476"

PIPELINE_REGISTRY = {}


class PipelineError(Exception):
    pass


def reynard_pipeline(name,
                     required_sensors=None,
                     required_containers=None,
                     description="",
                     version="",
                     requires_nvidia=False):
    def wrap(cls):
        if name in PIPELINE_REGISTRY:
            log.warning("Conflicting pipeline names '{0}'".format(name))
        PIPELINE_REGISTRY[name] = {
            "description": description,
            "requires_nvidia": requires_nvidia,
            "version": "",
            "class": cls,
            "required_sensors": required_sensors
        }
        return cls
    return wrap


def nvidia_config(addr=NVIDA_DOCKER_PLUGIN_HOST):
    url = 'http://{0}/docker/cli/json'.format(addr)
    resp = urllib2.urlopen(url).read().decode()
    config = json.loads(resp)
    params = {
        "devices": config["Devices"],
        "volume_driver": config["VolumeDriver"],
        "volumes": config["Volumes"]
    }
    return params


def vma_config():
    path = None
    if os.path.isdir("/host-dev/infiniband/"):
        path = "/host-dev/infiniband/"
    elif os.path.isdir("/dev/infiniband/"):
        path = "/dev/infiniband/"
    else:
        raise Exception("Neither /dev/infiniband or /host-dev/infiniband found")
    response = {}
    response["devices"] = []
    rdma_cm = "{}/rdma_cm".format(path)
    if os.path.exists(rdma_cm):
        response["devices"].append("/dev/infiniband/rdma_cm")
    else:
        raise Exception("no rdma_cm device found")
    for device in glob.glob("{}/uverbs*".format(path)):
        response["devices"].append(device.replace("/host-dev/","/dev/"))
    return response


class Watchdog(Thread):
    def __init__(self, name, standdown, callback):
        Thread.__init__(self)
        self._client = docker.from_env()
        self._name = name
        self._disable = standdown
        self._callback = callback
        self.daemon = True

    def _is_dead(self, event):
        return (event["Type"] == "container" and
                event["Actor"]["Attributes"]["name"] == self._name and
                event["status"] == "die")

    def run(self):
        log.debug("Setting watchdog on container '{0}'".format(self._name))
        for event in self._client.events(decode=True):
            if self._disable.is_set():
                log.debug(
                    "Watchdog standing down on container '{0}'".format(
                        self._name))
                break
            elif self._is_dead(event):
                exit_code = int(event["Actor"]["Attributes"]["exitCode"])
                log.debug(
                    "Watchdog activated on container '{0}'".format(
                        self._name))
                log.debug(
                    "Container logs: {0}".format(
                        self._client.api.logs(
                            self._name)))
                self._callback(exit_code)


class Stateful(object):
    def __init__(self, initial_state):
        self._state = initial_state
        self._registry = []
        self._state_lock = Lock()

    def register_callback(self, callback):
        self._registry.append(callback)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, value):
        with self._state_lock:
            self._state = value
            for callback in self._registry:
                callback(self._state, self)


class Pipeline(Stateful):
    def __init__(self):
        self._watchdogs = []
        self._standdown = Event()
        self._lock = Lock()
        self._docker = DockerHelper()
        super(Pipeline, self).__init__("idle")

    def _set_watchdog(self, name, callback=None, persistent=False):
        def internal_callback(exit_code):
            log.debug(
                "Watchdog recieved exit code {1} from '{0}'".format(
                    name, exit_code))
            if persistent or exit_code != 0:
                log.error(
                    ("Watchdog on container {0} "
                     "saw unexpected exit [code: {1}]").format(
                        name, exit_code))
                container = self._docker.get(name)
                log.error("Container logs:\n{0}".format(container.logs()))
                self.stop(failed=True)
            else:
                log.debug("Watchdog on container {0} triggered".format(name))
                if callback is not None:
                    callback()
        guard = Watchdog(
            self._docker.get_name(name),
            self._standdown,
            internal_callback)
        guard.start()
        self._watchdogs.append(guard)

    def _call(self, next_state, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as error:
            log.exception(str(error))
            self.state = "failed"
            raise error
        else:
            self.state = next_state

    def configure(self, config, sensors):
        with self._lock:
            log.info("Configuring pipeline")
            if self.state != "idle":
                raise PipelineError(
                    "Can only configure pipeline in idle state")
            self.state = "configuring"
            self._call("ready", self._configure, config, sensors)

    def _configure(self, config, sensors):
        raise NotImplementedError

    def stop(self, failed=False):
        post_state = "failed" if failed else "ready"
        with self._lock:
            log.info(
                "Stopping pipeline {0}".format(
                    "(failure)" if failed else ""))
            if self.state != "running" and not failed:
                raise PipelineError("Can only stop a running pipeline")
            self.state = "stopping"
            self._standdown.set()
            self._watchdogs = []
            self._call(post_state, self._stop)

    def _stop(self):
        raise NotImplementedError

    def start(self, sensors):
        with self._lock:
            log.info("Starting pipeline")
            if self.state != "ready":
                raise PipelineError(
                    "Pipeline can only be started from ready state")
            self.state = "starting"
            self._standdown.clear()
            self._call("running", self._start, sensors)

    def _start(self):
        raise NotImplementedError

    def deconfigure(self):
        with self._lock:
            log.info("Deconfiguring pipeline")
            if self.state != "ready":
                raise PipelineError(
                    "Pipeline can only be deconfigured from ready state")
            self.state = "deconfiguring"
            self._call("idle", self._deconfigure)

    def _deconfigure(self):
        raise NotImplementedError

    def status(self):
        with self._lock:
            try:
                return self._status()
            except Exception as error:
                log.error(str(error))
                raise PipelineError(
                    "Could not retrieve status [error: {0}]".format(
                        str(error)))

    def _status(self):
        container_info = {}
        if self.state == "running":
            for watchdog in self._watchdogs:
                name = watchdog._name
                container = self._docker._client.containers.get(name)
                logs = container.logs(tail=20)
                logs = "\n".join([i.split("\r")[-1] for i in logs.split("\n")])
                container_info[name] = {
                    "name": container.name,
                    "status": container.status,
                    "procs": container.top(),
                    "logs": container.logs(tail=20)
                }
        return container_info

    def reset(self):
        try:
            self._deconfigure()
        except Exception as error:
            log.warning(
                "Error caught during reset call: {0}".format(
                    str(error)))
        try:
            self._stop()
        except BaseException:
            log.warning(
                "Error caught during reset call: {0}".format(
                    str(error)))
        self.state = "idle"


class DockerHelper(object):
    def __init__(self):
        self._client = docker.from_env()
        self._salt = "_{0}".format(binascii.hexlify(os.urandom(16)))

    def _update_from_key(self, key, params, func):
        if key not in params:
            return
        if not params[key]:
            return
        del params[key]
        updates = func()
        for update_key, update_item in updates.items():
            if update_key not in params:
                params[update_key] = update_item
            elif isinstance(params[update_key], list):
                params[update_key].extend(update_item)
            else:
                raise Exception(
                    "Key clash on parameter update: {0}".format(update_key))

    def run(self, *args, **kwargs):
        self._update_from_key('requires_nvidia', kwargs, nvidia_config)
        self._update_from_key('requires_vma', kwargs, vma_config)

        #Note: This is hardcoded for standard mpifr setups
        #needs to be changed to match setup on target systems
        #should be done via config file
        kwargs["user"] = "50000:50000"
        log.debug(
            "Running Docker containers with args: {0}, {1}".format(
                args, kwargs))
        if "name" in kwargs:
            kwargs["name"] = kwargs["name"] + self._salt
        try:
            return self._client.containers.run(*args, **kwargs)
        except Exception as error:
            raise PipelineError(
                ("Error starting container args='{0}' "
                 "and kwargs='{1}' [error: {2}]").format(
                    repr(args), repr(kwargs), str(error)))

    def get(self, name):
        return self._client.containers.get(name + self._salt)

    def get_name(self, name):
        return name + self._salt
