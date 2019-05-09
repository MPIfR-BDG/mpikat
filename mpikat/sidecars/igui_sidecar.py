import signal
import logging
import tornado
import requests
import types
import pprint
import json
from abc import ABCMeta, abstractmethod
from optparse import OptionParser
from katcp import KATCPClientResource

log = logging.getLogger("mpikat.katcp_to_igui_sidecar")


class IGUILoginException(Exception):
    pass


class IGUIMappingException(Exception):
    pass


class IGUIObject(object):
    __metaclass__ = ABCMeta

    def __init__(self, id_, name, detail, parent, child_map):
        """
        @brief      Abstract base class for IGUI objects

        @param id_         The iGUI ID for the object
        @param name        The iGUI name for the object
        @param detail      A dictionary containing the iGUI description for the object
        @param parent      The parent object for this object
        @param child_map   An IGUIMap subclass instance containing a mapping to any child objects
        """
        self.id = id_
        self.name = name
        self.detail = detail
        self.parent = parent
        self.children = child_map

    def __repr__(self):
        return "<class {}: {}>".format(self.__class__.__name__, self.name)


class IGUIRx(IGUIObject):

    def __init__(self, detail):
        """
        @brief      Class for igui receivers.

        @param  detail   A dictionary containing the iGUI description for the object

        @note  The 'detail' dictionary must contain at minimum 'rx_id' and 'rx_name' keys
        """
        super(IGUIRx, self).__init__(detail["rx_id"], detail["rx_name"],
                                     detail, None, IGUIDeviceMap(self))
        self.devices = self.children


class IGUIDevice(IGUIObject):

    def __init__(self, detail, parent):
        """
        @brief      Class for igui devices.

        @param  detail   A dictionary containing the iGUI description for the object
        @param  parent   The parent object for this object

        @note  The 'detail' dictionary must contain at minimum 'device_id' and 'name' keys
        """
        super(IGUIDevice, self).__init__(detail["device_id"], detail["name"],
                                         detail, parent, IGUITaskMap(self))
        self.tasks = self.children


class IGUITask(IGUIObject):

    def __init__(self, detail, parent):
        """
        @brief      Class for igui tasks.

        @param  detail   A dictionary containing the iGUI description for the object
        @param  parent   The parent object for this object

        @note  The 'detail' dictionary must contain at minimum 'task_id' and 'task_name' keys
        """
        super(IGUITask, self).__init__(detail["task_id"], detail["task_name"],
                                       detail, parent, None)


class IGUIMap(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        """
        @brief      Abstract base class for maps of iGUI objects.
        """
        self._id_to_name = {}
        self._name_to_id = {}
        self._by_id = {}

    def by_name(self, name):
        """
        @brief      Look up an iGUI object by name

        @param      name  The name of the object

        @return     An iGUI object
        """
        return self._by_id[self._name_to_id[name]]

    def by_id(self, id_):
        """
        @brief      Look up an iGUI object by id

        @param      self  The object
        @param      id_   The ID of the iGUI object

        @return     An iGUI object
        """
        return self._by_id[id_]

    @abstractmethod
    def add(self, id_, name, child):
        """
        @brief      Add an iGUI object to this map

        @param      id_    The ID of the object
        @param      name   The name of the object
        @param      child  The child iGUI object
        """
        self._id_to_name[id_] = name
        self._name_to_id[name] = id_
        self._by_id[id_] = child

    def __iter__(self):
        return self._by_id.values().__iter__()

    def _custom_repr(self):
        out = {}
        for id_, child in self._by_id.items():
            if child.children:
                out[repr(child)] = child.children._custom_repr()
            else:
                out[repr(child)] = ''
        return out

    def __repr__(self):
        return pprint.pformat(self._custom_repr(), indent=2)


class IGUIRxMap(IGUIMap):

    def __init__(self):
        """
        @brief      Class for igui receiver map.
        """
        super(IGUIRxMap, self).__init__()

    def add(self, rx):
        """
        @brief      Add and iGUI receiver to the map

        @param      task  A iGUI receiver dictionary
        """
        super(IGUIRxMap, self).add(rx["rx_id"], rx["rx_name"], IGUIRx(rx))


class IGUIDeviceMap(IGUIMap):

    def __init__(self, parent_rx):
        """
        @brief      Class for igui device map.

        @param  parent_rx  The IGUIRx instance that is this device's parent
        """
        super(IGUIDeviceMap, self).__init__()
        self._parent_rx = parent_rx

    def add(self, device):
        """
        @brief      Add and iGUI device to the map

        @param      device  A iGUI device dictionary
        """
        super(IGUIDeviceMap, self).add(device["device_id"], device["name"],
                                       IGUIDevice(device, self._parent_rx))


class IGUITaskMap(IGUIMap):

    def __init__(self, parent_device):
        """
        @brief      Class for igui task map.

        @param   parent_device  The IGUIDevice instance that is this task's parent
        """
        super(IGUITaskMap, self).__init__()
        self._parent_device = parent_device

    def add(self, task):
        """
        @brief      Add and iGUI task to the map

        @param      task  A iGUI task dictionary
        """
        super(IGUITaskMap, self).add(task["task_id"], task["task_name"],
                                     IGUITask(task, self._parent_device))

    def _custom_repr(self):
        return [repr(task) for task in self._by_id.values()]


class IGUIConnection(object):

    def __init__(self, host, user, password):
        """
        @brief      Class for igui connection.

        @detail     This class wraps a connection to an iGUI instance, providing methods
                    to query the server and update task values. Some functions require a
                    valid login. To use these functions the 'login' method of the class
                    must be called with valid credentials

        @param   host   The hostname or IP address for the iGUI server (can include a port number)
        """
        self._session = requests.Session()
        self._logged_in = False
        self._rx_by_name = {}
        self._rx_by_id = {}
        self._devices = {}
        self._tasks = {}
        self.igui_group_id = None
        self._host = host
        self._user = user
        self._password = password
        self._url = "http://{}/getData.php".format(self._host)
        self._headers = {
            'Host': '{}'.format(self._host),
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.5',
            'DNT': '1',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': 'http://{}/icom/home'.format(self._host),
            'Connection': 'keep-alive'}

    def login(self):
        """
        @brief      Login to the iGUI server

        @detail     If the login is unsuccessful and IGUILoginException will be raised

        @param      user      The username
        @param      password  The password
        """
        response = self._session.post(self._url, headers=self._headers,
                                      data=self._make_data("checkUser", [self._user, self._password]))
        if response.text != "true":
            log.debug(response.text)
            raise IGUILoginException(
                "Could not log into iGUI host {}".format(self._host))
        userdata = json.loads(self._session.post(self._url, headers=self._headers,
                                                 data=self._make_data("loadUsers")).text)
        fedata = json.loads(self._session.post(self._url, headers=self._headers,
                                               data=self._make_data("loadFEData")).text)
        for i in range(len(fedata)):
            for j in range(len(userdata)):
                if (fedata[i]['guid'] == userdata[j]['guid']) & (fedata[i]['username'] == self._user):
                    if userdata[j]['group_id'] == '95b109308df411e58cde0800277d0263':
                        log.info('User is an engineer')
                        self.igui_group_id = userdata[j]['group_id']
                    else:
                        raise IGUILoginException(
                            "You are not an engineer, you can't use the sidecar.")
        self._logged_in = True

    def build_igui_representation(self):
        """
        @brief      Builds a representation of the igui data model.

        @detail     The return map is only valid at the moment it is returned. Methods in
                    act directly on the map to register new receivers, devices and tasks
                    but remote changes to iGUI will not be reflected and the representation
                    must be rebuild to synchronize with any server-side changes.

        @return     An IGUIRxMap object
        """
        log.debug("building IGUI rep")
        rx_map = IGUIRxMap()
        for rx in self.load_all_rx_data():
            #        for rx in self.load_all_rx():
            log.debug(rx)
            rx_map.add(rx)
        for device in self.load_all_devices():
            try:
                # log.debug(rx_map.by_id(device["rx_id"]).devices.add(device))
                rx_map.by_id(device["rx_id"]).devices.add(device)
            except TypeError:
                pass
        for task in self.load_all_tasks():
            try:
                # log.debug(rx_map.by_id(task["rx_id"]).devices.by_id(task["device_id"]).tasks.add(task))
                rx_map.by_id(task["rx_id"]).devices.by_id(
                    task["device_id"]).tasks.add(task)
            except TypeError:
                pass
        return rx_map

    def _safe_post(self, *args, **kwargs):
        FAIL_RESPONSE = u'"Not a valid session!"'
        # log.debug("POST request: {}, {}".format(args, kwargs))
        response = self._session.post(*args, **kwargs)
        # log.debug("POST response: {}".format(response.text))
        if response.text == FAIL_RESPONSE:
            self.login()
            response = self._session.post(*args, **kwargs)
            if response.text == FAIL_RESPONSE:
                raise IGUILoginException("Unable to login to IGUI")
        return response

    def _update_task(self, task, value):
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("updateTask", task.id, value))
        return response.text

    def set_rx_status(self, reciever, values):
        """
        @brief      Set the status of a reciever

        @param      reciever  An IGUIRx object to be updated
        @param      values  A list of the new values for the reciever status
        @param      active (Y/N), test_mode (Y/N), port and username

        @return     true or false
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("setRxStatus", reciever.id, values))
        return response.text

    def set_device_status(self, device, value):
        """
        @brief      Set the status of a device

        @param      reciever  An IGUIDevice object to be updated
        @param      values  A value for the reciever status (Y/N)

        @return     true or false
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("setDeviceStatus", device.id, value))
        return response.text

    def update_group_rx_privileges(self, group_id, value):
        """
        @brief      Set the flag for "show_rx" of a task in the IGUI DB

        @param      task   Updated the show_rx flag for the newly created RX
        @param      values  Group ID and rx_id in fn_in, Y/N for the value field

        @note       This returns the text response from the server
        @note       update_group_rx_privileges([group_id,rx_id],"Y")
        @note       update_group_rx_privileges([group_id,rx_id,"update"],"Y")
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("updGrpRXPrivileges", group_id, value))
        return response.text

    def update_group_task_privileges(self, group_id, value):
        """
        @brief      Set the flag for "update task" of a task

        @param      task   An IGUITask object to be updated
        @param      value  The new value for the task

        @note       This returns the text response from the server
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("updGrpTasksPrivileges", group_id, value))
        return response.text

    def set_task_value(self, task, value):
        """
        @brief      Set the value of a task

        @param      task   An IGUITask object to be updated
        @param      value  The new value for the task

        @note       Currently this returns the text response from the server
                    but as the server-side method is not currently setup exactly
                    as is required this is not meaningful. When the server-side
                    implementation is updated, this will be replaced with a success
                    or fail check.
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("setTaskValue", task.id, value))
        return response.text

    def set_task_blob(self, task, value):
        """
        @brief      Update the blob in a task

        @param      task   An IGUITask object to be updated
        @param      value  The new value for the task

        @note       Currently this returns the text response from the server
                    but as the server-side method is not currently setup exactly
                    as is required this is not meaningful. When the server-side
                    implementation is updated, this will be replaced with a success
                    or fail check.
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("setTaskBlob", task.id, value))
        return response.text

    def update_task(self, task, value):
        """
        @brief      Update the value of a task

        @param      task   An IGUITask object to be updated
        @param      value  The new value for the task

        @note       Currently this returns the text response from the server
                    but as the server-side method is not currently setup exactly
                    as is required this is not meaningful. When the server-side
                    implementation is updated, this will be replaced with a success
                    or fail check.
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("updateTask", task.id, value))
        return response.text

    def load_all_devices(self):
        """
        @brief      Gets all iGUI devices.

        @return     An IGUIDeviceMap object.
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("loadAllDevices"))
        devices = response.json()
        if devices == "N":
            log.warning("No devices returned")
            devices = []
        return devices

    def load_all_rx(self):
        """
        @brief      Gets all iGUI recievers.

        @return     An IGUIRxMap object.
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("loadRx"))
        rx = response.json()
        if rx == "N":
            log.warning("No receivers returned")
            rx = []
        return rx

    def load_all_rx_data(self):
        """
        @brief      Gets all iGUI recievers data.

        @return     An IGUIRxMap object.
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("loadRXData"))
        rx = response.json()
        if rx == "N":
            log.warning("No receiver data returned (does the user have engineer priveledges?)")
            rx = []
        return rx

    def load_all_tasks(self):
        """
        @brief      Gets all iGUI tasks.

        @return     An IGUITaskMap object.
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("loadAllTasks"))
        tasks = response.json()
        return tasks

    def create_rx(self, icom_id, params):
        """
        @brief      Create a receiver

        @param      task   Create an IGUIRecever object
        @param      value  icom_id and the parameters for the new receiver

        @return     return a string of a JSON object of the newly created receiver
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("createRx", icom_id, params))
        return response.text

    def delete_rx(self, rx_id):
        """
        @brief      Delete a receiver

        @param      value rx_id of that receiver that you want to delete

        @return     return a true/false value
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("deleteRx", rx_id))
        return response.text

    def create_device(self, reciever, params):
        """
        @brief      Create a device

        @param      task   An IGUIRecever object where a device is added as a child
        @param      value  Name of the new device, version number, active status

        @return     return a string of a JSON object of the newly created device
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("createDevice", reciever.id, params))
        return response.text

    def create_task(self, device, params):
        """
        @brief      Create a task

        @param      task   An IGUIDevice object where a task is added as a child
        @param      value  List of parameters of the new task (task_name, task_type(fe_task_type),
                    task_unit, mysql_task_type,option_available, current_value, current_value_blob,init_val, lower_limit, upper_limit, update_interval)

        @return     IGUItask object

        @note       params example :["TEMP","NONE","C","GETSET","0", "12","blob","0","0","1000","300"]
        """
        response = self._safe_post(self._url, headers=self._headers,
                                   data=self._make_data("createTask", device.id, params))
        return response.text

    def _make_data(self, fn, fn_in='0', fn_in_param='0'):
        data = []

        def _helper(name, param):
            if hasattr(param, "__iter__") and not isinstance(param, types.StringTypes):
                for subparam in param:
                    data.append(("{}[]".format(name), subparam))
            else:
                data.append((name, param))
        _helper("data[fn]", fn)
        _helper("data[fn_in]", fn_in)
        _helper("data[fn_in_param]", fn_in_param)
        return data


class KATCPToIGUIConverter(object):

    def __init__(self, host, port, igui_host, igui_user, igui_pass, igui_rx_id, nodename, numa):
        """
        @brief      Class for katcp to igui converter.

        @param   host             KATCP host address
        @param   port             KATCP port number
        @param   igui_host        iGUI server hostname
        @param   igui_user        iGUI username
        @param   igui_pass        iGUI password
        @param   igui_device_id   iGUI device ID
        """
        self.rc = KATCPClientResource(dict(
            name="test-client",
            address=(host, port),
            controlled=True))
        self.host = host
        self.port = port
        self.igui_host = igui_host
        self.igui_user = igui_user
        self.igui_pass = igui_pass
        self.nodename = nodename
        self.numa = numa
        self.igui_group_id = None
        #self.igui_device_id = igui_device_id
        self.igui_rx_id = igui_rx_id
        self.igui_connection = IGUIConnection(
            self.igui_host, self.igui_user, self.igui_pass)
        self.igui_task_id = None
        self.igui_rxmap = None
        self.ioloop = None
        self.ic = None
        self.api_version = None
        self.implementation_version = None
        self.previous_sensors = set()

    def start(self):
        """
        @brief      Start the instance running

        @detail     This call will trigger connection of the KATCPResource client and
                    will login to the iGUI server. Once both connections are established
                    the instance will retrieve a mapping of the iGUI receivers, devices
                    and tasks and will try to identify the parent of the device_id
                    provided in the constructor.

        @param      self  The object

        @return     { description_of_the_return_value }
        """
        @tornado.gen.coroutine
        def _start():
            log.debug("Waiting on synchronisation with server")
            yield self.rc.until_synced()
            log.debug("Client synced")
            log.debug("Requesting version info")
            # This information can be used to get an iGUI device ID
            response = yield self.rc.req.version_list()
            log.info("response {}".format(response))
            # for internal device KATCP server, response.informs[2].arguments return index out of range
            #_, api, implementation = response.informs[2].arguments
            #self.api_version = api
            #self.implementation_version = implementation
            #log.info("katcp-device API: {}".format(self.api_version))
            #log.info("katcp-device implementation: {}".format(self.implementation_version))
            self.ioloop.add_callback(self.update)
        log.debug("Starting {} instance".format(self.__class__.__name__))
        # self.igui_connection.login()
        #self.igui_connection.login(self.igui_user, self.igui_pass)
        self.igui_rxmap = self.igui_connection.build_igui_representation()
        #log.debug(self.igui_rxmap)
        # Here we do a look up to find the parent of this device
        # Now instead, we use the rx_id directly, create device automatically
        #with the naming convention hostname-worker-numa#

        self.rx = self.igui_rxmap.by_id()
        try:
            log.debug("looking for device named {} from the database".format(self.nodename + "_worker_" + self.numa))
            self.igui_rxmap.by_id(self.igui_rx_id).devices.by_name(self.nodename + "_worker_" + self.numa)
        except KeyError:
            log.debug("device not found, let's add a device")
            paras = (self.nodename + "_worker_" + self.numa, "None", "N")
            result = json.loads(self.igui_connection.create_device(self.rx, paras))
            log.debug("device id = {}".format(result[0]["device_id"]))
            self.igui_device_id = result[0]["device_id"]

        self.rc.start()
        self.ic = self.rc._inspecting_client
        self.ioloop = self.rc.ioloop
        self.ic.katcp_client.hook_inform("interface-changed",
                                         lambda message: self.ioloop.add_callback(self.update))
        self.ioloop.add_callback(_start)

    @tornado.gen.coroutine
    def update(self):
        """
        @brief    Synchronise with the KATCP servers sensors and register new listners
        """
        log.debug("Waiting on synchronisation with server")
        yield self.rc.until_synced()
        log.debug("Client synced")
        current_sensors = set(self.rc.sensor.keys())
        log.debug("Current sensor set: {}".format(current_sensors))
        removed = self.previous_sensors.difference(current_sensors)
        log.debug("Sensors removed since last update: {}".format(removed))
        added = current_sensors.difference(self.previous_sensors)
        log.debug("Sensors added since last update: {}".format(added))
        for name in list(added):
            log.debug(
                "Setting sampling strategy and callbacks on sensor '{}'".format(name))
            # strat3 = ('event-rate', 2.0, 3.0)              #event-rate doesn't work
            # self.rc.set_sampling_strategy(name, strat3)    #KATCPSensorError:
            # Error setting strategy
            # not sure that auto means here
            self.rc.set_sampling_strategy(name, "auto")
            #self.rc.set_sampling_strategy(name, ["period", (10)])
            #self.rc.set_sampling_strategy(name, "event")
            self.rc.set_sensor_listener(name, self._sensor_updated)
        self.previous_sensors = current_sensors

    def _sensor_updated(self, sensor, reading):
        """
        @brief      Callback to be executed on a sensor being updated

        @param      sensor   The sensor
        @param      reading  The sensor reading
        """
        log.debug("Recieved sensor update for sensor '{}': {}".format(
            sensor.name, repr(reading)))
        try:
            rx = self.igui_rxmap.by_id(self.igui_rx_id)
        except KeyError:
            raise Exception(
                "No iGUI receiver with ID {}".format(self.igui_rx_id))
        try:
            device = rx.devices.by_id(self.igui_device_id)
        except KeyError:
            raise Exception(
                "No iGUI device with ID {}".format(self.igui_device_id))
        try:
            #self.igui_rxmap = self.igui_connection.build_igui_representation()
            #device = self.igui_rxmap.by_id(self.igui_rx_id).devices.by_id(self.igui_device_id)
            task = device.tasks.by_name(sensor.name)
        except KeyError:
            if (sensor.name[-3:] == 'PNG'):
                task = json.loads(self.igui_connection.create_task(
                    device, (sensor.name, "NONE", "", "IMAGE", "GET_SET", "0", "0", "0", "-10000000000000000", "10000000000000000", "300")))
            else:
                task = json.loads(self.igui_connection.create_task(
                    device, (sensor.name, "NONE", "", "GETSET", "GET", "0", "0", "0", "-10000000000000000", "10000000000000000", "300")))
            self.igui_task_id = str(task[0]['rx_task_id'])
            self.igui_connection.update_group_task_privileges(
                [self.igui_connection.igui_group_id, self.igui_task_id], "Y")
            self.igui_connection.update_group_task_privileges(
                [self.igui_connection.igui_group_id, self.igui_task_id, "update"], "Y")
            self.igui_rxmap = self.igui_connection.build_igui_representation()
            device = self.igui_rxmap.by_id(
                self.igui_rx_id).devices.by_id(self.igui_device_id)
            task = device.tasks.by_id(self.igui_task_id)

        if (sensor.name[-3:] == 'PNG'):  # or some image type that we finally agreed on
            #Crazy long sensor logging here
            #log.debug(sensor.name)
            #log.debug(sensor.value)
            #log.debug(len(sensor.value))
            self.igui_connection.set_task_blob(task, reading.value)
        else:
            self.igui_connection.set_task_value(task, sensor.value)

    def stop(self):
        """
        @brief      Stop the client
        """
        self.rc.stop()


@tornado.gen.coroutine
def on_shutdown(ioloop, client):
    log.info("Shutting down client")
    yield client.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='The hostname for the KATCP server to connect to')
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='The port number for the KATCP server to connect to')
    parser.add_option('', '--igui_host', dest='igui_host', type=str,
                      help='The hostname of the iGUI interface', default="127.0.0.1")
    parser.add_option('', '--igui_user', dest='igui_user', type=str,
                      help='The username for the iGUI connection')
    parser.add_option('', '--igui_pass', dest='igui_pass', type=str,
                      help='The password for the IGUI connection')
    parser.add_option('', '--igui_rx_id', dest='igui_rx_id', type=str,
                      help='The iGUI receiver ID for the managed device')
    parser.add_option('', '--igui_nodename', dest='igui_nodename', type=str,
                      help='The nodename for the managed device')
    parser.add_option('', '--igui_numa', dest='igui_numa', type=str,
                      help='The numa number for the managed device')
    parser.add_option('', '--log_level', dest='log_level', type=str,
                      help='Logging level', default="INFO")

    (opts, args) = parser.parse_args()
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('mpikat')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(opts.log_level.upper())
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting KATCPToIGUIConverter instance")
    #client = KATCPToIGUIConverter(opts.host, opts.port,
    #                              opts.igui_host, opts.igui_user,
    #                              opts.igui_pass, opts.igui_device_id)
    client = KATCPToIGUIConverter(opts.host, opts.port,
                                  opts.igui_host, opts.igui_user,
                                  opts.igui_pass, opts.igui_rx_id, opts.nodename, opts.numa)

    signal.signal(signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
        on_shutdown, ioloop, client))

    def start_and_display():
        client.start()
        log.info("Ctrl-C to terminate client")

    ioloop.add_callback(start_and_display)
    ioloop.start()

if __name__ == "__main__":
    main()
