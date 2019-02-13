import warnings
import Tkinter as tk
from optparse import OptionParser
from tornado.gen import coroutine
from tornado.ioloop import PeriodicCallback
from katcp.ioloop_manager import IOLoopManager
from katcp.resource_client import KATCPClientResource
from mpikat.effelsberg.status_config import EFF_JSON_CONFIG

KEYS = [
    "project",
    "scannum",
    "subscannum",
    "numsubscans",
    "observing",
    "source-name",
    "ra",
    "dec"
]

VAR_TYPES = {
    int: tk.IntVar,
    float: tk.DoubleVar,
    str: tk.StringVar,
    bool: tk.IntVar
}


class ParameterController(tk.Frame):

    def __init__(self, parent, key, value):
        tk.Frame.__init__(self, parent)
        self.value_type = type(value)
        self._var = VAR_TYPES[self.value_type]()
        self._var.set(value)
        self._label = tk.Label(self, text=key, justify=tk.LEFT, width=20)
        self._label.pack(side=tk.LEFT, padx=5, anchor="e", fill=tk.BOTH)
        validator = self.register(self.validator)
        self._entry = tk.Entry(self, textvariable=self._var, validate='all',
                               validatecommand=(validator, '%P', '%s'))
        self._entry.pack(side=tk.LEFT, expand=1)

    def set_bg(self, colour):
        try:
            self._entry.config(bg=colour)
        except BaseException:
            pass

    def validator(self, value, last_value):
        if not value.strip() and not self.value_type == str:
            self.set_bg('red')
            self.bell()
            return True
        else:
            try:
                self.value_type(value)
            except Exception as error:
                return False
            else:
                self.set_bg('white')
                return True

    def get(self):
        return self._var.get()

    def set(self, value):
        if self.validator(str(value), str(self._var.get())):
            self._var.set(self.value_type(value))


class DictController(tk.Frame):

    def __init__(self, parent, dict_):
        tk.Frame.__init__(self, parent)
        self._dict = {}
        self.update(dict_)

    def update(self, new_dict):
        self._dict.update(new_dict)
        for key, val in sorted(self._dict.items()):
            controller = ParameterController(self, key, val)
            controller.pack()
            self._dict[key] = controller

    def __getitem__(self, key):
        return self._dict[key].get()

    def __setitem__(self, key, value):
        self._dict[key].set(value)

    def as_dict(self):
        output = {}
        for key, val in self._dict.items():
            try:
                output[key] = val.get()
            except ValueError:
                raise ValueError("Invalid value for key '%s'" % key)
        return output


class SensorParameterController(ParameterController):

    def __init__(self, parent, key, value, client):
        ParameterController.__init__(self, parent, key, value)
        self._entry.configure(state=tk.DISABLED)
        self.key = key
        self.controlled = tk.BooleanVar()
        self.controlled.set(0)
        self.ioloop = parent.ioloop
        self._set = tk.Button(self, text="Set",
                              command=self.set_sensor,
                              state=tk.DISABLED)
        self._set.pack(side=tk.LEFT, expand=1)
        self._cont = tk.Button(self, text='Control',
                               command=lambda x=[0]: self.ioloop.add_callback(
                                   self.toggle, x),
                               highlightbackground="green")
        self._cont.pack(side=tk.LEFT, expand=1)
        self.client = client
        self.after(1000, self.get_sensor_value)

    def get_sensor_value(self):
        @coroutine
        def _get():
            yield self.client.until_synced()
            response = yield self.client.req.sensor_value(self.key)
            if not response.reply.reply_ok():
                print "Error: {0}".format(str(response.messages))
            self.set(response.informs[0].arguments[-1])
        if not self.controlled.get():
            self.ioloop.add_callback(_get)
        self.after(1000, self.get_sensor_value)

    @coroutine
    def toggle(self, tog=[0]):
        tog[0] = not tog[0]
        yield self.client.until_synced()
        if tog[0]:
            response = yield self.client.req.sensor_control(self.key)
            if not response.reply.reply_ok():
                print "Error: {0}".format(str(response.messages))
            else:
                self._cont.configure(text='Release', highlightbackground="red")
                self._set.configure(state=tk.NORMAL)
                self._entry.configure(state=tk.NORMAL)
                self.controlled.set(True)
        else:
            response = yield self.client.req.sensor_release(self.key)
            if not response.reply.reply_ok():
                print "Error: {0}".format(str(response.messages))
            self._cont.configure(text='Control', highlightbackground="green")
            self._set.configure(state=tk.DISABLED)
            self._entry.configure(state=tk.DISABLED)
            self.controlled.set(False)

    def set_sensor(self):
        @coroutine
        def _set():
            response = yield self.client.req.sensor_set(self.key, str(self.get()))
            if not response.reply.reply_ok():
                print "Error: {0}".format(str(response.messages))
        if self.controlled.get():
            self.ioloop.add_callback(_set)


class EffelsbergStatusController(tk.Frame):

    def __init__(self, parent, client, keys, ioloop):
        tk.Frame.__init__(self, parent)
        self._params = {}
        self.client = client
        self.keys = keys
        self.ioloop = ioloop
        self.controllers = []
        self.build()

    def _set_all(self):
        for controller in self.controllers:
            controller._set.invoke()

    def build(self):
        for key in self.keys:
            detail = EFF_JSON_CONFIG[key]
            controller = SensorParameterController(
                self, key, detail["default"], self.client)
            controller.pack()
            self.controllers.append(controller)
        self._set_all_but = tk.Button(
            self, text="Set All", command=self._set_all)
        self._set_all_but.pack(expand=1, fill=tk.BOTH, padx=100)


if __name__ == "__main__":
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-H', '--host', dest='host', type=str,
                      help='Host name', default="localhost")
    parser.add_option('-p', '--port', dest='port', type=long,
                      help='Port number to bind to')
    parser.add_option('', '--log_level', dest='log_level', type=str,
                      help='Log level to record', default="INFO")
    (opts, args) = parser.parse_args()
    keys = KEYS
    manager = IOLoopManager()
    manager.setDaemon(True)
    ioloop = manager.get_ioloop()
    manager.start()
    client = KATCPClientResource(dict(
        name="web-interface-client",
        address=(opts.host, opts.port),
        controlled=True))
    ioloop.add_callback(client.start)
    root = tk.Tk()
    c = EffelsbergStatusController(root, client, keys, ioloop)
    c.pack()
    root.mainloop()
