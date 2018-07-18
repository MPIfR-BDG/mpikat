from tornado.gen import coroutine, Return
from katpoint import Target
from mpikat.fbfuse_ca_server import BaseFbfConfigurationAuthority

class MockFbfConfigurationAuthority(BaseFbfConfigurationAuthority):
    def __init__(self, host, port):
        super(MockFbfConfigurationAuthority, self).__init__(host, port)
        self.sb_return_values = {}
        self.target_return_values = {}

    def set_sb_config_return_value(self, proxy_id, sb_id, return_value):
        self.sb_return_values[(proxy_id, sb_id)] = return_value

    def set_target_config_return_value(self, proxy_id, target_string, return_value):
        target = Target(target_string)
        self.target_return_values[(proxy_id, target.name)] = return_value

    @coroutine
    def get_target_config(self, proxy_id, target_string):
        target = Target(target_string)
        raise Return(self.target_return_values[(proxy_id, target.name)])

    @coroutine
    def get_sb_config(self, proxy_id, sb_id):
        raise Return(self.sb_return_values[(proxy_id, sb_id)])