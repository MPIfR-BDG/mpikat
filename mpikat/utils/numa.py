import os
import socket
import fcntl
import struct
import pynvml

__numaInfo = None


def updateInfo():
    """
    Updates the info dictionary.
    """
    global __numaInfo
    __numaInfo = {}
    nodes = open("/sys/devices/system/node/possible").read().strip().split('-')

    for node in nodes:
        __numaInfo[node] = {"net_devices":{} }

        cpurange = open('/sys/devices/system/node/node' + node + '/cpulist').read().strip().split('-')
        __numaInfo[node]['cores'] = map(str, range(int(cpurange[0]), int(cpurange[1])+1))
        __numaInfo[node]['gpus'] = []

    # check network devices
    for device in os.listdir("/sys/class/net/"):
        d = "/sys/class/net/" + device + "/device/numa_node"
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if os.path.isfile(d):
            node = open(d).read().strip()
            try:
                ipa = socket.inet_ntoa(fcntl.ioctl(s.fileno(),
                    0x8915,  # SIOCGIFADDR
                    struct.pack('256s', device[:15]))[20:24])
            except IOError:
                pass
            else:
                __numaInfo[node]["net_devices"][device] = ipa

    # check cuda devices:
    pynvml.nvmlInit()

    nGpus = pynvml.nvmlDeviceGetCount()

    for i in range(nGpus):
        handle = pynvml.nvmlDeviceGetHandleByIndex(i)
        pciInfo = pynvml.nvmlDeviceGetPciInfo(handle)

        d = '/sys/bus/pci/devices/' + pciInfo.busId + "/numa_node"
        node = open(d).read().strip()
        __numaInfo[node]['gpus'].append(str(i))


def getInfo():
    """
    Returns dict with info on numa configuration. For every numa node the dict
    contains a dict with the associated ressources.
    """
    global __numaInfo
    if not __numaInfo:
        updateInfo()
    return __numaInfo

if __name__ == "__main__":
    for node, res in getInfo().items():
        print("NUMA Node: {}".format(node))
        print("  CPU Cores: {}".format(", ".join(res['cores'])))
        print("  GPUs: {}".format(", ".join(map(str, res['gpus']))))
        print("  Network interfaces:")
        for nic, ip in res['net_devices'].items():
            print("     {}: {}".format(nic, ip))
