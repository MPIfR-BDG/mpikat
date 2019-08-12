"""
Copyright (c) 2018 Ewan Barr <ebarr@mpifr-bonn.mpg.de>

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
import ipaddress

log = logging.getLogger('mpikat.ip_manager')

SKIP = 1

class IpRangeAllocationError(Exception):
    pass


class ContiguousIpRange(object):
    def __init__(self, base_ip, port, count):
        """
        @brief      Wrapper for a contiguous range of IPs

        @param      base_ip    The first IP address in the range as a string, e.g. '239.11.1.150'
        @param      port       A port number associated with this range of IPs
        @param      count      The number of IPs in the range

        @note       No checks are made to determine whether a given IP is valid or whether
                    the range crosses subnet boundaries.

        @note       This class is intended for managing SPEAD stream IPs, hence the assocated
                    port number and the 'spead://' prefix used in the format_katcp method.
        """
        self._base_ip = ipaddress.ip_address(unicode(base_ip))
        self._ips = [self._base_ip+ii*SKIP for ii in range(count)]
        self._port = port
        self._count = count

    @property
    def count(self):
        return self._count

    @property
    def port(self):
        return self._port

    @property
    def base_ip(self):
        return self._base_ip

    def index(self, ip):
        return self._ips.index(ip)

    def __hash__(self):
        return hash(self.format_katcp())

    def __iter__(self):
        return self._ips.__iter__()

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__, self.format_katcp())

    def split(self, n):
        """
        @brief   Split the Ip range into n subsubsets of preferably equal length
        """
        allocated = 0
        splits = []
        while allocated < self._count:
            available = min(self._count-allocated, n)
            splits.append(ContiguousIpRange(str(self._base_ip+allocated*SKIP),
                          self._port, available))
            allocated+=available
        return splits

    def format_katcp(self):
        """
        @brief  Return a description of this IP range in a KATCP friendly format,
                e.g. 'spead://239.11.1.150+15:7147'
        """
        return "spead://{}+{}:{}".format(str(self._base_ip),
                                         self._count-1, self._port)


class IpRangeManager(object):
    def __init__(self, ip_range):
        """
        @brief  Class for managing allocation of sub-ranges from
                a ContiguousIpRange instance

        @param  ip_range   A ContiguousIpRange instance to be managed
        """
        self._ip_range = ip_range
        self._allocated = [False for _ in ip_range]
        self._allocated_ranges = set()

    def __repr__(self):
        return "<{} {}>".format(self.__class__.__name__,
                                self._ip_range.format_katcp())

    def format_katcp(self):
        """
        @brief      Return a description of full managed (allocated and free) IP range in
                    a KATCP friendly format, e.g. 'spead://239.11.1.150+15:7147'
        """
        return self._ip_range.format_katcp()

    def _free_ranges(self):
        state_ranges = {True: [], False: []}

        def find_state_range(idx, state):
            start_idx = idx
            while idx < len(self._allocated):
                if self._allocated[idx] == state:
                    idx += 1
                else:
                    state_ranges[state].append((start_idx, idx-start_idx))
                    return find_state_range(idx, not state)
            else:
                state_ranges[state].append((start_idx, idx-start_idx))
        find_state_range(0, self._allocated[0])
        return state_ranges[False]

    def largest_free_range(self):
        return max(self._free_ranges(), key=lambda r: r[1])

    def allocate(self, n):
        """
        @brief      Allocate a range of contiguous IPs

        @param      n   The number of IPs to allocate

        @return     A ContiguousIpRange object describing the allocated range
        """
        log.debug("Allocating {} contiguous multicast groups".format(n))
        ranges = self._free_ranges()
        best_fit = None
        for start, span in ranges:
            if span < n:
                continue
            elif best_fit is None:
                best_fit = (start, span)
            elif (span-n) < (best_fit[1]-n):
                best_fit = (start, span)
        if best_fit is None:
            raise IpRangeAllocationError(
                "Could not allocate contiguous range of {} addresses".format(n))
        else:
            start, span = best_fit
            for ii in range(n):
                offset = start+ii
                self._allocated[offset] = True
            allocated_range = ContiguousIpRange(
                str(self._ip_range.base_ip + start * SKIP), self._ip_range.port, n)
            self._allocated_ranges.add(allocated_range)
            log.debug("Allocated range: {}".format(
                allocated_range.format_katcp()))
            return allocated_range

    def free(self, ip_range):
        """
        @brief      Free an allocated IP range

        @param      ip_range  A ContiguousIpRange object allocated through a call to the
                              'allocate' method.
        """
        log.debug("Freeing range: {}".format(ip_range.format_katcp()))
        self._allocated_ranges.remove(ip_range)
        for ip in ip_range:
            self._allocated[self._ip_range.index(ip)] = False


def ip_range_from_stream(stream):
    """
    @brief      Generate a ContiguousIpRange object from a KATCP-style
                stream definition, e.g. 'spead://239.11.1.150+15:7147'

    @param      stream  A KATCP stream string

    @return     A ContiguousIpRange object
    """
    stream = stream.lstrip("spead://")
    ip_range, port = stream.split(":")
    port = int(port)
    try:
        base_ip, ip_count = ip_range.split("+")
        ip_count = int(ip_count)+1
    except ValueError:
        base_ip, ip_count = ip_range, 1
    return ContiguousIpRange(base_ip, port, ip_count)
