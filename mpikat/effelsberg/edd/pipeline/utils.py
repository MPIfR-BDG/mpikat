import re
import codecs
import sys
import json
from functools import wraps
from katcp import DeviceClient

"""
This regex is used to resolve escaped characters
in KATCP messages
"""
ESCAPE_SEQUENCE_RE = re.compile(r'''
    ( \\U........      # 8-digit hex escapes
    | \\u....          # 4-digit hex escapes
    | \\x..            # 2-digit hex escapes
    | \\[0-7]{1,3}     # Octal escapes
    | \\N\{[^}]+\}     # Unicode characters by name
    | \\[\\'"abfnrtv]  # Single-character escapes
    )''', re.UNICODE | re.VERBOSE)


def escape_string(s):
    return s.replace(" ", "\_")


def unescape_string(s):
    def decode_match(match):
        return codecs.decode(match.group(0), 'unicode-escape')
    return ESCAPE_SEQUENCE_RE.sub(decode_match, s)


def decode_katcp_message(s):
    """
    @brief      Render a katcp message human readable

    @params s   A string katcp message
    """
    return unescape_string(s).replace("\_", " ")


def pack_dict(x):
    return escape_string(json.dumps(x, separators=(',', ':')))


def unpack_dict(x):
    try:
        return json.loads(decode_katcp_message(x))
    except:
        return json.loads(x.replace("\_"," ").replace("\n","\\n"))


class StreamClient(DeviceClient):
    def __init__(self, server_host, server_port, stream=sys.stdout):
        self.stream = stream
        super(StreamClient, self).__init__(server_host, server_port)

    def to_stream(self, prefix, msg):
        self.stream.write("%s:\n%s\n" %
                          (prefix, decode_katcp_message(msg.__str__())))

    def unhandled_reply(self, msg):
        """Deal with unhandled replies"""
        self.to_stream("Unhandled reply", msg)

    def unhandled_inform(self, msg):
        """Deal with unhandled replies"""
        self.to_stream("Unhandled inform", msg)

    def unhandled_request(self, msg):
        """Deal with unhandled replies"""
        self.to_stream("Unhandled request", msg)


class DocInherit(object):
    """
    Docstring inheriting method descriptor

    The class itself is also used as a decorator
    """

    def __init__(self, mthd):
        self.mthd = mthd
        self.name = mthd.__name__

    def __get__(self, obj, cls):
        if obj:
            return self.get_with_inst(obj, cls)
        else:
            return self.get_no_inst(cls)

    def get_with_inst(self, obj, cls):

        overridden = getattr(super(cls, obj), self.name, None)

        @wraps(self.mthd, assigned=('__name__', '__module__'))
        def f(*args, **kwargs):
            return self.mthd(obj, *args, **kwargs)

        return self.use_parent_doc(f, overridden)

    def get_no_inst(self, cls):

        for parent in cls.__mro__[1:]:
            overridden = getattr(parent, self.name, None)
            if overridden:
                break

        @wraps(self.mthd, assigned=('__name__', '__module__'))
        def f(*args, **kwargs):
            return self.mthd(*args, **kwargs)

        return self.use_parent_doc(f, overridden)

    def use_parent_doc(self, func, source):
        if source is None:
            raise NameError("Can't find '%s' in parents" % self.name)
        func.__doc__ = source.__doc__
        return func


doc_inherit = DocInherit
