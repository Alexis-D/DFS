#-*- coding: utf-8 -*-

import json
import os.path

from contextlib import closing
from httplib import HTTPConnection

# TODO comment
class memoize:
    def __init__(self, fn):
        self.fn = fn
        self.cache = {}

    def __call__(self, *args, **kwds):
        key = tuple(args) + tuple(kwds)

        if key in self.cache:
            return self.cache[key]

        ans = self.fn(*args, **kwds)
        return self.cache.setdefault(key, ans)

    def renew(self, *args, **kwds):
        key = tuple(args) + tuple(kwds)

        if key in self.cache:
            del self.cache[key]

        return self(*args, **kwds)


def load_config(config, filepath):
    """Load the config file filename (JSON) if it exists and updates
       config, otherwise do nothing.
    """

    if not os.path.exists(filepath):
        return

    with open(filepath) as f:
        c = json.loads(f.read())
        config.update(c)


def get_host_port(s):
    """Return a tuple ('host', port) from the string s.
       e.g.: 'localhost:80' → ('localhost', 80)
    """

    host, port = s.split(':')
    return host, int(port)


def is_locked(filepath, host, port, lock_id=None):
    """Ask the lock server host:port if filepath is locked, if lock_id is
       supplied, ask the lock server using this id.
    """

    with closing(HTTPConnection(host, port)) as con:
        if lock_id is None:
            con.request('GET', filepath)

        else:
            con.request('GET', filepath + ('?lock_id=%d' % int(lock_id)))

        r = con.getresponse()

    return r.status != 200


# TODO comment
def get_server(filepath, host, port):
    with closing(HTTPConnection(host, port)) as con:
        con.request('GET', filepath)
        response = con.getresponse()
        status, srv = response.status, response.read()

    if status == 200:
        return srv

    return None


def get_lock(filepath, host, port):
    with closing(HTTPConnection(host, port)) as con:
        con.request('POST', filepath)
        response = con.getresponse()
        status = response.status

        if status != 200:
            raise Exeption('wtf?')

        lock_id = response.read()

    return lock_id


def revoke_lock(filepath, host, port, lock_id):
    with closing(HTTPConnection(host, port)) as con:
        con.request('DELETE', filepath + ('?lock_id=%d' % int(lock_id)))
        r = con.getresponse()

    return r.status != 200

