#-*- coding: utf-8 -*-

import json
import os.path

from contextlib import closing
from httplib import HTTPConnection

class memoize:
    """Decorator, equivalent of the Python 3 functools.lru_cache(None)."""

    def __init__(self, fn):
        """fn: the function to decorate."""

        self.fn = fn
        self.cache = {}

    def __call__(self, *args, **kwds):
        """Check if we already have the answer and return it, otherwise
           compute it and store the result."""

        key = tuple(args) + tuple(kwds)

        if key in self.cache:
            return self.cache[key]

        ans = self.fn(*args, **kwds)
        return self.cache.setdefault(key, ans)

    def renew(self, *args, **kwds):
        """Delete the previous return value of the function for arguments
           *args & **kwds and recompute the result.
        """

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
        if lock_id is not None:
            filepath += '?lock_id=%s' % lock_id

        con.request('GET', filepath)

        r = con.getresponse()

    return r.status != 200


@memoize
def get_server(filepath, host, port):
    """Return a server owning filepath.

       host & port: the address & port of a name server.
    """

    with closing(HTTPConnection(host, port)) as con:
        con.request('GET', filepath)
        response = con.getresponse()
        status, srv = response.status, response.read()

    if status == 200:
        return srv

    return None


def get_lock(filepath, host, port):
    """Try to get a lock from the lockserver (host, port), if not able
       to get it, raise an Exception.

       filepath: the file on which we want the lock
       host & port: the address & port of a lock server.
    """

    with closing(HTTPConnection(host, port)) as con:
        con.request('POST', filepath)
        response = con.getresponse()
        status = response.status

        if status != 200:
            raise Exception('Unable to grant lock on %s.' % filepath)

        lock_id = response.read()

    return lock_id


def revoke_lock(filepath, host, port, lock_id):
    """Revoke the lock on filepath, if it fails to revoke the lock,
       raise an Exception.

       host & port: the address & port of a lock server
       lock_id: the id of the current lock."""

    with closing(HTTPConnection(host, port)) as con:
        con.request('DELETE', filepath + ('?lock_id=%d' % int(lock_id)))
        response = con.getresponse()

    if response.status != 200:
        raise Exception('Unable to revoke lock on %s.' % filepath)

