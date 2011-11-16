#-*- coding: utf-8 -*-

import logging
import os.path
import time

from contextlib import closing
from httplib import HTTPConnection

import web

import utils

class FileServer:
    """Represent a fileserver which is responsible of holding & sharing
       files.
    """

    def GET(self, filepath):
        """Return the requested file if it's not locked, or if the correct
           lock is provided using the lock_id var.
        """

        web.header('Content-Type', 'text/plain; charset=UTF-8')

        _raise_if_dir_or_not_servable(filepath)
        _raise_if_not_exists(filepath)
        _raise_if_locked(filepath)

        p = _get_local_path(filepath)
        web.header('Last-Modified', time.ctime(os.path.getmtime(p)))
        with open(p) as f:
            return f.read()

    def PUT(self, filepath):
        """Replace the file by the data in the request."""

        _raise_if_dir_or_not_servable(filepath)
        _raise_if_locked(filepath)

        p = _get_local_path(filepath)

        with open(p, 'w') as f:
            f.write(web.data())

        web.header('Last-Modified', time.ctime(os.path.getmtime(p)))

        return ''

    def DELETE(self, filepath):
        """Remove the filepath if it's unlocked, or if the correct
           lock_id is supplied in 'lock_id'.
        """

        web.header('Content-Type', 'text/plain; charset=UTF-8')

        _raise_if_dir_or_not_servable(filepath)
        _raise_if_not_exists(filepath)
        _raise_if_locked(filepath)

        os.unlink(_get_local_path(filepath))
        return 'OK'

    def HEAD(self, filepath):
        """If the file exists/isn't locked, return the last-modified http
           header which corresponds to the last time was modified."""

        web.header('Content-Type', 'text/plain; charset=UTF-8')

        _raise_if_dir_or_not_servable(filepath)
        _raise_if_not_exists(filepath)
        _raise_if_locked(filepath)

        p = _get_local_path(filepath)
        web.header('Last-Modified', time.ctime(os.path.getmtime(p)))
        return ''


def _get_local_path(filepath):
    """Convert the filepath uri to an absolute path in the FS."""

    return os.path.join(os.getcwd(), _config['fsroot'], filepath[1:])


def _raise_if_locked(filepath):
    """Raise a 401 unauthorized it the filepath is locked, and the
       appropriate locked wasn't given in the request.
    """

    i = web.input()

    host, port = utils.get_host_port(_config['lockserver'])
    if utils.is_locked(filepath, host, port, i.get('lock_id', None)):
        raise web.unauthorized()


def _raise_if_dir_or_not_servable(filepath):
    """Raise a 406 notacceptable if the filepath isn't supposed to be
       served, or if it's a directory.
    """

    p = _get_local_path(filepath)

    if (os.path.dirname(filepath) not in _config['directories'] or
            os.path.isdir(p)):
        # request a file which this server isn't supposed to serve!
        raise web.notacceptable()


def _raise_if_not_exists(filepath):
    """Raise a 204 No Content if the file doesn't exists."""

    p = _get_local_path(filepath)

    if not os.path.exists(p):
        raise web.webapi.HTTPError('204 No Content',
                                   {'Content-Type': 'plain/text'})


def _init_file_server():
    """Just notify the nameserver about which directories we serve."""

    host, port = utils.get_host_port(_config['nameserver'])
    with closing(HTTPConnection(host, port)) as con:
        data = 'srv=%s&dirs=%s' % (_config['srv'],
                                '\n'.join(_config['directories']),)
        con.request('POST', '/', data)


_config = {
        'lockserver': None,
        'nameserver': None,
        'directories': [],
        'fsroot': 'fs/',
        'srv': None,
        }

logging.info('Loading config file fileserver.dfs.json.')
utils.load_config(_config, 'fileserver.dfs.json')

# just to speed up the search to know if we can serve a file
# O(n) → O(log n)
_config['directories'] = set(_config['directories'])

_init_file_server()

