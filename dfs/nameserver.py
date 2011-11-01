#-*- coding: utf-8 -*-

import atexit
import logging
import os
import shelve

import web

import utils

class NameServer:
    """NameServer is responsible of the mapping between directory names
       and file servers.
    """

    def GET(self, filepath):
        """Return a server which hold the directory in which filepath is
           located. If filepath is "/" return a list of directory/server.

           filepath: an absolute path to a file (not a directory!)
        """

        web.header('Content-Type', 'text/plain; charset=UTF-8')
        filepath = str(filepath)

        if filepath == '/':
            return '\n'.join('%s=%s' % (dirpath, _names[dirpath])
                    for dirpath in sorted(_names))

        dirpath = str(os.path.dirname(filepath))

        if dirpath in _names:
            return _names[dirpath]

        raise web.notfound('No file server serve this file.')


    def POST(self, dirpath):
        """See _update (with add=True)."""

        return _update(str(dirpath))

    def DELETE(self, dirpath):
        """See _update (with add=False)."""

        return _update(str(dirpath), False)


def _update(dirpath, add=True):
    """Add pair of directory/server to the name server.

        If add:
            If dirpath == '/' (root of NameServer) it add the list of
            directories  contained in 'dirs' (seperated by \\n) query var
            to the name server and associate them with the server
            in 'srv' query var.

            Else associate the 'dirpath' directory to the 'srv' query var.

        Else:
            Act in the same fashion as if add is True except that it
            removes pair directory/servers instead of adding them.

        Obviously there's a security hole here because everybody can
        fake/delete servers, so to be used in real world it would need
        an authentification process, which would allow server to prov
        their identities.
    """

    web.header('Content-Type', 'text/plain; charset=UTF-8')
    i = web.input()

    if 'srv' not in i:
        raise web.badrequest()

    srv = i['srv']

    if dirpath == '/':
        if 'dirs' not in i:
            raise web.badrequest()

        for dirpath in i['dirs'].split('\n'):
            if not dirpath:
                continue

            try:
                _update_names(dirpath, srv, add)
            except ValueError as e:
                logging.exception(e)

    else:
        try:
            _update_names(dirpath, srv, add)
        except ValueError as e:
            logging.exception(e)

    # return ok even if ValueError were raised, why?
    # just be because we were asked to delete a directory/server
    # pair in our dictionnary but it wasn't present so it's like
    # if we've done it
    return 'OK'


def _update_names(dirpath, srv, add=True):
    """Just update the name dictionnary and the database.

       dirpath: the path to the directory to update
       srv: the server to associate to dirpath (not used if add=False)
       add: if True add the couple (dirpath, srv) else delete the dirpath
            from the dictionnary and the database

       Notes: srv is unused when add=False but it still required because
              it'll be useful if we want to implement replication.
    """

    if dirpath[-1] == '/':
        dirpath = os.path.dirname(dirpath)

    if add:
        logging.info('Update directory %s on %s.', dirpath, srv)
        _names[dirpath] = srv

    elif dirpath in _names:
        logging.info('Remove directory %s on %s.', dirpath, srv)
        del _names[dirpath]

    else:
        raise ValueError('%s wasn\'t not deleted, because it wasn\'t'
                         ' in the dictionnary/database.' % dirpath)


_config = {
            'dbfile': 'names.db',
         }

logging.info('Loading config file nameserver.dfs.json.')
utils.load_config(_config, 'nameserver.dfs.json')
_names = shelve.open(_config['dbfile'])

atexit.register(lambda: _names.close())

