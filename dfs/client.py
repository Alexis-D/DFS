#-*- coding: utf-8 -*-

from contextlib import closing
from httplib import HTTPConnection
from tempfile import SpooledTemporaryFile

import utils

class DFSIOError(IOError):
    """Just represent an error (e.g. fil is locked)."""

    pass


class File(SpooledTemporaryFile):
    """Is a distant file, it's stored in memory if it size if less than
       the max_size parameter, otherwise it's stored on the disk.
    """

    def __init__(self, filepath, mode='rt'):
        """filepath: the path of the distant file
           mode: take the same argument as mode argument of the global
                 open().
        """

        self.mode = mode
        self.filepath = filepath
        host, port = utils.get_host_port(_config['nameserver'])
        self.srv = utils.get_server(filepath, host, port)

        if self.srv is None:
            raise DFSIOError('Impossible to find a server that serve %s.'
                    % filepath)

        SpooledTemporaryFile.__init__(self, _config['max_size'], mode)

        host, port = utils.get_host_port(_config['lockserver'])
        if utils.is_locked(filepath, host, port):
            raise DFSIOError('The file %s is locked.' % filepath)

        if 'w' not in mode:
            host, port = utils.get_host_port(self.srv)
            with closing(HTTPConnection(host, port)) as con:
                con.request('GET', filepath)
                response = con.getresponse()
                status = response.status

                if status != 200 and not ('a' in mode and status == 204):
                    raise DFSIOError('Error (%d) while opening file.' % status)

                if status != 204:
                    self.write(response.read())

                if 'r' in mode:
                    self.seek(0)

                self.lock_id = None

        if 'a' in mode or 'w' in mode:
            # automatically gets a lock if we're in write/append mode
            host, port = utils.get_host_port(_config['lockserver'])
            self.lock_id = int(utils.get_lock(filepath, host, port))

    def __exit__(self, exc, value, tb):
        """Send the change to the DFS, and close the file."""

        self.commit()
        return SpooledTemporaryFile.__exit__(self, exc, value, tb)

    def close(self):
        """Send the change to the DFS, and close the file."""

        self.commit()
        SpooledTemporaryFile.close(self)

    def commit(self):
        """Send the local file to the remote fileserver."""

        if 'a' in self.mode or 'w' in self.mode:
            # send the file from the begining
            self.seek(0)
            data = self.read()
            host, port = utils.get_host_port(self.srv)
            with closing(HTTPConnection(host, port)) as con:
                con.request('PUT', self.filepath + '?lock_id=%s' % self.lock_id,
                            data)

                status = con.getresponse().status
                if status != 200:
                    raise DFSIOError('Error (%d) while committing change to'
                                     ' the file.' % status)

        if self.lock_id is not None:
            host, port = utils.get_host_port(_config['lockserver'])
            utils.revoke_lock(self.filepath, host, port, self.lock_id)


def unlink(filepath):
    """Delete the file from the filesystem (if possible)."""

    # ns
    host, port = utils.get_host_port(_config['nameserver'])
    # fs
    fs = utils.get_server(filepath, host, port)
    host, port = utils.get_host_port(fs)

    with closing(HTTPConnection(host, port)) as con:
        con.request('DELETE', filepath)

        status = con.getresponse().status

        if status != 200:
            raise DFSIOError('Error (%d) while deleting %s.' %
                             (status, filepath))


def rename(filepath):
    # TODO
    pass


open = File

_config = {
        'nameserver': None,
        'lockserver': None,
        'max_size': 1024 ** 2,
         } # default
utils.load_config(_config, 'client.dfs.json')

