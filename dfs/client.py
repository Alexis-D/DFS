#-*- coding: utf-8 -*-

from contextlib import closing
from httplib import HTTPConnection
from tempfile import SpooledTemporaryFile

import utils

# TODO comment
# TODO unlink, rename?, mv

class DFSIOError(IOError):
    pass


class File(SpooledTemporaryFile):
    def __init__(self, filepath, mode='rt'):
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
            # TODO meaningful exceptions
            raise DFSIOError('wtf?')

        if 'w' not in mode:
            host, port = utils.get_host_port(self.srv)
            with closing(HTTPConnection(host, port)) as con:
                con.request('GET', filepath)
                response = con.getresponse()
                status = response.status

                if status != 200 and not ('a' in mode and status == 204):
                    # TODO meaningful exceptions
                    raise DFSIOError('wtf?')

                if status != 204:
                    self.write(response.read())

                if 'r' in mode:
                    self.seek(0)

                self.lock_id = None

        if 'a' in mode or 'w' in mode:
            host, port = utils.get_host_port(_config['lockserver'])
            self.lock_id = int(utils.get_lock(filepath, host, port))

    def __exit__(self, exc, value, tb):
        self.commit()
        return SpooledTemporaryFile.__exit__(self, exc, value, tb)

    def close(self):
        # TODO try
        self.commit()
        SpooledTemporaryFile.close(self)

    def commit(self):
        if 'a' in self.mode or 'w' in self.mode:
            self.seek(0)
            data = self.read()
            host, port = utils.get_host_port(self.srv)
            with closing(HTTPConnection(host, port)) as con:
                con.request('PUT', self.filepath + '?lock_id=%s' % self.lock_id,
                            data)
                # TODO check stattus value

        if self.lock_id is not None:
            host, port = utils.get_host_port(_config['lockserver'])
            # TODO check return
            utils.revoke_lock(self.filepath, host, port, self.lock_id)


open = File

_config = {
        'nameserver': None,
        'lockserver': None,
        'max_size': 1024 ** 2,
         } # default
utils.load_config(_config, 'client.dfs.json')

