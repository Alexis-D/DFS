#-*- coding: utf-8 -*-

import atexit
import collections
import copy
import datetime
import logging
import os.path
import random
import shelve

import web

import utils

Lock = collections.namedtuple('Lock', 'lock_id granted last_used')

class LockServer:
    """LockServer is responsible of handling locking on files."""

    def GET(self, filepath):
        """If filepath == '/' just print all the dirs/lock.
           Else if filepath isn't locked, return 200 OK (and no lock_id is
           provided)

           Else If lock_id is in the request and filepath is locked with
           this id:
               - return OK
               - update the last_used field

           Else:
               '409 Conflict'
        """

        web.header('Content-Type', 'text/plain; charset=UTF-8')
        filepath = str(filepath)
        i = web.input()

        if filepath == '/':
            # just a list of file=(granted, last_used)
            return '\n'.join('%s=(%s, %s)' % (filepath,
                   str(_locks[filepath].granted),
                   str(_locks[filepath].last_used),)
                   for filepath in sorted(_locks))

        elif filepath not in _locks and 'lock_id' not in i:
            return 'OK'

        elif 'lock_id' in i:
            lock = _locks.get(filepath, -1)
            try:
                if int(i['lock_id']) == lock.lock_id:
                    # GET shouldn't be used to change state of server
                    # but it's a "good" idea to do it here, because
                    # when a file server will ask the LockServer if
                    # a file is locked it's because it'll have to
                    # modify it so instead of:
                    # file server ask if locked (and with the good lock)
                    # and just after ask to update the last_used part
                    # of the lock
                    # we just ask if locked and update at the same time
                    # easier, and reduce network access
                    #
                    # moreover as the lockid is only known by the file
                    # server and the client there's no risk of CSRF
                    # (assuming that the security service is implemented)
                    _update_lock(filepath)
                    return 'OK'
                else:
                    raise Exception("Bad lock_id")

            except (Exception, ValueError) as e:
                # logging.exception(e)
                _revoke_lock(filepath)
                raise web.conflict()

        elif _lock_expired(filepath):
            # ok GET shouldn't change the state of server BUT in this
            # case it just update it because now we know that the lock
            _revoke_lock(filepath)
            return 'OK'

        # already locked, or wrong lock_id
        raise web.conflict()


    def POST(self, filepath):
        """If filepath == / (useless for now, but allow easy addition
           of transactions: try to get a lock for all filepaths in the
           request data (each one is separated from the next one
           by a newline). If it's possible return a list of
           filepath/lock_id like this:
               /data/sample.in=12345
               /src/linux/kernel.h=42
           If at least one file can't be locked, then no lock is granted
           and a '401 Unauthorized' response is send.

           Else, if there's no lock on filepath, or an old lock,
           grant a new one (and revoke the older one if needed).
           Return a 200 OK, with the lock id.

           If a client want mutliples locks it should request them in one
           query (/) because if it ask in one request for each lock, it may
           create dead locks.
        """

        web.header('Content-Type', 'text/plain; charset=UTF-8')
        filepath = str(filepath)

        if filepath == '/':
            granted_locks = {}

            for filepath in web.data().split('\n'):
                if not filepath:
                    # to allow an empty line at the end of the request data
                    continue

                try:
                    granted_locks[filepath] = _grant_new_lock(filepath)
                except Exception as e:
                    logging.exception(e)

                    # revoking all previoulsy allocated locks
                    for filepath in granted_locks:
                        _revoke_lock(filepath)

                    raise web.unauthorized()

            # list of filename=lock_id
            return '\n'.join('%s=%d' % (filepath, lock_id,)\
                    for filepath, lock_id in granted_locks.items())

        try:
            return _grant_new_lock(filepath)
        except Exception as e:
            logging.exception(e)
            raise web.unauthorized()


    def DELETE(self, filepath):
        """If filepath == '/' revoke all locks, they should be passed
           in the request as two vars: filepaths (containing the list
           of filepaths) and lock_ids (containing the corresponding
           lock_ids).

           Otherwise revoke the lock associated to filename is revoked
           it the lock_id vars match the actual lock_id.
        """

        web.header('Content-Type', 'text/plain; charset=UTF-8')

        filepath = str(filepath)
        i = web.input()

        # allow deletion of multiple locks
        # so it'll be easier to add transactions
        if filepath == '/':
            if 'filepaths' not in i or 'lock_ids' not in i:
                raise web.badrequest()

            for filepath, lock_id in\
                    zip(i['filepaths'].split('\n'), i['lock_ids'].split('\n')):
                if _locks[filepath].lock_id == int(lock_id):
                    _revoke_lock(filepath)

            # return OK even if some lock_ids were wrong
            # because they wanted to revoke them, so we don't need
            # to bother them with some lock_ids they want to revoke
            # are no longer valid
            return 'OK'

        elif filepath in _locks:
            if 'lock_id' in i:
                lock_id = i['lock_id']

                if _locks[filepath].lock_id == int(lock_id):
                    _revoke_lock(filepath)

                # see above for why always ok
                return 'OK'

            raise web.badrequest()

        else:
            return 'OK'


def _lock_expired(filepath):
    """Return True if the lock of filepath reach its expiration."""

    last_used = _locks[filepath].last_used
    return (datetime.datetime.now() - last_used).seconds\
            > _config['lock_lifetime']


def _grant_new_lock(filepath):
    """Check if we can create a new lock, if possible:
       revoke the former lock if needed, create a new one
       and return it's id.
       Otherwise raise an Exception.
    """

    if filepath in _locks:
        if not _lock_expired(filepath):
            # can't revoke the lock, it's still active
            raise Exception('Unable to grant a new lock (%s).' % filepath)

        _revoke_lock(filepath)

    return _new_lock(filepath)


def _new_lock(filepath):
    """Create a new lock for filepath, and return its id."""

    lock_id = random.randrange(0, 32768)
    logging.info('Granting lock (%d) on %s.', lock_id, filepath)
    t = datetime.datetime.now()
    _locks[filepath] = Lock(lock_id, t, t)

    return lock_id


def _update_lock(filepath):
    """Update the last_used fields of locks to now."""

    t = datetime.datetime.now()

    logging.info('Update lock on %s from %s to %s.',
                 filepath, _locks[filepath].last_used, t)

    l = _locks[filepath]
    l = Lock(l.lock_id, l.granted, t)
    _locks[filepath] = l


def _revoke_lock(filepath):
    """Revoke the lock associated to filepath."""

    if filepath in _locks:
        logging.info('Revoking lock on %s.', filepath)
        del _locks[filepath]


_config = {
            'dbfile': 'locks.db',
            'lock_lifetime': 60,
         }

logging.info('Loading config file lockserver.dfs.json.')
utils.load_config(_config, 'lockserver.dfs.json')
_locks = shelve.open(_config['dbfile'])

atexit.register(lambda: _locks.close())

