#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import time
import sys

import dfs.client

if __name__ == '__main__':
    filename = '/data/lorem'

    written = False

    # wait to grab the lock & write
    while not written:
        try:
            with dfs.client.open(filename, 'wtc') as f:
                f.write('The lock expired')
                f.seek(0)
                print '4.', f.read()
                written = True
        except dfs.client.DFSIOError as e:
            print >> sys.stderr, '4.', e
            time.sleep(5)

