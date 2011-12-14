#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import time
import sys

import dfs.client

if __name__ == '__main__':
    filename = '/data/lorem'

    with dfs.client.open(filename, 'wtc') as f:
        f.write('Lorem ipsum dolor sit amet!')
        # fake long process to make the lock expire
        time.sleep(30)

    # shouldn't be reached! (./client4.py will have take the lock...)

    f = dfs.client.File.from_cache(filename)
    if f == None:
        print >> sys.stderr, '3. Cache expired.'
    else:
        print '3.', f.read()

