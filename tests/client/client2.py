#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import random

import dfs.client

if __name__ == '__main__':
    fp = '/wtf/olol'
    dfs.client.unlink(fp)

    f = dfs.client.open(fp, 'wtc')
    f.write('tro lol\n')
    f.close()

    g = dfs.client.File.from_cache(fp)
    g.seek(0, 2)
    g.write('abc\n')
    g.close()

    fp = '/wtf/aaah'
    h = dfs.client.open(fp)
    h.read()
    h.close()

    with dfs.client.open(fp, 'at') as i:
        i.write('aaaah!')

    print (dfs.client.File.from_cache(fp) is None)

