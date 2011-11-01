#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import web

import dfs.lockserver

urls = (
        '(/.*)', 'dfs.lockserver.LockServer',
       )

app = web.application(urls, globals())

if __name__ == '__main__':
    app.run()

