#!/usr/bin/env python2
#-*- coding: utf-8 -*-

import web

import dfs.fileserver

urls = (
        '(/.*)', 'dfs.fileserver.FileServer',
       )

app = web.application(urls, globals())

if __name__ == '__main__':
    app.run()

