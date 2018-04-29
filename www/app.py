#/usr/bin/python3
# -*- coding: utf-8 -*-

__author__ = 'fanle'

'''
async web application.
'''

import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web

def index(request):
    return web.Response(body='<h1>i am zaza</h1>', headers={'content-type':'text/html'})

async def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 9090)
    logging.info('server started at http://127.0.0.1:9090...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()

