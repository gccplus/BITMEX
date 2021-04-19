# -*- coding: utf-8 -*-
#from bitmex_websocket import BitMEXWebsocket
from bitmex_rest import bitmex
import json
import datetime
from dateutil.tz import tzutc

api_key = 'FlvujwcHMC85oPzfML4RgAKY'
api_secret = 't5gttC4Wywcm-SDuizVeHY9KRWeWdbPoEOjwmM3uKctS40a6'

rest_cli = bitmex(False, None, api_key, api_secret)

print(dir(rest_cli.Execution))
orders = rest_cli.Execution.Execution_getTradeHistory(reverse=True, count=500).result()
trade_history = {}
for i in range(len(orders[0])):
    o = orders[i]
    orderID = o['orderID']
    timestamp = o['transactTime']
    execCost = o['execCost']
    execComm = o['execComm']
    side = o['side']
    avgPx = o['avgPx']
    cumQty = o['cumQty']
    if side == 'Sell':
        for j in range(i+1, len(orders)):

