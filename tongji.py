# -*- coding: utf-8 -*-
#from bitmex_websocket import BitMEXWebsocket
from time import time
from six import with_metaclass
from bitmex_rest import bitmex
import json
from datetime import datetime, date, timedelta
from dateutil.tz import tzutc
import re
api_key = 'FlvujwcHMC85oPzfML4RgAKY'
api_secret = 't5gttC4Wywcm-SDuizVeHY9KRWeWdbPoEOjwmM3uKctS40a6'

rest_cli = bitmex(False, None, api_key, api_secret)


def get_trade_info(orderID):
    filter = {
        'orderID' : orderID
    } 
    execCost = 0
    execComm = 0
    trade_info = rest_cli.Execution.Execution_getTradeHistory(reverse=True, filter=json.dumps(filter)).result()
    for o in trade_info[0]:
        #orderId= o['orderID']
        #timestamp = o['transactTime']
        execCost += o['execCost']
        execComm += o['execComm']
        orderQty = o['orderQty']
        price = o['price']
        #side = o['side']
        
        #cumQty = o['cumQty']
        #print(price, orderQty, execCost, execComm)
        
    return (price, orderQty, execCost, execComm)

if __name__ == '__main__':
    yesterday = (date.today() + timedelta(days = -1)).strftime("%Y-%m-%d")    # 昨天日期
    print(yesterday)
    # from_time = '2021-04-18 16'
    # to_time = '2021-04-19 15'
    pattern = '(.*) - INFO - orderID\tSell:(.*)-Buy:(.*)\n$'
    cnt = 0
    total = 0
    with open('C:\\Users\\SK\\Documents\\VSCODE\\BITMEX\\output', 'r', encoding='utf-8') as f:
        for line in f.readlines()[::-1]:
            match = re.match(pattern, line)
            if match:
                timestamp = match.group(1)
                sell_id = match.group(2)
                buy_id = match.group(3)
                if timestamp.startswith(yesterday):
                    cnt += 1
                    print(timestamp, sell_id, buy_id)
                    price, qty, sell_cost, sell_comm = get_trade_info(sell_id)
                    if buy_id:
                        price, qty, buy_cost, buy_comm = get_trade_info(buy_id)
                    else:
                        buy_cost = int(-100000000 * qty/(price-250))
                        buy_comm = int(-100000000 * qty/(price-250)*0.00025)
                    cost = sell_cost + buy_cost
                    comm = sell_comm + buy_comm
                    total += cost + comm
                    print(cnt, total/100000, cost+comm, cost, comm)
