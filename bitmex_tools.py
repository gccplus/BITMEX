# -*- coding: utf-8 -*-
#from bitmex_websocket import BitMEXWebsocket
from bitmex_rest import bitmex
import logging
import logging.handlers
import time
import redis
import threading
import json
import sys
#import os


class hedgingStrategy:
    # 开仓价格
    open_price = 0
    # 价格间距
    price_dist = 2
    # 利润间距
    profit_dist = 1
    # 初始仓位
    init_position = 11
    # 最终仓位
    final_position = 40
    # 单位数量,偶数
    unit_amount = 1

    contract_name = 'XBTUSD'
    redis_fragment_list = 'redis_fragment_list_0'
    filled_order_set = 'filled_order_set'

    def __init__(self):
        self.logger = setup_logger()
        test = False
        api_key = 'RbKv_8cp9-EM5sPKhM2-tcIh'
        api_secret = '6VEYWM7x6Cg5Uo3iTNAgD997tvgYT4711eXqTXgU6dj7cCoB'
        test_url = 'https://testnet.bitmex.com/api/v1'
        product_url = 'https://www.bitmex.com/api/v1'
        if test:
            url = test_url
        else:
            url = product_url
        self.cli = bitmex(test=test, api_key=api_key, api_secret=api_secret)
        # self.ws = BitMEXWebsocket(endpoint=url,
        #                           symbols=[self.contract_name],
        #                           api_key=api_key,
        #                           api_secret=api_secret)
        # init redis client
        self.redis_cli = redis.Redis(host='localhost', port=6379, decode_responses=True)

        # threading lock
        self._value_lock = threading.Lock()

        self.logger.info('从redis同步参数')
        if self.redis_cli.llen(self.redis_fragment_list) > 0:
            last_fm = json.loads(self.redis_cli.lindex(self.redis_fragment_list, -1))
            self.logger.info(last_fm)
            self.open_price = last_fm['open_price']
            self.price_dist = last_fm['price_dist']
            self.profit_dist = last_fm['profit_dist']
            self.init_position = last_fm['init_position']
            self.final_position = last_fm['final_position']
            self.unit_amount = last_fm['unit_amount']
            self.unfilled_buy_list = last_fm['buy_list_name']
            self.unfilled_sell_list = last_fm['sell_list_name']
        else:
            self.logger.info('当前redis为空')

    def get_filled_order(self, symbol):
        first_buy_order = None
        first_sell_order = None
        with self._value_lock:
            for order in self.ws.open_orders():
                if order['ordStatus'] == 'Filled' and order['symbol'] == symbol and (
                        not self.redis_cli.sismember(self.filled_order_set, order['orderID'])):
                    if order['side'] == 'Buy':
                        if not first_buy_order:
                            first_buy_order = order
                        else:
                            if order['price'] > first_buy_order['price']:
                                first_buy_order = order
                    else:
                        if not first_sell_order:
                            first_sell_order = order
                        else:
                            if order['price'] < first_sell_order['price']:
                                first_sell_order = order

        if first_buy_order and first_sell_order:
            if first_buy_order['timestamp'] < first_sell_order['timestamp']:
                result_order = first_buy_order
            else:
                result_order = first_sell_order
        elif first_buy_order:
            result_order = first_buy_order
        elif first_sell_order:
            result_order = first_sell_order
        else:
            result_order = None
        return result_order

    def redis_rem(self, name, order_id):
        for i in range(self.redis_cli.llen(name)):
            redis_order = self.redis_cli.lindex(name, i)
            json_redis_order = json.loads(redis_order)
            if json_redis_order['orderID'] == order_id:
                return self.redis_cli.lrem(name, redis_order, 1)
        return 0

    def get_delegated_orders(self, filter=None):
        times = 0
        result = []
        while times < 200:
            self.logger.info('第%s次获取未成交委托' % (times + 1))
            try:
                orders = self.cli.Order.Order_getOrders(reverse=True, filter=json.dumps(filter)).result()
            except Exception as e:
                self.logger.error('get orders error: %s' % e)
                time.sleep(1)
            else:
                for o in orders[0]:
                    if o['ordStatus'] == 'New':
                        result.append(o)
                break
            times += 1

        return result

    def sync_to_redis(self):
        fm = json.loads(self.redis_cli.lindex(self.redis_fragment_list, -1))
        unit_amount = fm['unit_amount']
        unfilled_buy_list = fm['buy_list_name']
        unfilled_sell_list = fm['sell_list_name']
        self.redis_cli.ltrim(unfilled_buy_list, 1, 0)
        self.redis_cli.ltrim(unfilled_sell_list, 1, 0)
        for o in self.get_delegated_orders({'orderQty': unit_amount, 'symbol': self.contract_name}):
            redis_item = {'orderID': o['orderID'],
                          'side': o['side'],
                          'price': o['price'],
                          'orderQty': o['orderQty']
                          }
            if o['side'] == 'Buy':
                self.redis_insert_buy(unfilled_buy_list, redis_item)
            else:
                self.redis_insert_sell(unfilled_sell_list, redis_item)

    def sync_from_redis(self):
        fm = json.loads(self.redis_cli.lindex(self.redis_fragment_list, -1))
        #unit_amount = fm['unit_amount']
        unfilled_buy_list = fm['buy_list_name']
        unfilled_sell_list = fm['sell_list_name']
        new_orders = []
        symbol = 'XBTUSD'
        for i in range(self.redis_cli.llen(unfilled_buy_list)):
            redis_order = self.redis_cli.lindex(unfilled_buy_list, i)
            json_redis_order = json.loads(redis_order)
            new_orders.append({
                'symbol': symbol,
                'side': 'Buy',
                'orderQty': json_redis_order['orderQty'],
                'ordType': 'Limit',
                'price': json_redis_order['price']
            })
        for i in range(self.redis_cli.llen(unfilled_sell_list)):
            redis_order = self.redis_cli.lindex(unfilled_sell_list, i)
            json_redis_order = json.loads(redis_order)
            new_orders.append({
                'symbol': symbol,
                'side': 'Sell',
                'orderQty': json_redis_order['orderQty'],
                'ordType': 'Limit',
                'price': json_redis_order['price']
            })
        self.new_bulk_orders(new_orders)

    def redis_insert_sell(self, name, item):
        flag = False
        for i in range(self.redis_cli.llen(name), 0, -1):
            redis_order = self.redis_cli.lindex(name, i - 1)
            json_redis_order = json.loads(redis_order)
            if json_redis_order['price'] <= item['price']:
                self.redis_cli.linsert(name, 'after', redis_order, json.dumps(item))
                flag = True
                break
        if not flag:
            self.redis_cli.lpush(name, json.dumps(item))

    def redis_insert_buy(self, name, item):
        flag = False
        for i in range(self.redis_cli.llen(name)):
            redis_order = self.redis_cli.lindex(name, i)
            json_redis_order = json.loads(redis_order)
            if json_redis_order['price'] >= item['price']:
                self.redis_cli.linsert(name, 'before', redis_order, json.dumps(item))
                flag = True
                break
        if not flag:
            self.redis_cli.rpush(name, json.dumps(item))

    def redis_update_item(self, name, item):
        flag = False
        for i in range(self.redis_cli.llen(name)):
            redis_order = self.redis_cli.lindex(name, i)
            json_redis_order = json.loads(redis_order)
            if json_redis_order['orderID'] == item['orderID']:
                self.redis_cli.lset(name, i, json.dumps(item))
                flag = True
                break
        return flag

    def cancel_order(self, orderid):
        times = 0
        result = False
        while times < 500:
            self.logger.info('第%s次发起撤销委托, orderId: %s' % (times + 1, orderid))
            try:
                self.cli.Order.Order_cancel(orderID=orderid).result()
            except Exception as e:
                self.logger.error('撤销错误: %s, 1秒后重试' % e)
                time.sleep(1)
            else:
                result = True
                break
            times += 1
        return result

    def cancel_all(self, symbol, filter=None):
        times = 0
        result = False
        while times < 200:
            self.logger.info('第 %s 次Cancle All' % (times + 1))
            try:
                result = self.cli.Order.Order_cancelAll(symbol=symbol, filter=json.dumps(filter)).result()
            except Exception as e:
                self.logger.error('cancel_all error: %s' % e)
            else:
                print(result)
                result = True
                break
            times += 1
        return result

    def amend_bulk_orders(self, orders):
        result = None
        times = 0
        while times < 200:
            self.logger.info('第%s次 amendBulk' % (times + 1))
            try:
                order = self.cli.Order.Order_amendBulk(orders=json.dumps(orders)).result()
            except Exception as e:
                self.logger.error('amendBulk error： %s' % e)
                time.sleep(1)
            else:
                result = order[0]
                for o in order[0]:
                    self.logger.info(
                        '修改成功: side: %s, price: %s, orderid: %s' % (o['side'], o['price'], o['orderID']))
                    redis_item = {'orderID': o['orderID'],
                                  'side': o['side'],
                                  'price': o['price'],
                                  'orderQty': o['orderQty']
                                  }
                    if o['side'] == 'Buy':
                        self.redis_update_item(self.unfilled_buy_list, redis_item)
                    else:
                        self.redis_update_item(self.unfilled_sell_list, redis_item)
                break
            times += 1
        return result

    def new_bulk_orders(self, orders):
        result = None
        times = 0
        while times < 200:
            self.logger.info('第%s次newBulk' % (times + 1))
            try:
                order = self.cli.Order.Order_newBulk(orders=json.dumps(orders)).result()
            except Exception as e:
                self.logger.error('newBulk error： %s' % e)
                time.sleep(1)
            else:
                result = order[0]
                for o in order[0]:
                    self.logger.info(
                        '委托成功: side: %s, price: %s, orderid: %s' % (o['side'], o['price'], o['orderID']))
                    redis_item = {'orderID': o['orderID'],
                                  'side': o['side'],
                                  'price': o['price'],
                                  'orderQty': o['orderQty']
                                  }
                    if o['side'] == 'Buy':
                        self.redis_insert_buy(self.unfilled_buy_list, redis_item)
                    else:
                        self.redis_insert_sell(self.unfilled_sell_list, redis_item)
                break
            times += 1
        return result

    def adjust_pending_orders(self, base_price):
        """
        :return:
        """
        sell_order_amount = self.redis_cli.llen(self.unfilled_sell_list)
        buy_order_amount = self.redis_cli.llen(self.unfilled_buy_list)

        amend_orders = []
        for i in range(buy_order_amount):
            order = json.loads(self.redis_cli.lindex(self.unfilled_buy_list, buy_order_amount - i - 1))
            order_price = base_price - (i + 1) * self.price_dist
            if order_price != order['price']:
                amend_orders.append({
                    'orderID': order['orderID'],
                    'price': order_price
                })
        for i in range(sell_order_amount):
            order = json.loads(self.redis_cli.lindex(self.unfilled_sell_list, i))
            order_price = base_price + self.price_dist * i + self.profit_dist
            if order_price != order['price']:
                amend_orders.append({
                    'orderID': order['orderID'],
                    'price': order_price
                })
        print(len(amend_orders))
        self.amend_bulk_orders(amend_orders)

    def create_orders(self, order_px):
        new_orders = []
        symbol = 'XBTUSD'
        # Sell Order
        self.redis_cli.ltrim(self.unfilled_sell_list, 1, 0)
        self.redis_cli.ltrim(self.unfilled_buy_list, 1, 0)

        for i in range(40):
            new_orders.append({
                'symbol': symbol,
                'side': 'Sell',
                'orderQty': self.unit_amount,
                'ordType': 'Limit',
                'price': order_px + self.price_dist * i + self.profit_dist
            })
        # # Buy Order
        # for i in range(self.final_position - self.init_position):
        #     new_orders.append({
        #         'symbol': symbol,
        #         'side': 'Buy',
        #         'orderQty': self.unit_amount,
        #         'ordType': 'Limit',
        #         'price': order_px - self.price_dist * (i + 1)
        #     })
        self.new_bulk_orders(new_orders)

    def func(self, order_px):
        symbol = 'XBTUSD'
        self.logger.info('开仓...')
        fragment_amount = 0
        # redis_item = {
        #     'open_price': order_px,
        #     'unit_amount': self.unit_amount,
        #     'init_position': self.init_position,
        #     'final_position': self.final_position,
        #     'price_dist': self.price_dist,
        #     'profit_dist': self.profit_dist,
        #     'sell_list_name': 'unfilled_sell_list_%d' % fragment_amount,
        #     'buy_list_name': 'unfilled_buy_list_%d' % fragment_amount,
        # }

        #self.redis_cli.rpush(self.redis_fragment_list, json.dumps(redis_item))

        # self.open_price = redis_item['open_price']
        # self.unit_amount = redis_item['unit_amount']
        # self.unfilled_sell_list = redis_item['sell_list_name']
        # self.unfilled_buy_list = redis_item['buy_list_name']
        # self.logger.info(redis_item)

        self.logger.info('清空redis数据')
        self.redis_cli.ltrim(self.unfilled_sell_list, 1, 0)
        self.redis_cli.ltrim(self.unfilled_buy_list, 1, 0)

        new_orders = []
        # Sell Order
        for i in range(40):
            new_orders.append({
                'symbol': symbol,
                'side': 'Sell',
                'orderQty': self.unit_amount,
                'ordType': 'Limit',
                'price': order_px + self.price_dist * i + self.profit_dist
            })
        self.new_bulk_orders(new_orders)


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - Thread-%(threadName)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    robot = hedgingStrategy()
    """
    usage:
    --adjust 6900
    """
    if len(sys.argv) > 2:
        if sys.argv[1].startswith('--'):
            option = sys.argv[1][2:]
            if option == 'adjust':
                price = float(sys.argv[2])
                robot.adjust_pending_orders(price)
            elif option == 'init':
                price = float(sys.argv[2])
                robot.func(price)
            elif option == 'temp':
                price = float(sys.argv[2])
                robot.create_orders(price)
            elif option == 'syncfrom':
                index = int(sys.argv[2])
                robot.sync_from_redis()
            elif option == 'syncto':
                index = int(sys.argv[2])
                robot.sync_to_redis()
            else:
                print("Unknown option.")
            sys.exit()

    else:
        print('参数错误')
