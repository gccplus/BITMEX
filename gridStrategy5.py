# -*- coding: utf-8 -*-
from bitmex_websocket import BitMEXWebsocket
from bitmex_rest import bitmex
import logging
import logging.handlers
import time
import redis
import threading
import json


class GridStrategy:
    # 开仓价格
    open_price = 100000
    # 价格间距
    price_dist = 2
    # 利润间距
    profit_dist = 2
    # 初始仓位
    init_position = 51

    # 最终仓位
    final_position = 102
    # 单位数量,每一个fragment不一样
    unit_amount = 1

    contract_name = 'XBTZ19'
    redis_fragment_list = 'redis_fragment_list'
    filled_order_set = 'filled_order_set'
    setting_ht = 'grid_setting_hash'

    def __init__(self):
        self.logger = setup_logger()
        test = False
        api_key = 'dbS7FklMUz4A0Ftf_0eb-khj'
        api_secret = 'UGbHj7ucCrz1xz5slMhPPAV72wemdXxxMk4J2OS_73foWObM'
        test_url = 'https://testnet.bitmex.com/api/v1'
        product_url = 'https://www.bitmex.com/api/v1'
        if test:
            url = test_url
        else:
            url = product_url
        self.cli = bitmex(test=test, api_key=api_key, api_secret=api_secret)
        self.ws = BitMEXWebsocket(endpoint=url,
                                  symbols=[self.contract_name],
                                  api_key=api_key,
                                  api_secret=api_secret)
        # init redis client
        self.redis_cli = redis.Redis(host='localhost', port=6379, decode_responses=True)

        # threading lock
        self._value_lock = threading.Lock()
        self.unfilled_sell_list = ''
        self.unfilled_buy_list = ''

        self.logger.info('从redis同步参数')
        if self.redis_cli.llen(self.redis_fragment_list) > 0:
            fm = json.loads(self.redis_cli.lindex(self.redis_fragment_list, -1))
            self.logger.info(fm)
            self.open_price = fm['open_price']
            self.price_dist = fm['price_dist']
            self.profit_dist = fm['profit_dist']
            self.init_position = fm['init_position']
            self.final_position = fm['final_position']
            self.unit_amount = fm['unit_amount']
            self.unfilled_buy_list = fm['buy_list_name']
            self.unfilled_sell_list = fm['sell_list_name']
        else:
            self.logger.info('当前redis为空')

    def get_filled_order(self, symbol):
        first_buy_order = None
        first_sell_order = None
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

    def send_order(self, symbol, side, qty, price):
        times = 0
        result = None
        while times < 500:
            self.logger.info('第%s次发起委托 side: %s, price: %s' % (times + 1, side, price))
            try:
                order = self.cli.Order.Order_new(symbol=symbol, side=side, orderQty=qty, price=price,
                                                 ordType='Limit', execInst='ParticipateDoNotInitiate').result()
            except Exception as e:
                if 'insufficient Available Balance' in str(e):
                    self.logger.info('余额不足，委托取消 %s' % e)
                    break
                elif '403 Forbidden' in str(e):
                    self.logger.info('403错误，委托取消 %s' % e)
                    break
                self.logger.error('订单error: %s,1秒后重试' % e)
                time.sleep(1)
            else:
                result = order[0]
                if result['ordStatus'] == 'Canceled':
                    if side == 'Buy':
                        return self.send_order(symbol, side, qty, price - self.price_dist)
                    else:
                        return self.send_order(symbol, side, qty, price + self.price_dist)
                self.logger.info(
                    '委托成功: side: %s, price: %s, orderid: %s' % (result['side'], result['price'], result['orderID']))
                redis_item = {'orderID': result['orderID'],
                              'side': result['side'],
                              'price': result['price'],
                              'orderQty': result['orderQty']
                              }
                if side == 'Buy':
                    self.redis_insert_buy(self.unfilled_buy_list, redis_item)
                else:
                    self.redis_insert_sell(self.unfilled_sell_list, redis_item)
                break
            times += 1
        return result

    def redis_rem(self, name, order_id):
        for i in range(self.redis_cli.llen(name)):
            redis_order = self.redis_cli.lindex(name, i)
            json_redis_order = json.loads(redis_order)
            if json_redis_order['orderID'] == order_id:
                return self.redis_cli.lrem(name, 1, order_id)
        return 0

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
                print(len(result))
                result = True
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
                # print(result)
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

    def run(self):
        self.logger.info('start')
        buy_amount = 0
        sell_amount = 0
        last_buy_qty = 0
        while True:
            filled_order = self.get_filled_order(self.contract_name)
            if filled_order:
                order_id = filled_order['orderID']
                cum_qty = filled_order['cumQty']
                order_px = filled_order['price']
                side = filled_order['side']
                ord_type = filled_order['ordType']
                symbol = filled_order['symbol']
                self.logger.info('--------------------------------------------------------------------------------')
                self.logger.info('side: %s, type: %s, symbol: %s, cum_qty: %s, order_px: %s, orderID: %s' %
                                 (side, ord_type, symbol, cum_qty, order_px, order_id))

                # init_position最好是素数,默认11
                if cum_qty % self.init_position == 0:
                    if order_px < self.open_price:
                        self.redis_cli.rpop(self.redis_fragment_list)
                        self.logger.info('清空redis: %s, %s' % (self.unfilled_buy_list, self.unfilled_sell_list))
                        self.redis_cli.ltrim(self.unfilled_buy_list, 1, 0)
                        self.redis_cli.ltrim(self.unfilled_sell_list, 1, 0)
                        self.logger.info('取消委托')
                        self.cancel_all(symbol, {'orderQty': self.unit_amount})

                    self.logger.info('开仓...')
                    self.init_position = int(self.redis_cli.hget(self.setting_ht, 'init_position'))
                    self.final_position = int(self.redis_cli.hget(self.setting_ht, 'final_position'))
                    self.price_dist = float(self.redis_cli.hget(self.setting_ht, 'price_dist'))
                    self.profit_dist = float(self.redis_cli.hget(self.setting_ht, 'profit_dist'))

                    fragment_amount = self.redis_cli.llen(self.redis_fragment_list)
                    redis_item = {
                        'open_price': order_px,
                        'unit_amount': int(cum_qty / self.init_position),
                        'init_position': self.init_position,
                        'final_position': self.final_position,
                        'price_dist': self.price_dist,
                        'profit_dist': self.profit_dist,
                        'sell_list_name': 'unfilled_sell_list_%d' % fragment_amount,
                        'buy_list_name': 'unfilled_buy_list_%d' % fragment_amount,
                    }

                    self.redis_cli.rpush(self.redis_fragment_list, json.dumps(redis_item))

                    self.open_price = redis_item['open_price']
                    self.unit_amount = redis_item['unit_amount']
                    self.unfilled_sell_list = redis_item['sell_list_name']
                    self.unfilled_buy_list = redis_item['buy_list_name']
                    self.logger.info(redis_item)

                    self.logger.info('清空redis: %s, %s' % (self.unfilled_buy_list, self.unfilled_sell_list))
                    self.redis_cli.ltrim(self.unfilled_sell_list, 1, 0)
                    self.redis_cli.ltrim(self.unfilled_buy_list, 1, 0)

                    new_orders = []
                    # Sell Order
                    for i in range(self.init_position):
                        new_orders.append({
                            'symbol': symbol,
                            'side': 'Sell',
                            'orderQty': self.unit_amount,
                            'ordType': 'Limit',
                            'price': order_px + self.price_dist * i + self.profit_dist
                        })
                    # Buy Order
                    for i in range(self.final_position - self.init_position):
                        new_orders.append({
                            'symbol': symbol,
                            'side': 'Buy',
                            'orderQty': self.unit_amount,
                            'ordType': 'Limit',
                            'price': order_px - self.price_dist * (i + 1)
                        })
                    self.new_bulk_orders(new_orders)

                else:
                    if side == 'Sell':

                        self.redis_rem(self.unfilled_sell_list, order_id)
                        price = order_px - self.profit_dist
                        self.send_order(symbol, 'Buy', self.unit_amount, price)
                        sell_amount += 1

                    else:
                        if 0 < last_buy_qty != cum_qty:
                            self.redis_cli.rpop(self.redis_fragment_list)
                            self.cancel_all(symbol, {'orderQty': last_buy_qty})
                            fm = json.loads(self.redis_cli.lindex(self.redis_fragment_list, -1))
                            self.logger.info(fm)
                            self.open_price = fm['open_price']
                            self.price_dist = fm['price_dist']
                            self.profit_dist = fm['profit_dist']
                            self.init_position = fm['init_position']
                            self.final_position = fm['final_position']
                            self.unit_amount = fm['unit_amount']
                            self.unfilled_buy_list = fm['buy_list_name']
                            self.unfilled_sell_list = fm['sell_list_name']

                        self.redis_rem(self.unfilled_buy_list, order_id)

                        price = order_px + self.profit_dist
                        self.send_order(symbol, 'Sell', self.unit_amount, price)
                        buy_amount += 1

                        # if order_px > self.open_price - self.price_dist * self.init_position + self.profit_dist:
                        #     price = order_px + self.profit_dist
                        #     self.send_order(symbol, 'Sell', self.unit_amount, price)
                        #     buy_amount += 1
                        # else:
                        #     self.redis_cli.rpop(self.redis_fragment_list)
                        #     self.cancel_all(symbol, {'orderQty': cum_qty})

                            # new order
                            #self.send_order(symbol, 'Buy', self.unit_amount, price)

                        last_buy_qty = cum_qty
                self.logger.info('TOTAL: %d\tBUY: %d\tSELL: %d' % (sell_amount + buy_amount, buy_amount, sell_amount))
                self.redis_cli.sadd(self.filled_order_set, filled_order['orderID'])
            time.sleep(0.2)


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    robot = GridStrategy()
    robot.run()
