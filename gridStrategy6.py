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
    init_position = 47
    # 最终仓位
    final_position = 94

    contract_names = ['XBTUSD', 'XBTZ19']
    filled_order_set = 'filled_order_set'
    setting_ht = 'grid_setting_hash'

    def __init__(self):
        self.logger = setup_logger()
        test = True
        api_key = 'kVfKITnQdJEzEC2sKYlVr9mM'
        api_secret = 'joccPUd5_DwOd3CDL1lSq_prKDxxM6oRQCmu7aALcw_6KWCi'
        test_url = 'https://testnet.bitmex.com/api/v1'
        product_url = 'https://www.bitmex.com/api/v1'
        if test:
            url = test_url
            self.filled_order_set = 'filled_order_set2'
            self.setting_ht = 'grid_setting_hash2'
            self.api_key = 'iU5CvQzE8-dAkGJ1syfcgnp0'
            self.api_secret = 'OIzn3ooCBTVP6g-WEMokNCRME82ut1-hNLtd48Enf6wPKrq2'
        else:
            url = product_url
        self.cli = bitmex(test=test, api_key=api_key, api_secret=api_secret)
        self.ws = BitMEXWebsocket(endpoint=url,
                                  symbols=self.contract_names,
                                  api_key=api_key,
                                  api_secret=api_secret)
        # init redis client
        self.redis_cli = redis.Redis(host='localhost', port=6379, decode_responses=True)

        # # threading lock
        # self._value_lock = threading.Lock()

        self.logger.info('从redis同步参数')

        self.open_price_sell = self.redis_cli.hget(self.setting_ht, 'open_price_sell')
        self.open_price_buy = self.redis_cli.hget(self.setting_ht, 'open_price_buy')
        self.price_dist = self.redis_cli.hget(self.setting_ht, 'price_dist')
        self.profit_dist = self.redis_cli.hget(self.setting_ht, 'profit_dist')
        self.init_position = self.redis_cli.hget(self.setting_ht, 'init_position')
        self.final_position = self.redis_cli.hget(self.setting_ht, 'final_position')
        self.unit_amount = self.redis_cli.hget(self.setting_ht, 'unit_amount')
        self.unfilled_buy_list = 'buy_list_name'
        self.unfilled_sell_list = 'sell_list_name'

        self.logger.info('同步委托列表')
        self.redis_cli.ltrim(self.unfilled_buy_list, 1, 0)
        self.redis_cli.ltrim(self.unfilled_sell_list, 1, 0)
        for o in self.get_unfilled_orders({'orderQty': self.unit_amount, 'ordStatus': 'New'}):
            redis_item = {'orderID': o['orderID'],
                          'side': o['side'],
                          'price': o['price'],
                          'orderQty': o['orderQty']
                          }
            if o['side'] == 'Buy':
                self.redis_insert_buy(self.unfilled_buy_list, redis_item)
            else:
                self.redis_insert_sell(self.unfilled_sell_list, redis_item)
        self.logger.info('同步完毕')

    def get_filled_order(self):
        filled_orders = []
        for order in self.ws.open_orders():
            if order['ordStatus'] == 'Filled' and not self.redis_cli.sismember(self.filled_order_set, order['orderID']):
                filled_orders.append(order)
        return filled_orders

    def get_unfilled_orders(self, filter=None):
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
                    result.append(o)
                break
            times += 1
        return result

    def close_order(self, symbol, side, price):
        try:
            order = self.cli.Order.Order_new(symbol=symbol, side=side, price=price, execInst='Close').result()
        except Exception as e:
            if 'insufficient Available Balance' in str(e):
                self.logger.info('余额不足，委托取消 %s' % e)
            elif '403 Forbidden' in str(e):
                self.logger.info('403错误，委托取消 %s' % e)
            self.logger.error('订单error: %s,1秒后重试' % e)
            time.sleep(1)
        else:
            result = order[0]
            self.logger.info(
                '委托成功: side: %s, price: %s, orderid: %s' % (result['side'], result['price'], result['orderID']))

    def send_market_order(self, symbol, side, qty):
        times = 0
        result = None
        while times < 500:
            self.logger.info('第%s次发起市价委托 side: %s' % (times + 1, side))
            try:
                order = self.cli.Order.Order_new(symbol=symbol, side=side, orderQty=qty,
                                                 ordType='Market').result()
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
                self.logger.info(
                    '委托成功: side: %s, price: %s, orderid: %s' % (result['side'], result['price'], result['orderID']))
                break
            times += 1
        return result

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
                return self.redis_cli.lrem(name, 1, redis_order)
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

    def cancel_all(self, symbol=None, filter=None):
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
        while True:
            filled_orders = self.get_filled_order()
            for filled_order in filled_orders:
                order_id = filled_order['orderID']
                cum_qty = filled_order['cumQty']
                order_px = filled_order['price']
                side = filled_order['side']
                ord_type = filled_order['ordType']
                symbol = filled_order['symbol']
                self.logger.info('--------------------------------------------------------------------------------')
                self.logger.info('side: %s, type: %s, symbol: %s, cum_qty: %s, order_px: %s, orderID: %s' %
                                 (side, ord_type, symbol, cum_qty, order_px, order_id))

                # init_position最好是素数
                if cum_qty % self.init_position == 0:
                    self.logger.info('开仓...')
                    if side == 'Sell':
                        self.redis_cli.hset(self.setting_ht, 'open_price_sell', order_px)
                    else:
                        self.redis_cli.hset(self.setting_ht, 'open_price_buy', order_px)

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
                        # 上涨止损
                        if self.redis_cli.llen(self.unfilled_sell_list) == 0:
                            #qty = self.redis_cli.llen(self.unfilled_buy_list) * self.unit_amount / 2
                            self.cancel_all()
                            self.close_order(self.contract_names[0], 'Buy', price + 500)
                            #self.send_market_order(symbol, 'Buy', qty)
                    else:
                        self.redis_rem(self.unfilled_buy_list, order_id)

                        price = order_px + self.profit_dist
                        self.send_order(symbol, 'Sell', self.unit_amount, price)
                        buy_amount += 1

                        # 下跌止损
                        if self.redis_cli.llen(self.unfilled_buy_list) == 0:
                            #qty = self.redis_cli.llen(self.unfilled_sell_list) * self.unit_amount / 2
                            self.cancel_all()
                            self.close_order(self.contract_names[1], 'Sell', price - 500)
                            #self.send_market_order(symbol, 'Sell', qty)

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
