# -*- coding: utf-8 -*-
from bitmex_websocket import BitMEXWebsocket
from bitmex_rest import bitmex
import logging
import logging.handlers
import time
import redis
import threading
import json


class hedgingStrategy:
    contract_1 = {
        'name': 'XBTUSD',
        'name2': 'XBTZ18',
        'side': 'Buy',
        'unfilled_buy_list': 'unfilled_buy_list_1',
        'unfilled_sell_list': 'unfilled_sell_list_1'
    }
    contract_2 = {
        'name': 'XBTZ18',
        'name2': 'XBTUSD',
        'side': 'Sell',
        'unfilled_buy_list': 'unfilled_buy_list_2',
        'unfilled_sell_list': 'unfilled_sell_list_2'
    }
    unit_amount = 25
    unit_price_dist = 1
    total_price_dist = 20
    base_position = unit_amount * total_price_dist
    profit_dist = 0.5

    filled_order_set = 'filled_order_set2'

    def __init__(self):
        self.logger = setup_logger()
        test = False
        api_key = 'vj708HQhWkv1JbTM9y_LI-Xn'
        api_secret = 'lWPOhvhY-yn-HAIo7k3mnjR7pijJJJQAKjTKtioQ_K1Wq3vf'
        test_url = 'https://testnet.bitmex.com/api/v1'
        product_url = 'https://www.bitmex.com/api/v1'
        if test:
            url = test_url
        else:
            url = product_url
        self.cli = bitmex(test=test, api_key=api_key, api_secret=api_secret)
        self.ws = BitMEXWebsocket(endpoint=url, symbols=[self.contract_1['name'], self.contract_2['name']],
                                  api_key=api_key,
                                  api_secret=api_secret)
        # init redis client
        self.redis_cli = redis.Redis(host='localhost', port=6379, decode_responses=True)

        # threading lock
        self._value_lock = threading.Lock()

    def get_filled_order(self, symbol):
        first_buy_order = None
        first_sell_order = None
        with self._value_lock:
            for order in self.ws.open_orders():
                if order['ordStatus'] == 'Filled' and order['symbol'] == symbol and (
                        not self.redis_cli.sismember('filled_order_set2', order['orderID'])):
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

    def send_order(self, contract, side, qty, price):
        times = 0
        result = None
        while times < 500:
            self.logger.info('第%s次发起委托 side: %s, price: %s' % (times + 1, side, price))
            try:
                order = self.cli.Order.Order_new(symbol=contract['name'], side=side, orderQty=qty, price=price,
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
                        return self.send_order(contract, side, qty, price - self.unit_price_dist)
                    else:
                        return self.send_order(contract, side, qty, price + self.unit_price_dist)
                self.logger.info(
                    '委托成功: side: %s, price: %s, orderid: %s' % (result['side'], result['price'], result['orderID']))
                redis_item = {'orderID': result['orderID'],
                              'side': result['side'],
                              'price': result['price'],
                              'orderQty': result['orderQty']
                              }
                if side == 'Buy':
                    self.redis_insert_buy(contract['unfilled_buy_list'], redis_item)
                else:
                    self.redis_insert_sell(contract['unfilled_sell_list'], redis_item)
                break
            times += 1
        return result

    def redis_rem(self, name, order_id):
        for i in range(self.redis_cli.llen(name)):
            redis_order = self.redis_cli.lindex(name, i)
            json_redis_order = json.loads(redis_order)
            if json_redis_order['orderID'] == order_id:
                return self.redis_cli.lrem(name, redis_order, 1)
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
            self.redis_cli.rpush(name, json.dumps(item))

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
            self.redis_cli.lpush(name, json.dumps(item))

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

    def close_position(self, symbol):
        times = 0
        result = False
        while times < 200:
            self.logger.info('第 %s 次Close Position' % (times + 1))
            try:
                result = self.cli.Order.Order_closePosition(symbol=symbol).result()
            except Exception as e:
                self.logger.error('close_position error: %s' % e)
            else:
                print(result[0])
                result = True
                break
            times += 1
        return result

    def start_strategy(self):
        self.logger.info('start')
        t1 = threading.Thread(target=self.run, name=self.contract_1['name'], args=(self.contract_1,))
        t1.start()
        t2 = threading.Thread(target=self.run, name=self.contract_2['name'], args=(self.contract_2,))
        t2.start()

    def run(self, contract):
        self.logger.info('run %s', contract['name'])
        order_amount = int(self.total_price_dist / self.unit_price_dist)
        while True:
            filled_order = self.get_filled_order(contract['name'])
            if filled_order:
                order_id = filled_order['orderID']
                cum_qty = filled_order['cumQty']
                order_px = filled_order['price']
                avg_px = adjust_price(filled_order['avgPx'])
                side = filled_order['side']
                ord_type = filled_order['ordType']
                symbol = filled_order['symbol']
                self.logger.info('--------------------------------------------------------------------------------')
                self.logger.info('side: %s, type: %s, symbol: %s, cum_qty: %s, order_px: %s, avg_px: %s, orderID: %s' %
                                 (side, ord_type, symbol, cum_qty, order_px, avg_px, order_id))

                if cum_qty == self.base_position:
                    self.logger.info('清空redis数据')
                    self.redis_cli.ltrim(contract['unfilled_sell_list'], 1, 0)
                    self.redis_cli.ltrim(contract['unfilled_buy_list'], 1, 0)

                    if side == 'Buy':
                        # Buy Order
                        price = avg_px - self.unit_price_dist
                        self.send_order(contract, 'Buy', self.unit_amount, price)
                        # Sell order
                        for i in range(order_amount):
                            price = avg_px + self.unit_price_dist * i + self.profit_dist
                            self.send_order(contract, 'Sell', self.unit_amount, price)
                    else:
                        # Buy order
                        for i in range(order_amount):
                            price = avg_px - self.unit_price_dist * i - self.profit_dist
                            self.send_order(contract, 'Buy', self.unit_amount, price)
                        # Sell Order
                        price = avg_px + self.unit_price_dist
                        self.send_order(contract, 'Sell', self.unit_amount, price)
                elif cum_qty > self.unit_amount:
                    self.logger.info('%s 已平仓，撤销其余委托' % contract['name'])
                    self.cancel_all(contract['name'])
                    self.close_position(contract['name'])

                    self.cancel_all(contract['name2'], {'side': side})
                else:
                    if contract['side'] == 'Buy':
                        if side == 'Sell':
                            self.redis_rem(contract['unfilled_sell_list'], order_id)

                            price = order_px - self.profit_dist
                            self.send_order(contract, 'Buy', self.unit_amount, price)
                            buy_order_amount = self.redis_cli.llen(contract['unfilled_buy_list'])

                            order_thd = int(0.3 * order_amount)
                            if buy_order_amount > order_thd:
                                for i in range(int(order_thd / 2)):
                                    order = json.loads(self.redis_cli.lpop(contract['unfilled_buy_list']))
                                    self.logger.info('cancel order: %s' % order['orderID'])
                                    self.cancel_order(order['orderID'])

                            sell_order_amount = self.redis_cli.llen(contract['unfilled_sell_list'])
                            if sell_order_amount == 0:
                                self.logger.info('撤销所有委托并平仓')
                                self.cancel_all(contract['name'])
                                self.cancel_all(contract['name2'])
                                self.close_position(contract['name2'])
                        else:
                            self.redis_rem(contract['unfilled_buy_list'], order_id)

                            sell_order_amount = self.redis_cli.llen(contract['unfilled_sell_list'])
                            price = order_px + self.profit_dist
                            if sell_order_amount > int(1.8 * order_amount):
                                qty = self.unit_amount * sell_order_amount
                            else:
                                qty = self.unit_amount
                            self.send_order(contract, 'Sell', qty, price)

                            buy_order_amount = self.redis_cli.llen(contract['unfilled_buy_list'])
                            if buy_order_amount == 0:
                                price = order_px - self.unit_price_dist
                                self.send_order(contract, 'Buy', self.unit_amount, price)

                    else:
                        if side == 'Sell':
                            self.redis_rem(contract['unfilled_sell_list'], order_id)
                            buy_order_amount = self.redis_cli.llen(contract['unfilled_buy_list'])
                            price = order_px - self.profit_dist
                            if buy_order_amount > int(1.8 * order_amount):
                                qty = self.unit_amount * buy_order_amount
                            else:
                                qty = self.unit_amount
                            self.send_order(contract, 'Buy', qty, price)

                            sell_order_amount = self.redis_cli.llen(contract['unfilled_sell_list'])
                            if sell_order_amount == 0:
                                price = order_px + self.unit_price_dist
                                self.send_order(contract, 'Sell', self.unit_amount, price)

                        else:
                            self.redis_rem(contract['unfilled_buy_list'], order_id)

                            price = order_px + self.profit_dist
                            self.send_order(contract, 'Sell', self.unit_amount, price)

                            sell_order_amount = self.redis_cli.llen(contract['unfilled_sell_list'])
                            order_thd = int(0.3 * order_amount)
                            if sell_order_amount > order_thd:
                                for i in range(int(order_thd / 2)):
                                    order = json.loads(self.redis_cli.rpop(contract['unfilled_sell_list']))
                                    self.logger.info('cancel order: %s' % order['orderID'])
                                    self.cancel_order(order['orderID'])

                            buy_order_amount = self.redis_cli.llen(contract['unfilled_buy_list'])
                            if buy_order_amount == 0:
                                self.logger.info('撤销所有委托并平仓')
                                self.cancel_all(contract['name'])
                                self.cancel_all(contract['name2'])
                                self.close_position(contract['name2'])

                self.redis_cli.sadd('filled_order_set2', filled_order['orderID'])
            time.sleep(0.2)


def adjust_price(price):
    """
    正常下单的价格精度是0.5，但是成交价格的精度不是0.5
    此函数用来统一下单价格精度
    """
    import re
    match = re.match(r"(\d+)\.(\d{2})", '%.2f' % price)
    if match:
        integer = int(match.group(1))
        decimal = int(match.group(2))
        if decimal < 25:
            decimal = 0
        elif decimal < 75:
            decimal = 0.5
        else:
            decimal = 1
        return integer + decimal
    else:
        return price


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
    robot.start_strategy()
