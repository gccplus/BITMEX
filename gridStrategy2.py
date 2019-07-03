# -*- coding: utf-8 -*-
from bitmex_websocket import BitMEXWebsocket
from bitmex_rest import bitmex
import logging
import logging.handlers
import time
import redis
import threading
import os


class GridStrategy:
    # 开仓价格
    open_price = 11000
    # 价格间距
    price_dist = 100
    # 利润间距
    profit_dist = 50

    contract_name = 'XBTZ19'
    filled_order_set = 'filled_order_set'

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
        #init redis client
        self.redis_cli = redis.Redis(host='localhost', port=6379, decode_responses=True)

        # test reids
        self.redis_cli.sadd(self.filled_order_set, 'test orderid')

        # threading lock
        self._value_lock = threading.Lock()
        self.unfilled_sell_list = ''
        self.unfilled_buy_list = ''

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
                break
            times += 1
        return result

    def run(self):
        self.logger.info('start')
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

                if side == 'Sell':
                    price = order_px - self.profit_dist
                    self.send_order(symbol, 'Buy', cum_qty, price)
                else:
                    price = order_px + self.profit_dist
                    self.send_order(symbol, 'Sell', cum_qty, price)
                self.redis_cli.sadd(self.filled_order_set, filled_order['orderID'])
            time.sleep(0.2)


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - Thread-%(threadName)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    robot = GridStrategy()
    robot.run()
