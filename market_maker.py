# -*- coding: utf-8 -*-
from bitmex_websocket import BitMEXWebsocket
from bitmex_rest import bitmex
import logging
import logging.handlers
import time
import redis
import json

# 做市策略
class MarketMaker:
    contract_names = ['XBTUSD']
    if_test = True
    def __init__(self):
        self.logger = setup_logger()
        # init redis client
       
        test_url = 'https://testnet.bitmex.com/api/v1'
        product_url = 'https://www.bitmex.com/api/v1'
        print(self.if_test)
        if self.if_test:
            print('TEST.')
            url = test_url
            self.api_key = 'YaZ6c81UNsKVCW2eh87a7OeL'
            self.api_secret = '4lursf1Lk5DBrl7M28hJTBsxMiVeBIhnNyciL_glYQDPCJdy'
        else:
            url = product_url

        self.cli = bitmex(test=self.if_test, api_key=self.api_key, api_secret=self.api_secret)
        self.ws = BitMEXWebsocket(endpoint=url,
                                  symbols=self.contract_names,
                                  api_key=self.api_key,
                                  api_secret=self.api_secret)

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
                orders = self.cli.Order.Order_getOrders(reverse=True, count=500, filter=json.dumps(filter)).result()
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

    def run(self):
        self.logger.info('start')
        buy_amount = 0
        sell_amount = 0
        last_buy = []
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
                if cum_qty == self.open_amount:
                    self.logger.info('开仓...')
                    if side == 'Sell':
                        self.redis_cli.hset(self.setting_ht, 'open_price_sell', order_px)
                        self.redis_cli.ltrim(self.unfilled_sell_list, 1, 0)
                    else:
                        self.redis_cli.hset(self.setting_ht, 'open_price_buy', order_px)
                        self.redis_cli.ltrim(self.unfilled_buy_list, 1, 0)

                    new_orders = []
                    # Sell Order
                    for i in range(self.init_sell_cnt):
                        price = order_px + self.price_dist * i + self.profit_dist
                        qty = int(price * self.unit_value)
                        new_orders.append({
                            'symbol': symbol,
                            'side': 'Sell',
                            'orderQty': qty,
                            'ordType': 'Limit',
                            'price': price
                        })
                    self.new_bulk_orders(new_orders)
                    new_orders = []
                    # Buy Order
                    for i in range(self.init_buy_cnt):
                        price = order_px - self.price_dist * (i + 1)
                        qty = int(price * self.unit_value)
                        new_orders.append({
                            'symbol': symbol,
                            'side': 'Buy',
                            'orderQty': qty,
                            'ordType': 'Limit',
                            'price': price
                        })
                    self.new_bulk_orders(new_orders)

                else:
                    if side == 'Sell':
                        self.redis_rem(self.unfilled_sell_list, order_id)
                        price = order_px - self.profit_dist
                        self.send_order(symbol, 'Buy', cum_qty, price)
                        sell_amount += 1
                        if last_buy:
                            self.logger.info('orderID\tSell:%s-Buy:%s', order_id, last_buy[-1])
                            last_buy.pop()
                        else:
                            self.logger.info('orderID\tSell:%s-Buy:%s', order_id, '')
                    else:
                        self.redis_rem(self.unfilled_buy_list, order_id)

                        price = order_px + self.profit_dist
                        self.send_order(symbol, 'Sell', cum_qty, price)
                        buy_amount += 1
                        last_buy.append(order_id)

                self.logger.info('TOTAL: %d\tBUY: %d\tSELL: %d' % (sell_amount + buy_amount, buy_amount, sell_amount))
                self.redis_cli.sadd(self.filled_order_set, filled_order['orderID'])
            time.sleep(0.2)


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    robot = MarketMaker()
    robot.run()
