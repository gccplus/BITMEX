# -*- coding: utf-8 -*-
from bitmex_websocket import BitMEXWebsocket
from bitmex_rest import bitmex
import logging
import logging.handlers
import time
import redis
import os

# 钱包地址：3BMEXwPZU7VuPtrSxYyEYdKU6Ym8RV24CK
"""
order = {'orderID': '7262a79f-340d-9783-4a73-fe42c04669a4',
         'clOrdID': '',
         'clOrdLinkID': '',
         'account': 509254,
         'symbol': 'XBTZ18',
         'side': 'Buy',
         'simpleOrderQty': None,
         'orderQty': 1996,
         'price': 7798,
         'displayQty': None,
         'stopPx': None,
         'pegOffsetValue': None,
         'pegPriceType': '',
         'currency': 'USD',
         'settlCurrency': 'XBt',
         'ordType': 'Limit',
         'timeInForce': 'GoodTillCancel',
         'execInst': '',
         'contingencyType': '',
         'exDestination': 'XBME',
         'ordStatus': 'Filled',
         'triggered': '',
         'workingIndicator': False,
         'ordRejReason': '',
         'simpleLeavesQty': 0,
         'leavesQty': 0,
         'simpleCumQty': 0.25596704,
         'cumQty': 1996,
         'avgPx': 7798,
         'multiLegReportingType': 'SingleSecurity',
         'text': 'Submitted via API.',
         'transactTime': '2018-07-23T07:31:58.326Z',
         'timestamp': '2018-07-23T07:33:48.339Z'
         }
"""


class MyRobot:
    lever = 5
    contract_name = 'XBTZ18'
    new_position_thd = 350
    re_position_thd = 30
    price_table = {
        '8': [4, 10, 21, 43],
        '12': [6, 15, 31, 64],
        '16': [8, 20, 42, 85],
        '24': [12, 30, 63, 127],
        '32': [16, 40, 84, 170]
    }
    open_price_list = 'open_price_list'
    base_price_list = 'base_price_list'
    base_price = 'base_price'
    filled_order_set = 'filled_order_set'

    unit_amount_list = 'unit_amount_list'

    def __init__(self):
        self.logger = setup_logger()
        test = False
        api_key = os.getenv('API_KEY')
        api_secret = os.getenv('API_SECRET')
        test_url = 'https://testnet.bitmex.com/api/v1'
        product_url = 'https://www.bitmex.com/api/v1'

        self.logger.info('APIKEY: %s' % api_key)
        if test:
            url = test_url
        else:
            url = product_url
        self.cli = bitmex(test=test, api_key=api_key, api_secret=api_secret)
        self.ws = BitMEXWebsocket(endpoint=url, symbols=["XBTUSD", self.contract_name], api_key=api_key,
                                  api_secret=api_secret)

        # init redis client
        self.redis_cli = redis.Redis(host='35.230.143.112', password='jsgx1912', port=6379, decode_responses=True)

        self.last_sms_time = 0

    """
    2018/6/14 更新
    每次选取最邻近的订单
    
    2018/7/23 更新
    解决当价格变动很大，订单没有按照顺序成交，导致重复订单
    """

    def get_filled_order(self, last_trans_qty=0, last_trans_side='', last_trans_type=''):
        recent_order = None
        for order in self.ws.open_orders():
            if order['ordStatus'] == 'Filled' and (
                    not self.redis_cli.sismember('filled_order_set', order['orderID'])):
                # 目前仅考虑买单
                if last_trans_side == 'Sell' and order['ordType'] == 'Limit':
                    if order['side'] == 'Buy' and last_trans_type == 'Limit' and order['cumQty'] != last_trans_qty:
                        continue
                elif last_trans_side == 'Buy' and order['ordType'] == 'Limit':
                    if order['side'] == 'Buy' and last_trans_type == 'Limit' and order['cumQty'] != last_trans_qty * 2:
                        continue
                if not recent_order:
                    recent_order = order
                else:
                    if order['timestamp'] > recent_order['timestamp']:
                        recent_order = order
        return recent_order

    # 2018/7/26 添加
    def get_unfilled_order(self):
        unfilled_orders = []
        for o in self.ws.open_orders():
            if o['ordStatus'] == 'New' or o['ordStatus'] == 'PartiallyFilled':
                unfilled_orders.append(o)
        return unfilled_orders

    """
    2018/7/23 更新
    增加异常多次请求
    
    2018/7/25
    修改过滤条件, 增加PartiallyFilled
    
    2018/7/26
    重写函数，并命名为get_unfilled_order
    """

    def get_delegated_orders(self):
        times = 0
        result = []
        while times < 200:
            self.logger.info('第%s次获取未成交委托' % (times + 1))
            try:
                orders = self.cli.Order.Order_getOrders(reverse=True).result()
            except Exception as e:
                self.logger.error('get orders error: %s' % e)
                time.sleep(1)
            else:
                for o in orders[0]:
                    if o['ordStatus'] == 'New' or o['ordStatus'] == 'PartiallyFilled':
                        result.append(o)
                break
            times += 1

        return result

    def get_ticker(self, symbol):
        # tickers = self.ws.get_ticker()
        while True:
            tickers = self.ws.get_ticker()
            if len(tickers) > 0:
                for ticker in tickers[::-1]:
                    if ticker['symbol'] == symbol:
                        return ticker
            time.sleep(0.5)

    def send_order(self, symbol, side, qty, price, ordtype='Limit'):
        times = 0
        result = 0
        for o in self.get_delegated_orders():
            if o['side'] == side and o['price'] == price:
                self.logger.info('委托已存在, orderid: %s' % o['orderID'])
                return 1
        while times < 500:
            self.logger.info('第%s次发起订单委托' % (times + 1))
            if ordtype == 'Limit':
                try:
                    order = self.cli.Order.Order_new(symbol=symbol, side=side, orderQty=qty, price=price,
                                                     ordType=ordtype).result()
                except Exception as e:
                    self.logger.error('订单error: %s,1秒后重试' % e)
                    time.sleep(1)
                else:
                    self.logger.info('委托成功')
                    result = order[0]['orderID']
                    break
            else:
                try:
                    order = self.cli.Order.Order_new(symbol=symbol, side=side, orderQty=qty, ordType=ordtype).result()
                except Exception as e:
                    self.logger.error('订单error: %s,1秒后重试' % e)
                    time.sleep(1)
                else:
                    result = order[0]['orderID']
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

    def amend_order(self, orderid, qty):
        times = 0
        while times < 500:
            self.logger.info('第%s次修改订单信息, orderID: %s' % (times + 1, orderid))
            try:
                self.cli.Order.Order_amend(orderID=orderid, orderQty=qty).result()
            except Exception as e:
                logging.error('修改订单错误: %s' % e)
                time.sleep(1)
            else:
                self.logger.info('修改成功')
                break
            times += 1

    def sms_notify(self, msg):
        if int(time.time() - self.last_sms_time > 900):
            self.logger.info('短信通知: %s' % msg)
            url = 'http://221.228.17.88:8080/sendmsg/send'
            params = {
                'phonenum': '18118999630',
                'msg': msg
            }
            # requests.get(url, params=params)
            self.last_sms_time = int(time.time())

    """
    2018/6/28
    修改使支持len>3的情形
    """

    def get_current_index(self, price):
        key = 'open_price_list'
        index = 0
        for i in range(10):
            if i < self.redis_cli.llen(key) and price < float(self.redis_cli.lindex(key, i)):
                index = i
        return index

    def run(self):
        self.logger.info('start')
        last_trans_qty = 0
        last_trans_side = ''
        last_trans_type = ''
        last_unit_amount = 0
        while True:
            filled_order = self.get_filled_order(last_trans_qty, last_trans_side, last_trans_type)
            if filled_order:
                cum_qty = filled_order['cumQty']
                order_px = filled_order['price']
                avg_px = adjust_price(filled_order['avgPx'])
                side = filled_order['side']
                ord_type = filled_order['ordType']
                self.logger.info('--------------------------------------------------------------------------------')
                self.logger.info('side: %s, type: %s, cum_qty: %s, order_px: %s, avg_px: %s, orderID: %s' %
                                 (side, ord_type, cum_qty, order_px, avg_px, filled_order['orderID']))
                index = self.get_current_index(order_px)
                price_base = 8
                if self.redis_cli.llen('open_price_list') > 0:
                    price_base = int(self.redis_cli.lindex('base_price_list', index))
                    self.logger.info('index: %s, price_base: %s' % (index, price_base))
                price_table = self.price_table[str(price_base)]

                if ord_type == 'Market':
                    if side == 'Buy':
                        self.logger.info('建仓  仓位: %s, 价格: %a' % (cum_qty, avg_px))
                        self.logger.info('rpush')
                        self.redis_cli.rpush('open_price_list', avg_px)
                        self.redis_cli.rpush('unit_amount_list', cum_qty)
                        if self.redis_cli.get('base_price'):
                            self.redis_cli.rpush('base_price_list', self.redis_cli.get('base_price'))
                        else:
                            self.logger.info('base_price未赋值')
                            time.sleep(300)
                        price_base = float(self.redis_cli.get('base_price'))
                        self.logger.info('买入: %s,价格: %s' % (cum_qty, avg_px - price_base))
                        orderid = self.send_order(self.contract_name, 'Buy', cum_qty, avg_px - price_base)
                        if orderid == 0:
                            self.logger.info('委托失败，程序终止')
                            break
                    else:
                        self.logger.info('平仓  仓位: %s, 价格: %s' % (cum_qty, avg_px))
                        self.logger.info('市价开启新的仓位  仓位: %s' % last_unit_amount)
                        orderid = self.send_order(self.contract_name, 'Buy', last_unit_amount, 0, 'Market')
                        if orderid == 0:
                            self.logger.info('委托失败，程序终止')
                            break

                else:
                    if side == 'Buy':
                        if cum_qty % 16 == 0:
                            self.logger.info('卖出: %s,价格: %s' % (cum_qty * 2, order_px + price_table[3]))
                            orderid = self.send_order(self.contract_name, 'Sell', cum_qty * 2,
                                                      order_px + price_table[3])
                            if orderid == 0:
                                self.logger.info('委托失败，程序终止')
                                break
                        elif cum_qty % 2 == 0:
                            if cum_qty % 8 == 0:
                                price_buy = order_px - price_base * 8
                                price_sell = order_px + price_table[2]
                            elif cum_qty % 4 == 0:
                                price_buy = order_px - price_base * 4
                                price_sell = order_px + price_table[1]
                            else:
                                price_buy = order_px - price_base * 2
                                price_sell = order_px + price_table[0]
                            self.logger.info('卖出: %s,价格: %s' % (cum_qty, price_sell))
                            orderid = self.send_order(self.contract_name, 'Sell', cum_qty, price_sell)
                            if orderid == 0:
                                self.logger.info('委托失败，程序终止')
                                break

                            self.logger.info('买入: %s,价格: %s' % (cum_qty * 2, price_buy))
                            orderid = self.send_order(self.contract_name, 'Buy', cum_qty * 2, price_buy)
                            if orderid == 0:
                                self.logger.info('委托失败，程序终止')
                                break
                    else:
                        if cum_qty % 32 == 0:
                            self.logger.info('撤销多余Buy委托')
                            open_price = float(self.redis_cli.lindex('open_price_list', index))
                            unfilled_orders = self.get_delegated_orders()
                            if open_price < order_px:
                                self.logger.info('open price: %s' % open_price)
                                for o in unfilled_orders:
                                    if o['side'] == 'Buy' and o['price'] < open_price:
                                        self.logger.info(
                                            'cancel order orderID: %s, price: %s' % (o['orderID'], o['price']))
                                        self.cancel_order(o['orderID'])
                                self.logger.info('rpop')
                                self.redis_cli.rpop('open_price_list')
                                self.redis_cli.rpop('base_price_list')
                                self.redis_cli.rpop('unit_amount_list')

                                open_price = float(self.redis_cli.lindex('open_price_list', index))
                            else:
                                self.logger.info('没有多余的Buy委托')

                            self.logger.info('撤销多余Sell委托')
                            self.logger.info('open price: %s' % open_price)
                            for o in unfilled_orders:
                                if o['side'] == 'Sell' and order_px < o['price'] < open_price:
                                    self.logger.info(
                                        'cancel order orderID: %s, price: %s' % (o['orderID'], o['price']))
                                    self.cancel_order(o['orderID'])
                            #
                            self.logger.info('rpop')
                            self.redis_cli.rpop('open_price_list')
                            self.redis_cli.rpop('base_price_list')
                            self.redis_cli.rpop('unit_amount_list')
                            self.logger.info('已全部平仓，待重建仓位')
                        elif cum_qty % 2 == 0:
                            if cum_qty % 16 == 0:
                                buy_price = order_px - price_table[3]
                            elif cum_qty % 8 == 0:
                                buy_price = order_px - price_table[2]
                            elif cum_qty % 4 == 0:
                                buy_price = order_px - price_table[1]
                            else:
                                buy_price = order_px - price_table[0]
                            self.logger.info('买入: %s,价格: %s' % (cum_qty, buy_price))
                            orderid = self.send_order(self.contract_name, 'Buy', cum_qty, buy_price)
                            if orderid == 0:
                                self.logger.info('委托失败，程序终止')
                                break
                #
                self.redis_cli.sadd('filled_order_set', filled_order['orderID'])
                last_trans_side = side
                last_trans_qty = cum_qty
                last_trans_type = ord_type

            if self.redis_cli.llen('open_price_list') > 0:
                ticker = self.get_ticker(self.contract_name)
                bid_price = ticker['bidPrice']
                last_open_price = float(self.redis_cli.lindex('open_price_list', -1))
                unit_amount = int(self.redis_cli.lindex('unit_amount_list', -1))

                if bid_price - last_open_price < -1 * self.new_position_thd:
                    self.sms_notify(
                        '开启新的仓位 bid_price: %s, last_open_price: %s' % (
                            bid_price, self.redis_cli.lindex('open_price_list', -1)))
                #
                if bid_price - last_open_price > self.re_position_thd:
                    min_sell_price = 100000
                    for o in self.get_delegated_orders():
                        if o['side'] == 'Sell' and o['price'] < min_sell_price:
                            min_sell_price = o['price']
                    if min_sell_price - bid_price > 100:
                        self.logger.info('rpop')
                        open_price = float(self.redis_cli.rpop('open_price_list'))
                        self.redis_cli.rpop('base_price_list')
                        self.redis_cli.rpop('unit_amount_list')
                        self.logger.info('撤销多余委托')
                        # PartiallyFilled 需要特别处理
                        unfilled_amount = 0
                        for o in self.get_delegated_orders():
                            if o['price'] < open_price and o['side'] == 'Buy':
                                if o['ordStatus'] == 'PartiallyFilled':
                                    self.logger.info('已部分成交: %s' % o['cumQty'])
                                    unfilled_amount += o['cumQty']

                                self.logger.info(
                                    'cancel order, orderID: %s, price: %s' % (o['orderID'], o['price']))
                                self.cancel_order(o['orderID'])
                        # self.sms_notify('重建仓位, bid_price: %s, last_open_price: %s' % (
                        #     bid_price, self.redis_cli.lindex('open_price_list', -1)))
                        orderid = self.send_order(self.contract_name, 'Sell', unit_amount + unfilled_amount, 0,
                                                  'Market')
                        if orderid == 0:
                            self.logger.info('委托失败，程序终止')
                            break
                        last_unit_amount = unit_amount
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
    logger.setLevel(logging.DEBUG)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger


if __name__ == "__main__":
    robot = MyRobot()
    robot.run()