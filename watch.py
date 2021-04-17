from bitmex_rest import bitmex
import logging
import logging.handlers
import time

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Change this to DEBUG if you want a lot more info
    ch = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - Thread-%(threadName)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def send_mail():
    import smtplib
    from smtplib import SMTP
    from email.mime.text import MIMEText
    from email.header import Header
    
    #构造纯文本邮件内容
    msg = MIMEText('hello,send by Python.....','plain','utf-8')
    
    #发送者邮箱
    sender = 'outsource-recruition@chinatelecom.cn'
    
    #发送者的登陆用户名和密码
    user = 'outsource-recruition@chinatelecom.cn'
    password = 'mail@2021'
    
    #发送者邮箱的SMTP服务器地址
    smtpserver = 'smtp.chinatelecom.cn'
    
    #接收者的邮箱地址
    receiver = ['shengkang.js@chinatelecom.cn'] #receiver 可以是一个list
    
    smtp = smtplib.SMTP() #实例化SMTP对象
    smtp.connect(smtpserver,25) #（缺省）默认端口是25 也可以根据服务器进行设定
    smtp.login(user,password) #登陆smtp服务器
    smtp.sendmail(sender,receiver,msg.as_string()) #发送邮件 ，这里有三个参数
    '''
    login()方法用来登录SMTP服务器，sendmail()方法就是发邮件，由于可以一次发给多个人，所以传入一个list，邮件正文
    是一个str，as_string()把MIMEText对象变成str。
    '''
    smtp.quit()

    
if __name__ == "__main__":
    send_mail()
    # setup_logger()
    # contract_name = 'XBTUSD'
    # test = False
    # api_key = '0DSLWKFd8iCVQHWGvBxAqqjl'
    # api_secret = 'mCtCfZfeKOuprqWS-79T6H0PDZuYhJAnVWPvq4wd3XkZ1VYQ'
    # test_url = 'https://testnet.bitmex.com/api/v1'
    # product_url = 'https://www.bitmex.com/api/v1'
    # cli = bitmex(test=test, api_key=api_key, api_secret=api_secret)
    # print(dir(cli.Execution))
    # print(dir(cli.Position))
    
    # while True:
    #     res = cli.Position.Position_get().result()
    #     for item in res[0]:
    #         symbol = item['symbol']
    #         currentQty = item['currentQty']
    #         currentCost = item['currentCost']
    #         markPrice = item['markPrice']
    #         markValue = item['markValue']
    #         avgCostPrice = item['avgCostPrice']
    #         liquidationPrice = item['liquidationPrice']
    #         print(symbol, currentQty, currentCost, avgCostPrice, markPrice, markValue, liquidationPrice)
    #     time.sleep(5)
    
    #print(res[0])
    # filter = {
    #     "execType": "Funding"
    # }
    #res = cli.Execution.Execution_getTradeHistory(reverse=True, symbol=contract_name, filter=json.dumps(filter)).result()
    #print(res)