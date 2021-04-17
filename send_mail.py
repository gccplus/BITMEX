from email.mime.text import MIMEText
from email.header import Header
import smtplib

message ='''
故障故障，快检查下。。
'''

msg = MIMEText(message,'plain','utf-8')

msg['Subject'] = Header("来之服务器的报警",'utf-8')
#msg['From'] = Header('')
#msg['To'] = Header('','utf-8')

from_addr = 'outsource-recruition@chinatelecom.cn' #发件邮箱
password = 'mail@2021'     #邮箱密码(或者客户端授权码)
to_addr = 'shengkang.js@chinatelecom.cn' #收件邮箱

smtp_server = 'smtp.chinatelecom.cn' #企业邮箱地址，若是个人邮箱地址为：smtp.163.com


try:
    server = smtplib.SMTP_SSL(smtp_server,465) #第二个参数为默认端口为25，这里使用ssl，端口为994
    print('开始登录')
    server.login(from_addr,password) #登录邮箱
    print('登录成功')
    print("邮件开始发送")
    server.sendmail(from_addr,to_addr,msg.as_string())  #将msg转化成string发出
    server.quit()
    print("邮件发送成功")
except smtplib.SMTPException as e:
    print("邮件发送失败",e)