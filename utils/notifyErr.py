import smtplib
from email.mime.text import MIMEText
from email.header import Header

emails = ['171250670@smail.nju.edu.cn', '171250647@smail.nju.edu.cn', '727798928@qq.com']

def notify(msg):
    for email in emails:
        from_addr = '1471915689@qq.com'  # 邮件发送账号
        to_addrs = email  # 接收邮件账号
        qqCode = 'rlusfnhselfnjgij'  # 授权码（这个要填自己获取到的）
        smtp_server = 'smtp.qq.com'  # 固定写死
        smtp_port = 465  # 固定端口

        # 配置服务器
        stmp = smtplib.SMTP_SSL(smtp_server, smtp_port)
        stmp.login(from_addr, qqCode)

        # 组装发送内容
        message = MIMEText(msg, 'plain', 'utf-8')  # 发送的内容
        message['From'] = Header("OASIS Python邮件预警系统", 'utf-8')  # 发件人
        message['To'] = Header("项目管理员", 'utf-8')  # 收件人
        subject = 'schedule task 项目运行错误'
        message['Subject'] = Header(subject, 'utf-8')  # 邮件标题

        try:
            stmp.sendmail(from_addr, to_addrs, message.as_string())
        except Exception as e:
            print('邮件发送失败--' + str(e))
        print('邮件发送成功')
