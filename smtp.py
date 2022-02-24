import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
import os


class SMTP:
    def __init__(self,
                 receiver: list,
                 attachment: list,
                 subject: str,
                 content: str,
                 sender="e-case_tracing@service.cdc.gov.tw",
                 smtp_ip="192.168.171.182",
                 smtp_port=25
                 ):
        self._receiver = receiver
        self._attachment = attachment
        self._subject = subject
        self._content = content
        self._sender = sender
        self._smtp_ip = smtp_ip
        self._smtp_port = smtp_port
        self._message = self._bundle_message()

    def _bundle_message(self):
        message = MIMEMultipart()
        message["FROM"] = Header(self._sender)
        message["To"] = Header(";".join(self._receiver))
        message["Subject"] = self._subject
        message.attach(MIMEText(self._content, "plain", "utf-8"))
        for attachment in self._attachment:
            att = MIMEText(open(attachment, 'rb').read(), 'base64', 'utf-8')
            att['Content-Type'] = 'application/octet-stream'
            att.add_header("Content-Disposition", "attachment", filename=("utf-8", "", os.path.basename(attachment)))
            message.attach(att)
        return message

    def send(self):
        smtp_obj = smtplib.SMTP(
            host=self._smtp_ip,
            port=self._smtp_port
        )

        smtp_obj.sendmail(self._sender, self._receiver, self._message.as_string())


if __name__ == '__main__':
    # receiver = 收件者 list
    main_receiver = ["yudihsu@cdc.gov.tw"]
    # subject = 主旨
    main_subject = "今晚吃pizza大餐"

    # content = 內文
    main_content = "好吃好吃，詳細菜單如附件。"

    # attachment = 附件的路徑 list

    main_attachment = ["/home/bearman/PycharmProjects/Acute group/20200222-入境國人使用居家檢疫書電子化表單統計.xlsx"]

    # sender 寄件者的顯示名稱 可以自己改但一定要@service.cdc.gov.tw結尾
    # smtp_port 跟 smtp_ip 不可以動

    # 建構寄件實例
    x = SMTP(receiver=main_receiver, subject=main_subject, content=main_content, attachment=main_attachment)

    # 送出
    x.send()
