import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from dotenv import load_dotenv

load_dotenv()

EMAIL_SENDER = os.getenv("SMTP_USER", "")
EMAIL_PASSWORD = os.getenv("SMTP_PASS", "")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER", "")


def send_email(subject, body, receiver=None, to_email=None):
    to = to_email or receiver or EMAIL_RECEIVER
    if not EMAIL_SENDER or not EMAIL_PASSWORD or not to:
        logging.warning("Email credentials not configured. Skipping.")
        return False

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = to
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, to, msg.as_string())
        server.quit()
        logging.info(f"Email sent to {to}")
        return True
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        return False
