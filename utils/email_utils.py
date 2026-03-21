import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from dotenv import load_dotenv

# Asegurar que las variables de entorno están cargadas
load_dotenv()

EMAIL_SENDER = os.getenv("SMTP_USER", "danisuperk@gmail.com") 
EMAIL_PASSWORD = os.getenv("SMTP_PASS", "urxvfyzsjkrfnlmw")
EMAIL_RECEIVER = "daniel.karimi@alumnos.upm.es"

def send_email(subject, body, receiver=EMAIL_RECEIVER):
    if EMAIL_SENDER == "your-email@gmail.com" or not EMAIL_PASSWORD:
        logging.warning("Email credentials not configured. Skipping email sent.")
        print("⚠️ Configura SMTP_USER y SMTP_PASS para enviar correos.")
        return False

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = receiver
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        text = msg.as_string()
        server.sendmail(EMAIL_SENDER, receiver, text)
        server.quit()
        logging.info("Email sent successfully to " + receiver)
        return True
    except Exception as e:
        logging.error(f"Failed to send email to {receiver}: {e}")
        return False
