import os
import smtplib
from email.message import EmailMessage
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv

load_dotenv()

SMTP_HOST = "smtp-relay.brevo.com"
SMTP_PORT = 587
SMTP_USER = os.environ.get("USER_EMAIL_BREVO")
SMTP_PASS = os.environ.get("PASS_EMAIL_BREVO")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL_BREVO")

if not SMTP_USER or not SMTP_PASS or not SENDER_EMAIL:
    raise ValueError("As variáveis de ambiente USER_EMAIL_BREVO, PASS_EMAIL_BREVO e SENDER_EMAIL_BREVO devem estar definidas")

env = Environment(loader=FileSystemLoader('./src/resources/mail/'))

def send_severity_email(to_email: str, user_name: str, code: str):
    template = env.get_template('reset_password.html')

    html_content = template.render(userName=user_name, code=code)

    # Monta o email
    msg = EmailMessage()
    msg['Subject'] = "Risco de incêndio"
    msg['From'] = SENDER_EMAIL
    msg['To'] = to_email
    msg.set_content("Para visualizar esse email, use um cliente que suporte HTML.")  # fallback texto
    msg.add_alternative(html_content, subtype='html')

    # Envia o email via SMTP
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        print("Email de redefinição de senha enviado com sucesso para", to_email)
    except Exception as e:
        print("Erro ao enviar email:", e)
        raise