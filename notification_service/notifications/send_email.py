import time
from django.core.mail import send_mail
from django.conf import settings

def send_email(user_email):
    subject = ""
    message = ""


    send_mail(
        subject,
        message,
        settings.EMAIL_HOST_USER,
        [user_email],
        fail_silently=False
    )

def retry(func, user_email, retries=3):
    for attempt in range(retries):
        try:
            func(user_email)
            print(" Email sent successfully")
            return True

        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")

    return False