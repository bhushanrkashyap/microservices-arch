import os
from datetime import timedelta

JWT_SECRET = os.environ.get(
    "JWT_SECRET",
    "django-insecure-yi-hon&3h3!k9h+g34j%0xqqxr_%r-t&6j5)a0_am5dm(*261g",
)
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")

SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(minutes=30),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=1),
    "ROTATE_REFRESH_TOKENS": False,
}import os
from datetime import timedelta

JWT_SECRET = os.environ.get(
    "JWT_SECRET",
    "django-insecure-yi-hon&3h3!k9h+g34j%0xqqxr_%r-t&6j5)a0_am5dm(*261g",
)
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")

SIMPLE_JWT = {
    "ACCESS_TOKEN_LIFETIME": timedelta(minutes=30),
    "REFRESH_TOKEN_LIFETIME": timedelta(days=1),
    "ROTATE_REFRESH_TOKENS": False,
}
