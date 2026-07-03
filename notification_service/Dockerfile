FROM python:3.10-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY notification_service/ /app/
COPY shared/ /app/shared/

EXPOSE 8004

CMD ["sh", "-c", "python manage.py migrate && python manage.py runserver 0.0.0.0:8004"]
