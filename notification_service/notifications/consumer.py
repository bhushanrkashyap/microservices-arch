import os, sys, json, django

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "notification_service.settings")
django.setup()

from confluent_kafka import Consumer, Producer
from notifications.models import Notification
from notifications.send_email import send_email, retry

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "notification_service_group_new_v2",
    "auto.offset.reset": "earliest"
})

dlq_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})

consumer.subscribe(["notifications"])

print("Notification consumer started")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Kafka Error:", msg.error())
        continue

    try:
        data = json.loads(msg.value().decode("utf-8"))

        event = data.get("event", "Unknown")
        payload = data.get("data", {})

        username = payload.get("username", "User")
        email = payload.get("email")

        if not email:
            print("No email found, skipping")
            continue

        if event in ["USER_LOGIN", "USER_REGISTERED"]:
            success = retry(send_email, email)
            message = f"{username} - {event}"
        else:
            success = True
            message = f"Event received: {event}"

        if success:
            Notification.objects.create(
                user_name=username,
                user_email=email,
                event=event,
                status="Sent"
            )
            print(f" Notification saved: {message}")

        else:
            print(" Sending to DLQ")

            dlq_producer.produce(
                "notifications-dlq",
                json.dumps({
                    "event": event,
                    "data": payload
                }).encode("utf-8")
            )
            dlq_producer.flush()

    except Exception as e:
        print("Processing error:", e)