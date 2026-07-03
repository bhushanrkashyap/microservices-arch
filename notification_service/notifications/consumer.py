import os, sys, json, django

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "notification_service.settings")
django.setup()

from confluent_kafka import Consumer, Producer
from notifications.models import Notification
from notifications.send_email import send_email, retry
from shared.config.kafka import producer_config, consumer_config
from shared.config.logging import logger

DLQ_TOPIC = "notifications.dlq"

consumer = Consumer(consumer_config("notification_service_group_new_v2"))
dlq_producer = Producer(producer_config())
consumer.subscribe(["notifications"])

logger.info("Notification consumer started")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        logger.error("kafka error: %s", msg.error())
        continue

    try:
        data = json.loads(msg.value().decode("utf-8"))
        event = data.get("event", "Unknown")
        payload = data.get("data", {})
        username = payload.get("username", "User")
        email = payload.get("email")

        if not email:
            logger.info("no email found, skipping event %s", event)
            continue

        if event in ["USER_LOGIN", "USER_REGISTERED"]:
            success = retry(send_email, email)
            message = f"{username} - {event}"
        else:
            success = True
            message = f"Event received: {event}"

        if success:
            Notification.objects.create(
                user_name=username, user_email=email, event=event, status="Sent"
            )
            logger.info("notification saved: %s", message)
        else:
            logger.error("notification failed, routing to DLQ: %s", message)
            dlq_producer.produce(
                DLQ_TOPIC,
                json.dumps({"event": event, "data": payload}).encode("utf-8"),
            )
            dlq_producer.flush()

    except Exception as e:
        logger.error("DLQ route -> %s | source_topic=%s payload=%s error=%s", DLQ_TOPIC, msg.topic(), msg.value(), e)
        try:
            dlq_producer.produce(DLQ_TOPIC, msg.value())
            dlq_producer.flush()
        except Exception as ex:
            logger.error("failed to publish to DLQ: %s", ex)
