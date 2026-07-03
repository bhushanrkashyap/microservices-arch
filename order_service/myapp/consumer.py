import os, sys, json, django
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "order.settings")
django.setup()

from django.core.cache import cache
from myapp.models import Order
from shared.config.kafka import producer_config, consumer_config
from shared.config.logging import logger

DLQ_TOPIC = "orders.dlq"

producer = Producer(producer_config())
consumer = Consumer(consumer_config("order_service_group"))
consumer.subscribe(["order-cancelled", "order-confirmed"])

logger.info("Order consumer started")


def send_to_dlq(msg, error):
    logger.error("DLQ route -> %s | source_topic=%s payload=%s error=%s", DLQ_TOPIC, msg.topic(), msg.value(), error)
    try:
        producer.produce(topic=DLQ_TOPIC, value=msg.value())
        producer.flush()
    except Exception as e:
        logger.error("failed to publish to DLQ: %s", e)


while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        logger.error("kafka error: %s", msg.error())
        continue

    try:
        topic = msg.topic()
        data = json.loads(msg.value().decode("utf-8"))
        payload = data.get("data", {})
        order_id = payload.get("order_id")
        user_id = payload.get("user_id")

        if topic == "order-cancelled":
            Order.objects.filter(order_id=order_id).update(status="FAILED")
            status, event_name = "FAILED", "ORDER_CANCELLED"
        else:  # order-confirmed
            Order.objects.filter(order_id=order_id).update(status="CONFIRMED")
            status, event_name = "CONFIRMED", "ORDER_CONFIRMED"

        cache.set(f"order_{order_id}", status, timeout=3600)
        logger.info("order %s -> %s", order_id, status)

        event = {"event": event_name, "data": {"order_id": order_id, "user_id": user_id}}
        producer.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
        producer.flush()

    except KeyboardInterrupt:
        break
    except Exception as e:
        send_to_dlq(msg, e)

consumer.close()
