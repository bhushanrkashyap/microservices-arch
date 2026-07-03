import os, sys, json, django
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "payment_service.settings")
django.setup()

from shared.config.kafka import producer_config, consumer_config
from shared.config.logging import logger

DLQ_TOPIC = "payment.dlq"

producer = Producer(producer_config())
consumer = Consumer(consumer_config("payment_service_group"))
consumer.subscribe(["inventory_reserved"])

logger.info("Payment consumer started")


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
        data = json.loads(msg.value().decode("utf-8"))
        if data.get("status") == "Inventory Reserved":
            logger.info("processing payment for order %s", data.get("order_id"))
            event = {
                "order_id": data.get("order_id"),
                "product_name": data.get("product_name"),
                "quantity": data.get("quantity"),
                "price": data.get("price"),
                "status": "Payment Successful",
            }
            logger.info("payment successful for order %s", data.get("order_id"))
            producer.produce(topic="payment_success", value=json.dumps(event).encode("utf-8"))
            producer.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
            producer.flush()

    except KeyboardInterrupt:
        break
    except Exception as e:
        send_to_dlq(msg, e)

consumer.close()
