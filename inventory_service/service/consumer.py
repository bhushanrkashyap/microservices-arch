import json, os, sys, django

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "inventory.settings")
django.setup()

from confluent_kafka import Consumer, KafkaError, Producer
from service.models import Inventory
from shared.config.kafka import producer_config, consumer_config
from shared.config.logging import logger

DLQ_TOPIC = "inventory.dlq"

producer = Producer(producer_config())
consumer = Consumer(consumer_config("inventory_service_group"))
consumer.subscribe(["order_created", "payment_success", "payment_failed"])

logger.info("Inventory consumer started")


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
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        logger.error("kafka error: %s", msg.error())
        continue

    try:
        topic = msg.topic()
        data = json.loads(msg.value().decode("utf-8"))

        if topic == "order_created":
            product_name = data.get("product_name")
            quantity = int(data.get("quantity"))
            inventory = Inventory.objects.filter(product_name=product_name).first()
            if inventory:
                if inventory.quantity >= quantity:
                    inventory.quantity -= quantity
                    inventory.save()
                    price = inventory.price * quantity
                    logger.info("inventory reserved: %s qty %s", product_name, inventory.quantity)
                    event = {
                        "order_id": data.get("order_id"),
                        "product_name": product_name,
                        "quantity": quantity,
                        "price": float(price),
                        "status": "Inventory Reserved",
                    }
                    producer.produce(topic="inventory_reserved", value=json.dumps(event).encode("utf-8"))
                else:
                    logger.info("insufficient inventory: %s", product_name)
                    event = {
                        "order_id": data.get("order_id"),
                        "product_name": product_name,
                        "quantity": quantity,
                        "status": "Inventory failed",
                    }
                    producer.produce(topic="inventory_failed", value=json.dumps(event).encode("utf-8"))
                producer.flush()
            else:
                logger.info("product not found: %s", product_name)

        elif topic == "payment_success":
            logger.info("order %s confirmed", data.get("order_id"))
            event = {
                "order_id": data.get("order_id"),
                "product_name": data.get("product_name"),
                "quantity": data.get("quantity"),
                "status": "Order Confirmed",
            }
            producer.produce(topic="order_confirmed", value=json.dumps(event).encode("utf-8"))
            producer.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
            producer.flush()

        elif topic == "payment_failed":
            product_name = data.get("product_name")
            quantity = int(data.get("quantity"))
            inventory = Inventory.objects.filter(product_name=product_name).first()
            if inventory:
                inventory.quantity += quantity
                inventory.save()
                logger.info("inventory released: %s", product_name)
                event = {
                    "order_id": data.get("order_id"),
                    "product_name": product_name,
                    "quantity": quantity,
                    "status": "Release Inventory",
                }
                producer.produce(topic="order_cancelled", value=json.dumps(event).encode("utf-8"))
                producer.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
                producer.flush()

    except KeyboardInterrupt:
        break
    except Exception as e:
        send_to_dlq(msg, e)

consumer.close()
