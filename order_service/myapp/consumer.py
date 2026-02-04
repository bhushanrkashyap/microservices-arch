import os
import sys
import django
import json
from confluent_kafka import Consumer

# Setup Django
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) + "/../../"
sys.path.append(BASE_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")
django.setup()

# Kafka configuration
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order_service_group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)

consumer.subscribe(["order_cancelled", "order_confirmed"])

print("Order Service Consumer started - listening for order_cancelled and order_confirmed")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Kafka Error:", msg.error())
        continue
    topic = msg.topic()

    data = json.loads(msg.value().decode("utf-8"))

    if topic == "order_cancelled":
        print(f"Order Cancelled Event Received for Order ID: {data.get('order_id')}")

    elif topic == "order_confirmed":
        print(f"Order Confirmed Event Received for Order ID: {data.get('order_id')}")
