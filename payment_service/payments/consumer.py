import sys, django, os
from confluent_kafka import Consumer , Producer
import json
import time
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "payment_service.settings")
django.setup()

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "payment_service_group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
producer = Producer(conf)
consumer.subscribe(["inventory_reserved"])

print("Payment consumer started")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Kafka error:", msg.error())
        continue

    data = json.loads(msg.value().decode("utf-8"))

    print("Received:", data)

    if data.get("status") == "Inventory Reserved":
        price = data.get("price")
        print("order amount" , price)
        print("Yes or No")
        user_input = input().strip().lower()
        if user_input == "yes":
            payment_success = True
            event = {
                "order_id": data.get("order_id"),
                "product_name": data.get("product_name"),
                "quantity": data.get("quantity"),
                "price": price,
                "status": "Payment Successful"
            }
            print("Payment Successful")
            producer.produce(topic = "payment_success" , value = json.dumps(event).encode("utf-8"))
        else:
            payment_success = False
            event = {
                "order_id": data.get("order_id"),
                "product_name": data.get("product_name"),
                "quantity": data.get("quantity"),
                "status": "Payment Failed"
            }
            print("Payment Failed")
            producer.produce(topic = "payment_failed" , value = json.dumps(event).encode("utf-8"))
        producer.flush()

consumer.close()