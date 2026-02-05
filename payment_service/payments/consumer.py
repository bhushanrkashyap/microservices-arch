import os, sys, json, django
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "payment_service.settings")
django.setup()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer_conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
consumer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "payment_service_group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)
consumer.subscribe(["inventory_reserved"])

print("Payment consumer started")

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        data = json.loads(msg.value().decode("utf-8"))

        if data.get("status") == "Inventory Reserved":
            print(f"Processing payment for order {data.get('order_id')}")
            event = {
                "order_id": data.get("order_id"),
                "product_name": data.get("product_name"),
                "quantity": data.get("quantity"),
                "price": data.get("price"),
                "status": "Payment Successful"
            }
            print(f"Payment successful for order {data.get('order_id')}")
            producer.produce(topic="payment_success", value=json.dumps(event).encode("utf-8"))
            producer.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
            producer.flush()

    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")

consumer.close()