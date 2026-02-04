import os, sys, json, django
from confluent_kafka import Consumer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "order.settings")
django.setup()

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "order_service_group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe(["order_cancelled", "order_confirmed"])

print("Order consumer started")

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue
        
        topic = msg.topic()
        data = json.loads(msg.value().decode("utf-8"))

        if topic == "order_cancelled":
            print(f"Order {data.get('order_id')} cancelled")
        elif topic == "order_confirmed":
            print(f"Order {data.get('order_id')} confirmed")

    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")

consumer.close()