import os, sys, json, django
from confluent_kafka import Consumer, Producer
from django.core.cache import cache

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "order.settings")
django.setup()

from myapp.models import Order

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "order_service_group",
    "auto.offset.reset": "earliest"
}

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
consumer = Consumer(conf)
consumer.subscribe(["order-cancelled", "order-confirmed"])

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
        
        payload = data.get("data", {})
        order_id = payload.get("order_id")
        user_id = payload.get("user_id")

        if topic == "order-cancelled":
            print(f"Order {order_id} cancelled")
            
            Order.objects.filter(order_id=order_id).update(status="FAILED")
            
            event = {
                "event": "ORDER_CANCELLED",
                "data": {
                    "order_id": order_id,
                    "user_id": user_id
                }
            }
            cache.set(f"order_{order_id}", "FAILED", timeout=3600)
            print("CACHE SET:", f"order_{order_id}", cache.get(f"order_{order_id}"))
            
            producer.produce(
                topic="notifications",
                value=json.dumps(event).encode("utf-8")
            )
            producer.flush()
            
        elif topic == "order-confirmed":
            print(f"Order {order_id} confirmed")
            
            Order.objects.filter(order_id=order_id).update(status="CONFIRMED")
            
            event = {
                "event": "ORDER_CONFIRMED",
                "data": {
                    "order_id": order_id,
                    "user_id": user_id
                }
            }
            
            cache.set(f"order_{order_id}", "CONFIRMED", timeout=3600)
            print("CACHE SET:", f"order_{order_id}", cache.get(f"order_{order_id}"))
            
            producer.produce(
                topic="notifications",
                value=json.dumps(event).encode("utf-8")
            )
            producer.flush()

    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")

consumer.close()