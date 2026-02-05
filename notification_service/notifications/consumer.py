import os, sys, json, django

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "notification_service.settings")
django.setup()

from confluent_kafka import Consumer, KafkaError
from notifications.models import Notification

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "notification_service_group",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe(["notifications", "order_confirmed", "order_cancelled", "payment_success", "payment_failed", "user_login", "user-registration"])

print("Notification consumer started")

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print("Error:", msg.error())
            continue

        topic = msg.topic()
        data = json.loads(msg.value().decode("utf-8"))
        status = data.get("status", "Unknown")

        if status == "Order Confirmed":
            message = f"Order {data.get('order_id')} confirmed for {data.get('product_name')}"
        elif status == "Release Inventory":
            message = f"Order {data.get('order_id')} cancelled"
        elif status == "Payment Successful":
            message = f"Payment successful for order {data.get('order_id')}"
        elif status == "Payment Failed":
            message = f"Payment failed for order {data.get('order_id')}"
        elif topic == "user_login":
            message = f"User {data.get('username')} logged in"
        elif status == "user-registered":
            message = f"User {data.get('username')} registered"
        elif status == "Order Placed":
            message = f"Order {data.get('order_id')} placed for {data.get('product_name')}"
        else:
            message = f"Order {data.get('order_id', 'N/A')} - {status}"

        Notification.objects.create(
            order_id=data.get("order_id"),
            user_id=data.get("user_id"),
            message=message,
            status=status
        )
        print(f"Notification saved: {message}")

    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")

consumer.close()