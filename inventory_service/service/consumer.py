import json, os, sys, django

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "inventory.settings")
django.setup()

from confluent_kafka import Consumer, KafkaError, Producer
from service.models import Inventory

producer_conf = {"bootstrap.servers": "localhost:9092"}
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "inventory_service_group",
    "auto.offset.reset": "earliest"
}

producer = Producer(producer_conf)
consumer = Consumer(consumer_conf)
consumer.subscribe(["order_created", "payment_success", "payment_failed"])

print("Inventory consumer started")

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
        
        if topic == "order_created":
            product_name = data.get("product_name")
            quantity = int(data.get("quantity"))
            inventory = Inventory.objects.filter(product_name=product_name).first()
            if inventory:
                if inventory.quantity >= quantity:
                    inventory.quantity -= quantity
                    inventory.save()
                    price = inventory.price * quantity
                    print(f"Inventory reserved: {product_name}, qty: {inventory.quantity}")
                    event = {
                        "order_id": data.get("order_id"),
                        "product_name": product_name,
                        "quantity": quantity,
                        "price": float(price),
                        "status": "Inventory Reserved"
                    }
                    producer.produce(topic="inventory_reserved", value=json.dumps(event).encode("utf-8"))
                else:
                    print(f"Insufficient inventory: {product_name}")
                    event = {
                        "order_id": data.get("order_id"),
                        "product_name": product_name,
                        "quantity": quantity,
                        "status": "Inventory failed"
                    }
                    producer.produce(topic="inventory_failed", value=json.dumps(event).encode("utf-8"))
                producer.flush()
            else:
                print(f"Product not found: {product_name}")
        
        elif topic == "payment_success":
            print(f"Order {data.get('order_id')} confirmed")
            event = {
                "order_id": data.get("order_id"),
                "product_name": data.get("product_name"),
                "quantity": data.get("quantity"),
                "status": "Order Confirmed"
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
                print(f"Inventory released: {product_name}")
                event = {
                    "order_id": data.get("order_id"),
                    "product_name": product_name,
                    "quantity": quantity,
                    "status": "Release Inventory"
                }
                producer.produce(topic="order_cancelled", value=json.dumps(event).encode("utf-8"))
                producer.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
                producer.flush()
                
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"Error: {e}")

consumer.close()