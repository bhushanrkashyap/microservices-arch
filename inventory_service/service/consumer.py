import json, os, sys, django

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "inventory.settings")
django.setup()

from confluent_kafka import Consumer, KafkaError , Producer
from service.models import Inventory

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "inventory_service_group",
    "auto.offset.reset": "earliest"
}
p = Producer(conf)
c = Consumer(conf)
c.subscribe(["order_created", "payment_success", "payment_failed"])

print("Inventory consumer started - listening for order_created, payment_success, payment_failed")
while True:
    try:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print("Error:", msg.error())
            continue
        
        topic = msg.topic()
        data = json.loads(msg.value().decode("utf-8"))
        
        # Handle order_created topic
        if topic == "order_created":
            product_name = data.get("product_name")
            quantity = int(data.get("quantity"))
            x = Inventory.objects.filter(product_name=product_name).first()
            if x:
                try:
                    if x.quantity >= quantity:
                        x.quantity -= quantity
                        x.save()
                        price = x.price * quantity
                        print(f"Inventory reserved for product {product_name}. New quantity: {x.quantity}")
                        event = {
                            "order_id": data.get("order_id"),
                            "product_name": product_name,
                            "quantity": quantity,
                            "price": float(price),
                            "status": "Inventory Reserved"
                        }
                        p.produce(topic="inventory_reserved", value=json.dumps(event).encode("utf-8"))
                        p.flush()
                    else:
                        print(f"Insufficient inventory for product {product_name}. Available quantity: {x.quantity}")
                        event = {
                            "order_id": data.get("order_id"),
                            "product_name": product_name,
                            "quantity": quantity,
                            "status": "Inventory failed"
                        }
                        p.produce(topic="inventory_failed", value=json.dumps(event).encode("utf-8"))
                        p.flush()
                except Exception as e:
                    print("Error:", e)
            else:
                print(f"Product {product_name} not found in inventory.")
        
        # Handle payment_success topic
        elif topic == "payment_success":
            print(f"Order {data.get('order_id')} confirmed.")
            event = {
                "order_id": data.get("order_id"),
                "product_name": data.get("product_name"),
                "quantity": data.get("quantity"),
                "status": "Order Confirmed"
            }
            p.produce(topic="order_confirmed", value=json.dumps(event).encode("utf-8"))
            p.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
            p.flush()
        
        # Handle payment_failed topic
        elif topic == "payment_failed":
            product_name = data.get("product_name")
            quantity = int(data.get("quantity"))
            x = Inventory.objects.filter(product_name=product_name).first()
            if x:
                try:
                    x.quantity += quantity
                    x.save()
                    print("Inventory released for", product_name)
                    event = {
                        "order_id": data.get("order_id"),
                        "product_name": product_name,
                        "quantity": quantity,
                        "status": "Release Inventory"
                    }
                    p.produce(topic="order_cancelled", value=json.dumps(event).encode("utf-8"))
                    p.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
                    p.flush()
                except Exception as e:
                    print("Error:", e)
            else:
                print(f"Product {product_name} not found in inventory.")
                
    except KeyboardInterrupt:
        print("Keyboard interrupt detected. Exiting...")
        break

c.close()