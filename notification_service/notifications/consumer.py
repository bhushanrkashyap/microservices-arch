import os , django , sys , json
from confluent_kafka import Consumer , KafkaError
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)       
django.setup()
from django.contrib import messages

from notifications.models import Notification
conf = {
    "bootstrap.servers" : "localhost:9092",
    "group.id" : "notification_service_group",
    "auto.offset.reset" : "earliest"
}
c = Consumer(conf)
c.subscribe(["user_created" , "user_registered","payment_success" , "payment_failed" , "order_confirmed" , "order_failed"])
while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        print("Error:" , msg.error())
        continue
