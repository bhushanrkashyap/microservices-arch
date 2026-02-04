import json

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from confluent_kafka import Producer

from myapp.models import Order

producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order_service_group'
}

producer = Producer(producer_conf)


@csrf_exempt
def order(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            data = request.POST

        product_name = data.get('product_name')
        quantity = data.get('quantity')
        price = data.get('price')

        order_obj = Order.objects.create(
            product_name=product_name,
            quantity=quantity,
            price=price
        )

        order_details = {
            "order_id": order_obj.order_id,
            "product_name": product_name,
            "quantity": quantity,
            "price": price,
            "status" : "Order Placed"
        }

        producer.produce(
            topic="order_created",
            value=json.dumps(order_details).encode("utf-8")
        )
        producer.produce(topic = "notification", value=json.dumps(order_details).encode("utf-8"))
        producer.flush()

        return JsonResponse({"status": "Order Placed Successfully"})
