import json
import os
from django.core.cache import cache
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from confluent_kafka import Producer , Consumer
from django.http import HttpResponse

from myapp.models import Order

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
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
    "data": {
        "order_id": order_obj.order_id,

    }
}

        producer.produce(
            topic="order-confirmed",
            value=json.dumps(order_details).encode("utf-8")
        )
        producer.produce(topic="notifications", value=json.dumps(order_details).encode("utf-8"))
        producer.flush()

        return JsonResponse({"status": "Order Placed Successfully"})

def getupdate(request):
    order_id = request.GET.get('order_id')
    
    if not order_id:
        return JsonResponse({"error": "order_id parameter is required"}, status=400)
    
    try:
        order_id = int(order_id)
    except ValueError:
        return JsonResponse({"error": "order_id must be an integer"}, status=400)
    
    # Debug: Check what key format is being used
    cache_key = f"order_{order_id}"
    print(f"Attempting to get cache key: {cache_key}")
    
    status = cache.get(cache_key)
    print(f"Cache result: {status}")
    
    if status:
        return JsonResponse({
            "order_id": order_id,
            "status": status,
            "source": "redis-cache"
        })
    
    # Fall back to database
    try:
        order = Order.objects.get(order_id=order_id)
        return JsonResponse({
            "order_id": order_id,
            "status": order.status,
            "source": "db"
        })
    except Order.DoesNotExist:
        return JsonResponse({"error": "Order not found"}, status=404)
    
