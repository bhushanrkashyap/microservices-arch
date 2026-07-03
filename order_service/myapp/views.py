import json
import os
from django.core.cache import cache
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from confluent_kafka import Producer , Consumer
from django.http import HttpResponse

from myapp.models import Order
from shared.config.kafka import producer_config

producer = Producer(producer_config())


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

    try:
        order = Order.objects.get(order_id=order_id)
        return JsonResponse({
            "order_id": order_id,
            "status": order.status,
            "source": "db"
        })
    except Order.DoesNotExist:
        return JsonResponse({"error": "Order not found"}, status=404)
    


def order_detail(request, order_id):
    if request.method != "GET":
        return JsonResponse({"error": "Only GET allowed"}, status=405)

    key = f"order:details:{order_id}"
    cached = cache.get(key)
    if cached is not None:
        return JsonResponse({**cached, "source": "cache"})

    try:
        o = Order.objects.get(order_id=order_id)
    except Order.DoesNotExist:
        return JsonResponse({"error": "Order not found"}, status=404)

    data = {
        "order_id": o.order_id,
        "product_name": o.product_name,
        "quantity": o.quantity,
        "price": str(o.price),
        "status": o.status,
    }
    cache.set(key, data, timeout=300)
    return JsonResponse({**data, "source": "db"})
