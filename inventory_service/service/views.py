from django.core.cache import cache
from django.http import JsonResponse

from service.models import Inventory


def inventory_detail(request, inventory_id):
    """Read-heavy GET: cache inventory details in Redis (TTL 300s)."""
    if request.method != "GET":
        return JsonResponse({"error": "Only GET allowed"}, status=405)

    key = f"inventory:details:{inventory_id}"
    cached = cache.get(key)
    if cached is not None:
        return JsonResponse({**cached, "source": "cache"})

    try:
        item = Inventory.objects.get(id=inventory_id)
    except Inventory.DoesNotExist:
        return JsonResponse({"error": "Inventory not found"}, status=404)

    data = {
        "id": item.id,
        "product_name": item.product_name,
        "quantity": item.quantity,
        "price": str(item.price),
    }
    cache.set(key, data, timeout=300)
    return JsonResponse({**data, "source": "db"})
