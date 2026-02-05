import json
import os
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate
from .models import User
from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

@csrf_exempt
def user_login(request):
    if request.method == "POST":
        data = json.loads(request.body)
        user = authenticate(username=data["username"], password=data["password"])
        
        if not user:
            return JsonResponse({"error": "Invalid credentials"}, status=401)

        event = {"user_id": user.id, "username": user.username}
        producer.produce(topic="user_login", value=json.dumps(event).encode("utf-8"))
        producer.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
        producer.flush()

        return JsonResponse({"authenticated": True}, status=200)

@csrf_exempt
def user_register(request):
    if request.method == "POST":
        data = json.loads(request.body)
        
        if User.objects.filter(email=data["email"]).exists():
            return JsonResponse({"error": "Email already registered"}, status=400)
        
        user = User.objects.create(
            username=data["username"],
            password=data["password"],
            email=data["email"],
            first_name=data.get("first_name", ""),
            last_name=data.get("last_name", "")
        )
        
        event = {
            "user_id": user.id,
            "username": user.username,
            "email": user.email,
            "status": "user-registered"
        }
        producer.produce(topic="user-registration", value=json.dumps(event).encode("utf-8"))
        producer.produce(topic="notifications", value=json.dumps(event).encode("utf-8"))
        producer.flush()
        
        return JsonResponse({"registered": True}, status=201)
