import os
import json
from datetime import datetime
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from confluent_kafka import Producer
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.exceptions import AuthenticationFailed
from rest_framework_simplejwt.authentication import JWTAuthentication
from ipware import get_client_ip
from django.core.cache import cache


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

def delivery(err , msg):
    if err:
        print("Message delivery failed")
    else:
        print(f"Message delivered to {msg.topic()}")

@csrf_exempt
def user_login(request):
    rate_limit  = rate_limiting(request)
    if rate_limit:
        return rate_limit
    if request.method != "POST":
        return JsonResponse({"error": "Only POST allowed"}, status=405)
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({"error": "Invalid JSON"}, status=400)
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return JsonResponse({"error": "Username and password required"}, status=400)

    user = authenticate(username=username, password=password)

    if not user:
        return JsonResponse({"error": "Invalid credentials"}, status=401)

    if not user.is_active:
        raise AuthenticationFailed("User is not active")

    refresh = RefreshToken.for_user(user)

    event = {
        "event" : "USER_LOGIN",
        "data" : {
            "user_id" : user.id , 
            "username" : user.username , 
            "email" : user.email , 
        },
        "timestamp": datetime.utcnow().isoformat(),
        "source": "user-service"
    }
    try:
        producer.produce(topic = "user-login" , value = json.dumps(event).encode("utf-8") , callback= delivery)
        producer.flush()
    except Exception as e:
        print("Kafka error:", e)
    return JsonResponse({
        "authenticated": True,
        "refresh": str(refresh),
        "access": str(refresh.access_token),
    }, status=200)


@csrf_exempt
def user_register(request):
        rate_limit  = rate_limiting(request)
        if rate_limit:
            return rate_limit
        if request.method != "POST":
            return JsonResponse({"error": "Only POST allowed"}, status=405)
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)
        username = data.get("username")
        email = data.get("email")
        password = data.get("password")
        if not username or not email or not password:
            return JsonResponse({"error": "Username, email and password required"}, status=400)
        if User.objects.filter(email=email).exists():
            return JsonResponse({"error": "Email already registered"}, status=400)
        if User.objects.filter(username=username).exists():
            return JsonResponse({"error": "Username already exists"}, status=400)
        user = User.objects.create(
            username = data.get("username") , 
            email = data.get("email")
        )
        user.set_password(data["password"])
        user.save()
        
        event = {
            "event" : "USER_REGISTERED",
            "data" : {
            "user_id": user.id,
            "username": user.username,
            "email": user.email,
            },
            "timestamp" : datetime.utcnow().isoformat(),
        }
        try:
            producer.produce(topic="user-registration", value=json.dumps(event).encode("utf-8") , callback=delivery)
            producer.produce(topic="notifications", value=json.dumps(event).encode("utf-8") , callback = delivery)
            producer.flush()
        except Exception as e:
            print("kafka error" , e)
        return JsonResponse({"registered": True}, status=201)
@csrf_exempt
def verify_token(request):
    if request.method != "POST" :
        return JsonResponse({"error" : "Only POST allowed"} , status = 405)
    data = json.loads(request.body)
    token = data.get("token")
    if not token:
        return JsonResponse({"error" : "Token required"} , status = 400)
    auth = JWTAuthentication()
    try:
        token = auth.get_validated_token(token)
        user = auth.get_user(token)
        return JsonResponse({"valid" : True , "user_id" : user.id} , status = 200)
    
    except Exception as e:
        return JsonResponse({"valid" : False , "error" : str(e)} , status = 401)

def rate_limiting(request):
    ip , _ = get_client_ip(request)
    key = f"rate:{ip}"
    total_calls = cache.get(key)
    if total_calls is not None:
        if total_calls >= 5:
            return JsonResponse({"error": "Too many requests"}, status=429)
        else:
            cache.set(key, total_calls + 1, timeout= 30)
    else:
        cache.set(key, 1, timeout=60)
    return None