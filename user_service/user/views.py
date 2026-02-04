from xml.dom.domreg import registered
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate
from django.shortcuts import redirect
from .models import User
from django.contrib import messages
from confluent_kafka import Producer
import json

producer = Producer({"bootstrap.servers": "localhost:9092"})

def delivery_report(err, msg):
    if err is not None:
        print(f" Message delivery failed: {err}")
    else:
        print(f" Message delivered to {msg.topic()} [{msg.partition()}]")

@csrf_exempt
def user_login(request):
    if request.method == "POST":
        data = json.loads(request.body)

        user = authenticate(
            username=data["username"],
            password=data["password"]
        )

        if not user:
            messages.error(request, "Invalid credentials. Please register.")
            return redirect("/register")
            


        event = {
            "user_id": user.id,
            "username": user.username
        }

        print(f" Producing event: {event}")
        producer.produce(
            topic="user_login",
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report
        )
        producer.produce(topic = "notifications" , value = json.dumps(event).encode("utf-8"))
        producer.flush()

        return JsonResponse({"authenticated": True}, status=200)



def user_register(request):
    if request.method == "POST":
        data = json.loads(request.body)
        username = data["username"]
        password = data["password"]
        email = data["email"]
        first_name = data.get("first_name", "")
        last_name = data.get("last_name", "")
        if User.objects.filter(email=email).exists():
            messages.error(request, "Email already registered. Please login.")
            return redirect("/login")
        else:
            user = User.objects.create(
                username=username,
                password=password,
                email=email,
                first_name=first_name,
                last_name=last_name
            )
            event = {
                "user_id": user.id,
                "username": user.username,
                "email": user.email,
                "status" : "user-registered"
            }
            producer.produce(topic = "user-registration" , value = json.dumps(event).encode("utf-8") , callback = delivery_report)
            producer.produce(topic = "notifications")
            return JsonResponse({"registered": True}, status=201)

        
