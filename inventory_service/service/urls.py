from django.urls import path
from .views import inventory_detail

urlpatterns = [
    path('details/<int:inventory_id>/', inventory_detail, name='inventory_detail'),
]
