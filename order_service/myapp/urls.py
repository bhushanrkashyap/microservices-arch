from django.urls import path
from .views import getupdate, order, order_detail

urlpatterns = [
    path('order/', order, name='order'),
    path('getupdate/', getupdate, name='getupdate'),
    path('details/<int:order_id>/', order_detail, name='order_detail'),
]
