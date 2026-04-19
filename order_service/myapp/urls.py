from django.urls import path
from .views import getupdate, order 

urlpatterns = [
    path('order/', order, name='order'),
    path('getupdate/', getupdate, name='getupdate'),
]
