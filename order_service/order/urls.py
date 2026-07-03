from django.contrib import admin
from django.http import JsonResponse
from django.urls import path, include

urlpatterns = [
    path('health/', lambda request: JsonResponse({"status": "healthy"}), name='health'),
    path('admin/', admin.site.urls),
    path('', include('myapp.urls')),
]
