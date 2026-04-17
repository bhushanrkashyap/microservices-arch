from django.db import models
from django.utils import timezone

class Notification(models.Model):
    notification_id = models.AutoField(primary_key=True)
    user_name = models.CharField(max_length=100, null=True, blank=True)
    user_email = models.EmailField(null=True, blank=True)
    event = models.CharField(max_length=100)
    status = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True, null=True)

    class Meta:
        db_table = 'notifications'