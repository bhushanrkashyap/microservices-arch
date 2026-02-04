from django.db import models

# Create your models here.
class Notification(models.Model):
    notification_id = models.AutoField(primary_key=True)
    order_id = models.IntegerField()
    message = models.CharField(max_length=255)
    status = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'notifications'

    def __str__(self):
        return f"Notification {self.notification_id} for Order {self.order_id}: {self.status}"