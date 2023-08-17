from django.db import models

#Create your models here.

class MyModel(models.Model):
    dataset_id= models.IntegerField()
    dataset_name= models.CharField(max_length=20)
    dataset_type= models.CharField(max_length=20)
    language= models.CharField(max_length=20)
    source_type = models.CharField(max_length=20)
    vendor =models.CharField(max_length=20)
    domain = models.CharField(max_length=20)
    customer_name = models.CharField(max_length=20)

