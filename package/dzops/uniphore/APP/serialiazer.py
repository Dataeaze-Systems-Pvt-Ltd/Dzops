from rest_framework import serializers
from .models import MyModel

class dataset(serializers.ModelSerializer):
      class meta:
            model = MyModel
            fields = '__all__'

