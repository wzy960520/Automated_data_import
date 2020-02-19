from django.urls import path
from .views import main,Data_processing
urlpatterns = [
    path('',main),
    path('form/',Data_processing)
]