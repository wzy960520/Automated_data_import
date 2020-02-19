from django.urls import path
from .views import Adiut_Data,Data_Download,Import_Data

urlpatterns = [
    path('',Adiut_Data),
    path('form/',Data_Download),
    path('import/',Import_Data)
]