from django.urls import path
from labops import api
from labops.telemetry import metrics
urlpatterns = [path('metrics',metrics),path('login/',api.login_page),path('logout/',api.logout_page),path('api/v1/<path:route>',api.dispatch),path('',api.app_page),path('<path:page>',api.app_page)]
