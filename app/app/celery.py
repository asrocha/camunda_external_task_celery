
import os
from celery import Celery
from django.conf import settings
from django.apps import apps

app = Celery('app')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.conf.task_track_started = True
app.config_from_object('django.conf:settings')

app.autodiscover_tasks(lambda: [n.name for n in apps.get_app_configs()])

#app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
#app.autodiscover_tasks('app.tasks')
