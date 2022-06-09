
from __future__ import absolute_import, unicode_literals

import time

from app.celery import app

import random


@app.task()
def run_task_intenals(data):


    time.sleep(5)

    "Run a task to "
    print (f"Running a task: data {data}")


    return {'random1': random.random(),   'random2':random.randrange(1,15000) }

    #raise Exception ({"ERROR":"ERRO no CELERY"})



@app.task()
def callback(result,data):

    print (F"calback:\n result: {result} : \n\n data: {data}")