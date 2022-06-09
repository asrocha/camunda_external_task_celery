"""
This tasks is about  callbacks and error (incidents and business error) for camunda BPM
This portion of code aways will run with bpcore workers
"""
import time
import json
import logging
import os
import http.client
from  core.exceptions import BusinessException
import pycamunda.externaltask
import pycamunda
import pycamunda.processinst

from gettext import  gettext as _
from celery.result import AsyncResult

import requests

from celery.utils.log import get_task_logger

from app.celery import app

logger = get_task_logger(__name__)
url = os.getenv('CAMUNDA_ENGINE_BASE_URL','http://localhost:8080/engine-rest')
app.conf.task_track_started = True


def remove_celery_var(task):
    """
    remove var celery_task from processinstance it is usefull for knows if a task is running on celery
    :param task: Task parsed
    :return:
    """
    try:
        
        var_name = F"{task['id_']}_{task['activity_id']}_celery_task"


        del_var = pycamunda.processinst.VariablesDelete(url=url, process_instance_id=task['process_instance_id'], var_name=var_name)
        del_var()
        
    except(Exception) as e:
        
        raise Exception ('ERROR_ON_DEL_CELERY_TASK')









@app.task()
def complete_task(result,task):
    """
    This is when a Camunda Task was completed with success it will complete the task
    :param result:  Result of a Async Task from Celery
    :param task: CamundaExternalTask object with context etc to be completed
    :return:
    """
    try:

        try:
            if task is None:

                return

            try:
                remove_celery_var(task=task)
            except :
                print ("ERROR_ON_DELETE_CELERY_TASK")
                pass


            complete = pycamunda.externaltask.Complete(url=url, id_=task['id_'], worker_id=task['worker_id'])
            if isinstance(result,dict):
               for k in result.keys():
                   complete.add_variable(name=k, value=result[k])
            elif isinstance(result,list):
                complete.add_variable(name='bulk_data', value=result)

            res = complete()


        except (Exception) as e:
            logger.error(f"call_comnplete:{e}", exc_info=True)

    except (Exception) as e:
        logger.error(f"ERROR_ON_COMPLETE_TASK_ON_BPMENGINE:{e}",extra={'task':task})



@app.task()
def fail_task(request, exc, traceback, task):
    """
    This is when a Camunda Task was completed with success it will complete the task
    :param result:  Result of a Async Task from Celery
    :param task: CamundaExternalTask object with context etc to be completed
    :return:
    """
    try:

        
        try:
            print(F"Falhar a Tarefa : {task}")
            try:
                remove_celery_var(task=task)
            except :
                pass


            if isinstance(exc, BusinessException):
        
                pycamunda.externaltask.HandleBPMNError(url=url,id_=task['id'],worker_id=task['worker_id'], error_code=exc.error_code, error_message=exc.error_message)

            else:
                failure = pycamunda.externaltask.HandleFailure(url=url, id_=task['id'], worker_id=task['worker_id'],
                                                               error_message=f"{exc}", error_details=f"{exc}", retries=0, retry_timeout=10)

                failure()


        except (Exception) as e:
            logger.error(f"ERROR call_comnplete:{e}")



    except (Exception) as e:
        pass
        logger.error(f"ERROR_ON_COMPLETE_TASK_ON_BPMENGINE:{e}",extra={'task':task})

