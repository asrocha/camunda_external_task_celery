import os
import sys
import json
import logging
import uuid
import django
sys.path.insert( 0, '/app' )
import pycamunda
import pycamunda.externaltask
import pycamunda.processinst
import requests
from gettext import gettext as _
import sys
import importlib
django.setup()
from art import art, tprint, text2art, decor
from inspect import signature
from app.celery import app
from app.settings import CELERY_RESULT_BACKEND
import logging
from camunda.external_task.external_task import ExternalTask, TaskResult
from camunda.external_task.external_task_worker import ExternalTaskWorker
#from bpcore.tasks.bpmcontrol.bpmcontrol import complete_task, fail_task
import pycamunda
from concurrent.futures.thread import ThreadPoolExecutor
#from bpcore.exceptions import BusinessException

app.conf.task_track_started = True

logger = logging.getLogger('bpcore')
#app.loader.import_default_modules()


#### Configuration
url = os.getenv('CAMUNDA_ENGINE_BASE_URL','http://localhost:8080/engine-rest')
worker_id = 'Core'
topics = os.getenv('TOPICS', ['network', 'Core','CTI' ])
long_polling_topics = os.getenv('LONG_POLLING_TOPICS', ['network'])
LOCK_DURATION = os.getenv('LOCK_DURATION',60000)
EXTEND_LOCK_DURATION = os.getenv('EXTEND_LOCK_DURATION',300000)

app.loader.import_default_modules()

tprint("C A M U N D A ", font="georgi16")
tprint( "Meetup Brasil #9")

for t in app.tasks:
    print(f"{worker_id}--> Waiting: {t}")
print ("=======================================================================")
tprint("Ready to rock ......", font="rdn-small")
print ("=======================================================================")


def  add_celery_task_to_process(celery_task,task):
    """
    Add a Celery Task Id to process variables
    it will be usefull to check if celery task was running on background
    @change the celery_task var name to var_name because many time can run in parallel tasks with same name
    :param celery_task:
    :param task: External Task_parset
    :return:
    """
    try:
        var_name = F"{task['id_']}_{task['activity_id']}_celery_task"

        set_var = pycamunda.processinst.VariablesModify(url=url,process_instance_id=task['process_instance_id'])
        set_var.add_variable(name=var_name,value=celery_task.id)
        set_var()
    except (Exception) as e:
        logger.error(F"{_('ERROR_ON_SET_CELERY_ID')}: {e}")
        raise Exception (F"{_('ERROR_ON_SET_CELERY_ID')}: {e}")



def get_celely_task(task_id):
    """
    Get info about a celery Task on Flower
    :param task_id:
    :return:
    """
    try:
        
        url = f"https://{CAMUNDA_ENGINE_BASE_URL}/celery_task/_doc/{task_id}"
        headers = {"Content-Type": "application/json"}
        res = requests.get(url=url, headers=headers, auth=('admin','admin'), verify=False)
        data = json.loads(res.text)
        
        if '_source' in  data:
        
            return data['_source']
        elif 'found' in data:
            if not data['found']:
                return None
        else:
            return None
    except (Exception ) as e:
        raise  Exception (f"{_('ERROR_ON_GET_TASK_FROM_OPENSEARCH')} : {e}")







def check_if_task_is_running(task):
    """
    Checking if task is running on celery Work
    :param task: (activity Camunda Task)
    :return: Boolean
    """
    try:
        
        vars = pycamunda.variable.GetList(url=url, process_instance_id_in=task['process_instance_id'])
        vars_intances = vars()
        celery_task = None
        for var in vars_intances:
            
            if "_celery_task" in getattr(var, "name") :
                print (F"tem celery_task: {getattr(var,'value')}")
                celery_task = getattr(var,"value")
                celery_task_data = get_celely_task(celery_task)
                
                if celery_task_data is not None:
                        if task['activity_id'] in celery_task_data['name']:
                            """
                            it is a activity with the same name of task
                            """
                            if celery_task_data [ 'state'] != "SUCCESS" and celery_task_data['state'] != "FAILURE":
                                return True
                        elif  (task['activity_id'].startswith('capability_') and task['activity_id'] in str(celery_task_data['args'])):
                            """
                            it is a capability
                            """
                            print ("Retornando true por causa da capability")
                            if celery_task_data [ 'state'] != "SUCCESS" and celery_task_data['state'] != "FAILURE":
                                return True

        return False

    except (Exception) as e:
        logger.error(f"{_('ERROR_VERIFY_TASK_IS_RUNNING_ON_CELERY')}: {e}")
        raise Exception (f"{_('ERROR_VERIFY_TASK_IS_RUNNING_ON_CELERY')}: {e}")



def extend_lock (task):
    """
    Extend lock duration for this task based on celery status
    :param celery_task:
    :param task:
    :return:
    """
    try:
        extend = pycamunda.externaltask.ExtendLock(url=url,id_=task['id_'],new_duration=EXTEND_LOCK_DURATION, worker_id=worker_id)
        extend()
        

    except (Exception) as e:
        logger.error(f"{_('ERROR_ON_EXTEND_LOCK_TIME')}: {e}")
        raise Exception(f"{_('ERROR_ON_EXTEND_LOCK_TIME')}: {e}")







def map_wftask_param_to_bp_param_task(signature, task):
    """
    This map the signature of task (celery task) in BP, with the camunda tasks variables
    :param signature: The signature of task
    :param task:  Camunda Task
    :return: Dict parsed of parameter to call
    """

    call = {}
    

    vars = pycamunda.variable.GetList(url=url,process_instance_id_in=task.process_instance_id)
    vars_intances = vars()

    vars = {}
    for var in vars_intances:

        if getattr(var,'type_') == "Object":

           j = pycamunda.variable.Get(url=url, id_=var.id_, deserialize_value=True)

           vars.update({getattr(var,'name'): j().value})


        else:
            pass
            vars.update({getattr(var,'name'): var.value})


    return vars



def handle_task(task: ExternalTask):
    # def handle_task(task):
    """
    Handle tasks to dispacher
    :param task:
    :return:
    """
    
    try:
        
        activityId = task.__dict__['activity_id']
        task_parsed = task.__dict__

  

        is_running = check_if_task_is_running(task_parsed)
        if is_running:
            
            extend_lock(task_parsed)
            logger.info(F"{_('EXTENDING_LOCK_DURATION')}: {LOCK_DURATION} : TASK:{activityId}")
            return

        
        for t in app.tasks:
            

            #it will usefull when  running the same task in parallell
            if activityId in t or activityId.rstrip('0123456789') in t:

                if activityId.rstrip('0123456789') in t:
                    module = t.split('.')
                    module = ".".join(module[:-1]).rstrip('0123456789')
                    activityId = activityId.rstrip('0123456789')
                else:
                    module = t.split('.')
                    module = ".".join(module[:-1])

                module = importlib.import_module(module)
                
                working = getattr(module, activityId)
                
                parameters = signature(working)
                call = map_wftask_param_to_bp_param_task(parameters, task)

                

                try:
                    if len(list(parameters.parameters)) == 1:
                        

                        celery_task  = working.apply_async(args=[call],link=complete_task.s(task=task_parsed),
                        link_error=fail_task.s(task=task_parsed))
                        pass

                    else:
                        parameters = list(parameters.parameters)
                        true_call = {}
                        call = map_wftask_param_to_bp_param_task(parameters, task)


                        for x in parameters:
                            if x in call:
                        
                                true_call.update({x: call[x]})
                        
                        celery_task = working.s(**true_call).apply_async(link=complete_task.s(task=task_parsed),link_error=fail_task.s(task=task_parsed))


                    add_celery_task_to_process(celery_task,task_parsed)

                except (TypeError) as e:
                    error = F"{e}"
                    logger.critical(f"ERRRO ON CALL celery TASK: {type(e)}{e} ",exc_info=True)
                    

                add_celery_task_to_process(celery_task, task_parsed)
                
                return True



    except (BusinessException) as e:
        
        failure = pycamunda.externaltask.HandleBPMNError(url=url, id_=task_parsed['id_'], worker_id=task_parsed['worker_id'],
                                                       error_message=f"{e.error_message}", error_code=f"{e.error_code}")
        failure()


    except (Exception) as e:
        logger.error(f"ERROR_ON_EXCECUTE TASK: {e}", extra=task_parsed, exc_info=True)

        # task.failure(f"ERROR_ON_EXCECUTE TASK" , result.result, default_config['retries'], default_config['retryTimeout'] )


fetch_and_lock = pycamunda.externaltask.FetchAndLock(url=url, worker_id=worker_id, max_tasks=5000)
print(f"Subscribing on Topic: {topics} ....... {LOCK_DURATION}")

while True:
    try:
        for topic in topics:
            try:

                fetch_and_lock.add_topic(name=topic, lock_duration=LOCK_DURATION)
            except (Exception) as e:
                logger.error(f"{_('ERROR_ON_SUBSCRIBE_BPMNEENGINE_TOPIC')}:{e}",
                             extra={'topic': 'topic', 'worker': worker_id})

        tasks = fetch_and_lock()
        for task in tasks:
            print (f"Tasks no topico : {task.__dict__['topic_name']}  {type} {task.__dict__['activity_id']} --> instancia: , {task.__dict__['activity_instance_id']}---\n")
            try:
                handle_task(task)
            except (Exception) as e:
                print(F"Erro ao diapachar task: {e} ")
                logger.error(F"{_('ERROR_ON_DISPACHER_TASK')}:{e} ", exc_info=True)

    except (Exception) as e:

        logger.error(f"{_('ERROR_ON_PRINT_TASK')}:{e}", extra={'topic': 'topic', 'worker': worker_id})
