import os
import pytz
from celery import Celery
from opensearchpy import OpenSearch
import logging
logger = logging.getLogger(__name__)
from datetime import datetime

host = os.getenv('OPENSEARCH_HOST','192.168.110.9')
port = 9200
auth = ('admin', 'admin')
index_name = 'celery_task'
update_url =  f"https://{host}:{port}/{index_name}/_update"
import ast

import requests
import json

from  nested_lookup import nested_lookup, get_all_keys
from art import art, tprint, text2art 



def send_activity(event):
    try:

        print(f"event:{event['uuid']}: {event['state']}  ")
        if  event['state'] == 'RECEIVED':
            
            if "args" in event:
                if "object" in event['kwargs']:

                    pass


            event.update({'created_at': datetime.now(pytz.utc).isoformat()})
            
            try:

                client = OpenSearch(hosts=[{'host': host, 'port': port}],
                    http_compress=True,  # enables gzip compression for request bodies
                    http_auth=auth,
                    # client_cert = client_cert_path,
                    # client_key = client_key_path,
                    use_ssl=True,
                    verify_certs=False,
                    ssl_assert_hostname=False,
                    ssl_show_warn=False,
                )
            
                event.update({'@timestamp': datetime.now(pytz.utc).isoformat() })
                response = client.index(
                    index=index_name,
                    body=event,
                    op_type="index",
                    id = event['uuid']
                )
            except (Exception) as e:
                raise Exception(f'Error on Index Task: {e}\n {event}')
        else :
            try:
            
              
                doc = {'doc':event}
                res = requests.post(F"{update_url}/{event['uuid']}", data=json.dumps(doc), auth=auth, verify=False, headers={'Content-Type': 'Application/json'})
                #print (res.text)
            except (Exception) as e:
                logger.error(f"ERROR_ON_UPDATE_CELETY_TASK:{e}", exc_info=True)


    except (Exception) as e:
        logger.error(f"ERROR_ON_SENT_CELETY_TASK_TO_OPENSEARCH:{e}",exc_info=True)





def my_monitor(app):
    state = app.events.State()

    def process_event_tasks(event):
        state.event(event)

        # task name is sent only with -received event, and state
        # will keep track of this for us.



        if 'state' in event:


            send_activity(event)


    with app.connection() as connection:
        monitored_states = ['task-sent','task-received','task-started','task-succeeded','task-failed','task-rejected','task-revoked', "task-retried"]
        rcv = app.events.Receiver(connection, handlers={'*': process_event_tasks})
        rcv.capture(limit=None, timeout=None, wakeup=True)



if __name__ == '__main__':
    tprint ("Celery \n Exporter ....", font='cyberlarge')
    
    print ("=======================================================================")
    tprint("Ready to rock ......", font="rnd-small")
    print ("=======================================================================")
    app = Celery(broker=f'amqp://guest@{host}//')
    my_monitor(app)