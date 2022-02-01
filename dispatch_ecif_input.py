# pylint: disable=C0111

import os
import json
from types import SimpleNamespace
from azure.servicebus import ServiceBusClient,ServiceBusMessage

class QueueForSf:
    q_retail= 'q_retail'
    q_wealth_management = 'q_wealth_management'
    q_commercial_banking = 'q_commercial_banking'

def sendToQueue(servicebus_client,qname,msg):
     # get a Queue Sender object to send messages to the queue
    sender = servicebus_client.get_queue_sender(queue_name=qname)
    # create a Service Bus message
    # https://docs.microsoft.com/en-us/python/api/azure-servicebus/azure.servicebus.servicebusmessage?view=azure-python#parameters
    # message = ServiceBusMessage(msg)
    message = ServiceBusMessage(
       msg,
       content_type="application/json",      
    )
   
    # send the message to the queue
    sender.send_messages(message)
    print("Message sent to queue :",qname)


def dispatchMsg(servicebus_client,msg):
    """
    # Convert string to Python dict 
    msg_dict = json.loads(msg) 
    #print(msg_dict) 
    print('ECRM_XREF_ID__c=' ,msg_dict['ECRM_XREF_ID__c'])
    print('Name=' ,msg_dict['Name'])
    print('Phone=' ,msg_dict['Phone'])
    print('ClientSegment=' ,msg_dict['ClientSegment']) 
    """
    # Parse JSON into an object with attributes corresponding to dict keys.
    x = json.loads(msg, object_hook=lambda d: SimpleNamespace(**d))
    # print(x.name, x.hometown.name, x.hometown.id)
    print('x=',x) 
    print('ClientSegment=' ,x.ClientSegment)
    print('ECRM_XREF_ID__c=' ,x.ECRM_XREF_ID__c)
    #print('Name=' ,x.Name)
    #print('Phone=' ,x.Phone)
    #print('BillingCity=' ,x.BillingCity)

 
   


    if ( x.ClientSegment == 'A' ):
        sendToQueue(servicebus_client,QueueForSf.q_retail,msg)
    elif  ( x.ClientSegment == 'E' ):
        sendToQueue(servicebus_client,QueueForSf.q_retail,msg)
        sendToQueue(servicebus_client,QueueForSf.q_wealth_management,msg)
    else :
        #sendToQueue(servicebus_client,queueE,msg)
        pass
    

   
    
    
   

# CONNECTION_STR = os.environ['SERVICE_BUS_CONNECTION_STR']
# QUEUE_NAME = os.environ["SERVICE_BUS_QUEUE_NAME"]

CONNECTION_STR ='Endpoint=sb://weng-sb1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=CMuOnM9kPo1u6AVhzbw5taBDVoLwSp2urSqR7Zgjnho=';
ECIF_INPUT_QUEUE_NAME = 'ecif_input'

servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR)

with servicebus_client:
    receiver = servicebus_client.get_queue_receiver(queue_name=ECIF_INPUT_QUEUE_NAME)
    with receiver:
        for msg in receiver:
            print('--------Message Begin--------')
            print(str(msg))
            dispatchMsg(servicebus_client,str(msg))
            print('--------Message End--------')
            receiver.complete_message(msg)

print("Receive is done.")


