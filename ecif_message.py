# import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage

CONNECTION_STR = "Endpoint=sb://weng-sb1.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=CMuOnM9kPo1u6AVhzbw5taBDVoLwSp2urSqR7Zgjnho="
QUEUE_NAME = "ecif_input"

Message_1= '{"ECRM_XREF_ID__c":"001","Name":"Ryan","Phone":"416-666-6666","ClientSegment":"A"}'
Message_2= '{"ECRM_XREF_ID__c":"002","Name":"Lebo","Phone":"416-555-5555","ClientSegment":"E" }'
Message_3= '{"ECRM_XREF_ID__c":"003","Name":"Andy","Phone":"416-444-4444","ClientSegment":"E" }'

# Message_1= '{"ClientSegment":"A", "ECRM_XREF_ID__c":"001"}'
# Message_2= '{"ClientSegment":"E", "ECRM_XREF_ID__c":"002"}'

 

def send_single_message(sender):
    message = ServiceBusMessage(Message_1)
    sender.send_messages(message)
    print("Sent a message of m1")

def send_a_list_of_messages(sender):
    messages = [ServiceBusMessage(Message_2) for _ in range(5)]
    sender.send_messages(messages)
    print("Sent 5 message m2")


servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)

with servicebus_client:
    sender = servicebus_client.get_queue_sender(queue_name=QUEUE_NAME)
    with sender:
        send_single_message(sender)
        send_a_list_of_messages(sender)
 

print("Done sending messages")


