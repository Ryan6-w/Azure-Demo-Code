# pylint: disable=C0111

import os
import requests
import json
from types import SimpleNamespace
from azure.servicebus import ServiceBusClient

ENV_JSON = {
        "salesforce":{
            "login_url"     : "https://login.salesforce.com/services/oauth2/token",
            "my_site_url"   : "https://liveitconsult-dev-ed.my.salesforce.com",
            "client_id"     : "3MVG9p1Q1BCe9GmAk95wwnogFKm88sBkiGOu0oslF9P6ld6JFbx4uTTcuxYSQJ3UE8aowaliIVblnX0DPKBNz",
            "client_secret" : "BF90A657B02C8CD31E1E0A1E9180D2B42A1B7E88882ECB1B71C23340E7498791",
            "username"      : "lebo.huang@yahoo.com",
            "password"      : "Sftest123!"
        },
        "servicebus":{
            "connection_string"    : "Endpoint=sb://servicebuspocns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DyMeUqdmGPraJ2gtFr36JD/4zLaTWTzvK2iL9Ds4RB0=",
            "queue_name"      : "q_wealth_management"
        }
}

ENV_OBJ = json.loads( json.dumps(ENV_JSON), object_hook=lambda d: SimpleNamespace(**d))

def getToken():
    #get from OS ?
    """
    login_url = 'https://login.salesforce.com/services/oauth2/token'
    sf_client_id = ''
    sf_client_secret = ''
    sf_username = ''
    sf_password = ''
    """
    login_url = ENV_OBJ.salesforce.login_url
    sf_client_id = ENV_OBJ.salesforce.client_id
    sf_client_secret = ENV_OBJ.salesforce.client_secret
    sf_username = ENV_OBJ.salesforce.username
    sf_password = ENV_OBJ.salesforce.password

    
    login_data = {
        'grant_type'    :'password',
        'client_id'     : sf_client_id,
        'client_secret' : sf_client_secret,
        'username'      : sf_username,
        'password'      : sf_password
        }
    print('...begin getToken...')
    x = requests.post(login_url, data = login_data, headers = {"Content-Type": "application/x-www-form-urlencoded"})
    """
    print('login_url=',login_url)
    print('login_data=',login_data)
    print('status_code=',x.status_code)
    print('headers=',x.headers)
    print('json=',x.json())
    """
    return x.json()['access_token']


def upsertAccountByXrefId(token,xrefId,accountInfo):
    print('...begin upsertAccountByXrefId...')

    #url = 'https://cibc64-dev-ed.my.salesforce.com/services/data/v53.0/sobjects/Account/ECRM_XREF_ID__c/'+xrefId
    url = ENV_OBJ.salesforce.my_site_url+'/services/data/v53.0/sobjects/Account/ECRM_XREF_ID__c/'+xrefId

    sf_headers_dict = {
        "Content-Type": "application/json",
        "Authorization" : "Bearer "+ token
    }


    x = requests.patch(url,  data=json.dumps(accountInfo), headers =sf_headers_dict)
    #print('status_code=',x.status_code)
    #print('headers=',x.headers)
    return x.status_code

def findSfAccountIdbyXrefId(token,xrefid):
    print('...begin findSfAccountIdbyXrefId...')

    #url = 'https://???.my.salesforce.com/services/data/v53.0/parameterizedSearch/?q='+xrefid+'&sobject=Account&Account.fields=id,ECRM_XREF_ID__c&Account.where=ECRM_XREF_ID__c=\''+xrefid+'\''

    url = ENV_OBJ.salesforce.my_site_url+'/services/data/v53.0/parameterizedSearch/?q='+xrefid+'&sobject=Account&Account.fields=id,ECRM_XREF_ID__c&Account.where=ECRM_XREF_ID__c=\''+xrefid+'\''



    sf_headers_dict = {
        "Authorization" : "Bearer "+ token
    }


    x = requests.get(url,  headers =sf_headers_dict)
    #print('status_code=',x.status_code)
    #print('headers=',x.headers)
    #print('json=',x.json())
    if(len(x.json()['searchRecords'])>0) : 
        return x.json()['searchRecords'][0]['Id']
    else:
        return None

def deleteAccountByAccountId(token,accountId):
    print('...begin deleteAccountByAccountId...')
    #url='https://???.my.salesforce.com/services/data/v53.0/sobjects/Account/'+accountId 

    url = ENV_OBJ.salesforce.my_site_url+'/services/data/v53.0/sobjects/Account/'+accountId

    sf_headers_dict = {
        "Authorization" : "Bearer "+ token
    }

    x = requests.delete(url,  headers =sf_headers_dict)
    print('status_code=',x.status_code)
    
    #print('headers=',x.headers)
    #print('json=',x.json())
    return x.status_code
    

def deleteAccountByXrefId(token,xrefId):
    print('...begin deleteAccountByXrefId...')
    accountId = findSfAccountIdbyXrefId(token,xrefId)
    print("accountId=",accountId)
    return deleteAccountByAccountId(token,accountId)

def mergeAccount(token,newXrefId,oldXrefId):
    print('...begin mergeAccount...')
    print('oldXrefId [',oldXrefId, '] will be replace by newXrefId [',newXrefId,'[]')
    accountId = findSfAccountIdbyXrefId(token,oldXrefId)
    print("accountId=",accountId)

    #url='https://???.my.salesforce.com/services/data/v53.0/sobjects/Account/'+accountId 

    url = ENV_OBJ.salesforce.my_site_url+'/services/data/v53.0/sobjects/Account/'+accountId
    print("url=",url)
    sf_headers_dict = {
        "Content-Type": "application/json",
        "Authorization" : "Bearer "+ token
    }

    mergeAccountInfo={
            "ECRM_XREF_ID__c"       : newXrefId
    }
    x = requests.patch(url,  data=json.dumps(mergeAccountInfo), headers =sf_headers_dict)
    #print('status_code=',x.status_code)
    #print('headers=',x.headers)
    return x.status_code


def processMsg(msg):
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
    print('OperationType=',x.OperationType)

    token=getToken()
    print('token=',token)
    if( x.OperationType == 'Create' ):
        #print('...Create SF account begin... ')
        createAccountInfo={
            "Name"        : x.Name,
            "Phone"       : x.Phone
        }
        createResult = upsertAccountByXrefId(token,x.ECRM_XREF_ID__c,createAccountInfo)
        print('createResult=',createResult)
    elif (x.OperationType == 'Update'):
        #print('...Update SF account begin... ')
        updateAccountInfo={
            "Phone"       : x.Phone
        }
        updateResult = upsertAccountByXrefId(token,x.ECRM_XREF_ID__c,updateAccountInfo)
        print('updateResult=',updateResult)
    elif ( x.OperationType == 'Delete' ):
        #print('...Delete SF account begin... ')
        deleteResult = deleteAccountByXrefId(token,x.ECRM_XREF_ID__c)
        print('deleteResult=',deleteResult)
    elif ( x.OperationType == 'Merge' ):
        #print('...Merge SF account begin... ')        
        mergeResult = mergeAccount(token,x.ECRM_XREF_ID__c,x.Non_Suvivor)
        print('mergeResult=',mergeResult)
    else:
         print('...Unknown opertation... ')


   


````
    
    
   

#CONNECTION_STR = os.environ['SERVICE_BUS_CONNECTION_STR']
#QUEUE_NAME = os.environ["SERVICE_BUS_QUEUE_NAME"]
#CONNECTION_STR ='Endpoint=sb://servicebuspocns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=DyMeUqdmGPraJ2gtFr36JD/4zLaTWTzvK2iL9Ds4RB0=';
#QUEUE_NAME = 'queue_one';


servicebus_client = ServiceBusClient.from_connection_string(conn_str= ENV_OBJ.servicebus.connection_string)

with servicebus_client:
    receiver = servicebus_client.get_queue_receiver(queue_name=ENV_OBJ.servicebus.queue_name)
    with receiver:
        for msg in receiver:
            print('--------Message Begin--------')
            print(str(msg))
            processMsg(str(msg))
            print('--------Message End--------')
            receiver.complete_message(msg)

print("Receive is done.")

