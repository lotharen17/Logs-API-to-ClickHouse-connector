import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt
import sshtunnel
import clickhouse_connect
import requests
import gzip
import json
from datetime import datetime
from datetime import timedelta
from clickhouse_connect.driver.tools import insert_file
import time


re_run = 0 #variable for future purposes: will be returned to the system, so the code can be re-run
default_sleep_time = 120 #seconds to wait before requesting prepared data 

api_file_path = "metrika_data.tsv"
log_path = 'logs.tsv'

#SSH credentials in case you need ssh. In case you don't - just comment this next line. 
# ssh_path = 'ssh_credentials.json'

######################Functions section 
######################
######################
######################
#File logger, to log results in log-file
def logger(response ='\t', endpoint='\t', description = '\t\n', path='logs.tsv'):
    """Logs data in file"""
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = date_time + response + endpoint +description
    with open(log_path, "a", encoding="utf-8", newline='\n') as f:
        f.write(log_line)
    return None


#Simply to read the file:
def file_reader(file_path):
    """Just reads the file in utf-8 and returns string content. arg = file_path"""
    try:
        with open(file_path, "r") as f:
            file_content = f.read()
        return file_content
    except: 
        logger(response='\t404', endpoint=f'\t{file_path}', description ='\tReading file not found\n', path=log_path)

#CLickhouse connection:
def clickhouse_connector(host='localhost', tcp_port = 666, username = 'default', password='default'):
    """Function to establish a connection with clickhouse server."""
    #SSH connection creation 
    def ssh_connection_creation(host, tcp_port):
        """Function, that reads global varibale ssh_path and creates. Needs ssh_path global variable to run proprely."""
        if 'ssh_path' in globals(): 
            try:
                with open(ssh_path, "r") as s:
                    ssh_json = s.read()
                    ssh_json = json.loads(ssh_json)
                server = sshtunnel.SSHTunnelForwarder(
                    (ssh_json['host'], ssh_json['port']),
                    ssh_username = ssh_json['login'],
                    ssh_password = ssh_json['password'],
                    #ssh_private_key = "id_ed25519",
                    #ssh_host_key = 'vanoing',
                    remote_bind_address = ('localhost', ssh_json['remote_port_bind']),
                    ssh_private_key_password = ssh_json['password'],
                    local_bind_address=(host, tcp_port), 
                    host_pkey_directories=[], 
                    set_keepalive=2.
                    )
                server.start()
                return server
            except: 
                logger(response ='\t404', endpoint=f'\t{ssh_path}', description = '\tFile with ssh config not fount\n', path=log_path)
                return None
        else: 
            return None 
    ssh_connection = ssh_connection_creation(host, tcp_port)
#     ssh_connection.stop()
#    ssh_connection.start() 
    try: 
        client = clickhouse_connect.get_client(host=host, port=tcp_port, username=username, password=password)
        logger(response ='\t200', endpoint=f'\t{host}:{tcp_port}', description = '\tSuccessfull connection to ClickHouse\n', path=log_path)
    except: 
        re_run=1 
        logger(response ='\t404', endpoint=f'\t{host}:{tcp_port}', description = '\tConnection to clickhouse failed miserably.\n', path=log_path)
        client = None 
    return client

#Main function to send requests and log their results. 
def requests_sender(method, url, headers, params='', data_format='json', logging=True):
    """Function to send requests. Args: method, url, params, kwarg = params('' by default)"""
    r = requests.request(method, url, headers = headers, params = params)
    response_code = r.status_code
    endpoint = url.removeprefix('https://api-metrika.yandex.net/management')
    if response_code == 200:
        if data_format == 'json':
            response = r.json()
        else: 
            response = r.text
        success = True
        if logging: 
            logger(response =f'\t{response_code}', endpoint=f'\t{endpoint}', description = '\tSuccess\n', path = log_path)
    else: 
        response = None
        success = False
        if logging:
            logger(response =f'\t{response_code}', endpoint=f'\t{endpoint}', description = '\tNot success\n', path = log_path)
    return response, success
        
            
#Function to check queues of queries to Logs API
def queue_checker(queue, success: bool) -> bool:
    """Function to check current state of queue of Logs API."""
    if success:
        if len(queue['requests']) < 10 :
            return True
        else: 
            return False 
    else:
        return success 
    
    
#Function for decision making whether to make a query of data or not at all: 
def if_to_request(possibility: bool, capacity: bool ) -> bool:
    """Decision-making function to send or not to send request to Logs API. Inputs: possibility(bool), capacity(bool))"""
    if possibility and capacity: 
        return True
    else: 
        re_run = 1
        date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = date_time+f'\t404\t\tFail.Possibility: {possibility}. Capacity: {capacity}\n'
        with open(log_path, "a", encoding="utf-8", newline='\n') as f:
            f.write(log_line)
        return False

    
#To return request_id from logs API creation report as a global variable then. 
def request_identifier(response, success:bool):
    """Function to return request identifier. Args: response, success->bool. Returns request_id"""
    if success: 
        request_id = response.get('log_request').get('request_id')
        return request_id
    
    
#To wait and check status of request in queue: 
def status_checker(url, success, if_run, headers, sleep_time=default_sleep_time):
    """Function, that will check the status of the query in queue and return results if we can go further."""
    if success and if_run: 
        sleep_iters = 1
        while sleep_iters < 15: 
            time.sleep(sleep_time)
            r, state = requests_sender('get', url, headers)
            if state: 
                if r['log_request']['status'] == 'processed':
                    parts_amount = r['log_request']['parts'][-1]['part_number']+1
                    parts = r['log_request']['parts']
                    return parts, parts_amount
                else: 
                    sleep_iters+=1
        re_run = 1
        endpoint = url.removeprefix('https://api-metrika.yandex.net/management')
        date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if state: 
            description = r['log_request']['status']
        else: 
            description = 'No successfull query of queued query at all.'
        log_line = date_time+f'\t{state}\t{endpoint}\t{description}\n'
        with open(log_path, "a", encoding="utf-8", newline='\n') as f:
            f.write(log_line)
        parts = []
        parts_amount = 0
        return parts, parts_amount
    
#Function to write data to clickhouse
def iterational_download_upload(url, parts, parts_amount, headers, api_file_path, is_repeated=False, 
                                indexes_to_repeat=[], method='get'):
    """Function to download data parts from logs api, to save it locally in files. And then - to upload these files to 
    CH server. Args: url, parts, parts_amount, headers, api_file_path, method='get'"""
#     part_no = 0
    parts_to_repeat = []
    for part in parts:
        if is_repeated and (parts.index(part) in indexes_to_repeat) or not(is_repeated):
            pointer = part['part_number']
            current_url=url+f'{pointer}/download'
            data_response, success = requests_sender(method, current_url, headers, data_format='tsv', logging=False)
            if success: 
                try:
                    with open(api_file_path, "w", encoding="utf-8", newline='\n') as ch_file:
                        # Writing data to a file
                        ch_file.write(data_response)
    #             with open(api_file_path, "r", encoding="utf-8", newline='\n') as ch_file:
    #                 row1 = ch_file.readline()
    #                 row2 = ch_file.readline()
                    insert_file(client,database='metrica_mos_data',table='visits',file_path=api_file_path,fmt='TSVWithNames', 
                    settings={'input_format_with_names_use_header': 0, 'input_format_allow_errors_ratio': .3,
                      'input_format_allow_errors_num': 10})
                    # logger(response ='\t200', endpoint=f'\t{api_file_path} part{parts.index(part)}', description = '\tSuccess writing data to CH\n',path=log_path)
                    print(f'Success. Written part: {parts.index(part)} out of {parts_amount-1}')
                except: 
                    print(f'File not written. Part {parts.index(part)} out of {parts_amount-1}')
                    parts_to_repeat.append(parts.index(part))  
                    # logger(response ='\t405', endpoint=f'\t{api_file_path} part{parts.index(part)}', description = '\tFile not written to CH. Pls, repeat\n',path=log_path)
            else: 
                print(f"""Query for {parts.index(part)} wasn't successfull. Try to repeat it.""")
        else: 
            continue
#         part_no+=1
    repeat = (len(parts_to_repeat)>0)
    return parts_to_repeat, repeat
######################
######################
######################
######################



#Loading file with with clickhouse credentials:
ch_credentials = json.loads(file_reader('ch_credentials.json'))

#Auth data
auth_dict= json.loads(file_reader("token_counter.txt"))
token = auth_dict['token']
counter = auth_dict['counter']

#Authorization: https://yandex.ru/dev/metrika/ru/intro/authorization
headers = { 'Authorization': f'OAuth {token}'} ##authorization header

#Setting source of data from Logs API and fields
#Source: https://yandex.ru/dev/metrika/ru/logs/openapi/createLogRequest
fields_n_source = json.loads(file_reader('source_fields.txt'))
fields = fields_n_source['fields']
source = fields_n_source['source']

#Setting last and first dates for data dates range. 
end_date = datetime.date(datetime.now()) - timedelta(days = 1)
begin_date = end_date #- timedelta(days = 1)

end_date = end_date.strftime("%Y-%m-%d")
begin_date = begin_date.strftime("%Y-%m-%d")
# begin_date = '2024-11-17'
# end_date = '2024-11-19'

print(begin_date, '-',  end_date)


#Forming json of parameters for request: 
parameters = {
    'date1': f'{begin_date}', 
    'date2': f'{end_date}', 
    'fields': fields, 
    'source': source
}

# To check whether there is a queue of queries or not at all. 
url = f'https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequests'
queue, is_success =  requests_sender('get', url, headers) 
is_capacity = queue_checker(queue, is_success) #if it's possible to send new query  
print(queue)

# To check if it's possible to execute our query: 
url = f'https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequests/evaluate'
possibility_response, is_success = requests_sender('get', url, params=parameters, headers = headers)
print(possibility_response)

#Final decision: to run or not to run
is_possibility = possibility_response.get('log_request_evaluation', False).get('possible', False)
is_run_request = if_to_request(is_possibility, is_capacity)
print(is_run_request)


#Running or not running query
if is_run_request: 
    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequests'
    parameters['attribution'] = 'last'
    creation_response, is_success = requests_sender('post', url, params = parameters,  headers=headers)
    request_id = request_identifier(creation_response, is_success)
else: 
    re_run=1
    
print(re_run)
print(creation_response)

#Obtaining meta-data to download
url = f'https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}'
parts, parts_amount = status_checker(url, is_success, is_run_request, headers=headers, sleep_time=default_sleep_time)


#Espablishing connection to clickhouse, downloading files in loop from Logs API, then loading it to ClickHouse
if is_success and is_run_request and parts_amount>0:
    client = clickhouse_connector(host= ch_credentials['host'], tcp_port = ch_credentials['port'], 
                                username = ch_credentials['login'], password=ch_credentials['password'])
    if client is not None:
        url = f'https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}/part/'
        i = 0
        parts_to_repeat = []
        to_repeat = False 
        while i<3: 
            parts_to_repeat, to_repeat = iterational_download_upload(url, parts, parts_amount, headers, api_file_path,
                                                                     is_repeated = to_repeat, indexes_to_repeat = parts_to_repeat)
            if not(to_repeat): 
                break
            i+=1
    else: 
        print('Connection to ClickHouse not established. Next time will be better (probably)')
        re_run = 1
        parts_to_repeat = []
        to_repeat = False 

#Kill request 
#
if is_success and is_run_request: 
    url = f'https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}/cancel'
    cancel_repsonse, cancel_success = requests_sender('post', url, headers, params='', data_format='text')
    if not(cancel_success):
        url = f'https://api-metrika.yandex.net/management/v1/counter/{counter}/logrequest/{request_id}/clean'
        cancel_repsonse2, cancel_success2 = requests_sender('post', url, headers, params='', data_format='text')
#https://api-metrika.yandex.net/management/v1/counter/{counterId}/logrequest/{requestId}/cancel

print(re_run)
print(is_success)
print(is_run_request)
if is_success and is_run_request and parts_amount>0: 
    print(f'Parts to repeat: {parts_to_repeat}')
    print(f'If to repeat: {to_repeat}')
    logger(response =f'\t200', endpoint=f'\tFinish', description = f'\tSuccessfully written {parts_amount-len(parts_to_repeat)+1} parts \
out of {parts_amount+1}.Unwritten parts: {parts_to_repeat}\n', path = log_path)
else: 
    print('To repeat: all')
    logger(response =f'\t404', endpoint=f'\tFinish', description = f'Nothing written.To repeat:{re_run}. \
Creation query sent: {is_success}. It was decided to create a logrequest (queue is empty): {is_run_request}\n', path = log_path)