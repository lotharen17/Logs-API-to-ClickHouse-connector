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

