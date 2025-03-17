from datetime import datetime
from datetime import timedelta
from .routines_utils import *
from .logger import Logger
from .database_utils import ClickHouseConnector
from .api_methods import * 
import time


class MainFlowWrapper: 

    SUCCESS_CODE = 200 
    DOWNLOAD_API_OPERATION_DEFAULT_ENDPOINT = '/download'
    LOG_TABLE_FIELDS = ['datetime', 'response', 'endpoint', 'description']
    DEFAULT_ERROR_CODE = 500
    DEFAULT_REQUEST_SLEEP = 1
    DEFAULT_API_QUERY_RETRIES = 3
    BAD_STATUS_CODES = ['canceled', 'cleaned_by_user', 'cleaned_automatically_as_too_old', 'processing_failed', 'awaiting_retry']
    
    def __init__(self, ch_credentials, api_settings, global_settings, queries, utilset):
        self.ch_credentials = ch_credentials
        self.api_settings = api_settings
        self.global_settings = global_settings
        self.utilset = utilset
        self.logger = Logger(self.global_settings.get('log_continuous_path'), self.global_settings.get('log_last_run_path'))
        self.data_path = self.global_settings.get('temporary_data_path', '')
        self.frequency = self.global_settings.get('frequency_api_status_check_sec')
        self.status_timeout = self.global_settings.get('api_status_wait_timeout_min')*60
        self.queries = queries
        self.counterId = self.api_settings.get('counter')
        self.token = self.api_settings.get('token')
        self.params = api_settings.copy()
        self.params.pop('token', None)
        self.params.pop('counter', None)
        #Let's call connection establishing from the start. 
        self.establish_db_connections()
        self.is_log_table = False
        self.check_db_tables()
        self.dates_parameters_normalization()
        self.files = []

    def dates_parameters_normalization(self): 
        if not self.params.get('date1') and not self.params.get('date2'):
            start_date = datetime.date(datetime.now()) - timedelta(days = 1)
            start_date = start_date.strftime("%Y-%m-%d")
            self.params['date1'] = start_date
            self.params['date2'] = start_date
        elif not self.params.get('date1') and self.params.get('date2'):
            self.params['date1'] = self.params.get('date2')

        if not self.params.get('date2'): 
            end_date = datetime.date(datetime.now()) - timedelta(days = 1)
            end_date = end_date.strftime("%Y-%m-%d")
            self.params['date2'] = end_date
        print(f"Date range for request: since {self.params.get('date1')} till {self.params.get('date2')}")

    def establish_db_connections(self): 
        login = self.ch_credentials.get('login')
        password = self.ch_credentials.get('password')
        host = self.ch_credentials.get('host')
        port = self.ch_credentials.get('port')
        db = self.ch_credentials.get('db')
        table = self.ch_credentials.get('table')
        log_table = self.ch_credentials.get('logTable')
        ssh = self.ch_credentials.get('ssh')
        self.ch = ClickHouseConnector(self.logger, login, password, host, port, db, table, log_table, ssh)
        if not self.ch : 
            raise ConnectionError("Connection to database wasn't established. Please, check credentials and re-run the script.")
        return self
    
    def check_db_tables(self): 
        if self.global_settings.get('run_db_test'): 
            #Getting api fields from config
            api_fields = self.api_settings.get('fields').split(',')

            ch_dbs = self.ch.query_data(self.queries['db_query'], parameters = self.ch_credentials)
            ch_table = self.ch.query_data(self.queries['table_query'], parameters = self.ch_credentials)
            ch_columns = self.ch.query_data(self.queries['columns_query'], parameters = self.ch_credentials)
            #Check if there is data table and there are data columns and their amount is less or equal to fields in api config. 
            if ch_dbs is not None: 
                if len(ch_dbs) > 0:
                    if len(ch_table) > 0:
                        if len(ch_columns) > 0:
                            ch_cols_list = [col[0] for col in ch_columns]
                            if len(api_fields) > len(ch_cols_list):
                                self.logger.add_to_log(self.__class__.DEFAULT_ERROR_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('table')}", 
                                                    f"Table {self.ch_credentials.get('table')} has less columns, than API request.")
                                if not self.global_settings.get('continue_on_columns_test_fail'): 
                                    raise DatabaseException("Sorry, you want to download more fields, then there are in your table.")
                                else: 
                                    print(f"Table {self.ch_credentials.get('table')} of database: {self.ch_credentials.get('db')} didn't pass colymns test (more columns in API request than in table). \
                                          \n But continue_on_columns_test_fail parameter in global_config allows us to continue.")
                            else: 
                                print(f"Table {self.ch_credentials.get('table')} of database: {self.ch_credentials.get('db')} passed shallow test successfully.")
                    else:
                        raise DatabaseException("Table doesn't exist.")
                else:
                    raise DatabaseException("Database doesn't exist.")
            else: 
                raise DatabaseException("Query wasn't perform. Probably, not enough rights to perform SELECT query.")

        if self.global_settings.get('run_log_table_test') and self.ch_credentials.get('logTable'):
            #Check if log table exists and if columns of log table are those should be. 
            ch_log_table = self.ch.query_data(self.queries.get('log_table_query'), parameters = self.ch_credentials)
            ch_log_table_columns = self.ch.query_data(self.queries['log_table_query_columns'], parameters = self.ch_credentials)
            ch_log_columns = [col[0] for col in ch_log_table_columns] if ch_log_table_columns is not None else []
            if not(ch_log_table is None or ch_log_columns is None): 
                if (len(ch_log_table) == 0 or ch_log_columns != self.__class__.LOG_TABLE_FIELDS) and self.global_settings.get('create_log_table_on_fail'):
                    self.is_log_table = self.ch.create_table(self.queries['log_table_create'], self.ch_credentials.get('logTable'))
                    if not(self.is_log_table or self.global_settings.get('continue_on_log_table_creation_fail')): 
                        raise DatabaseException(f"Sorry. Log Table didn't pass a check and a new one called {self.ch_credentials.get('logTable')} couldn't be created.")
                elif len(ch_log_table) > 0 and ch_log_columns == self.__class__.LOG_TABLE_FIELDS: 
                    self.is_log_table = True
                    print(f"Log table {self.ch_credentials.get('logTable')} of database {self.ch_credentials.get('db')} successfully passed shallow check.")
            else:
                print(f"Query of {self.ch_credentials.get('logTable')} wasn't successfull")
                if not(self.global_settings.get('continue_on_log_table_creation_fail')): 
                    raise DatabaseException(f"Table {self.ch_credentials.get('logTable')} or its columns weren't queried. Probably, not enough rights or other query issue")
                
    def check_log_evaluation(self): 
        self.log_evaluation = LogEvaluation(self.counterId, self.token, self.logger, self.params)
        self.log_evaluation.send_request()
        if not self.log_evaluation.is_success:
            if not self.global_settings.get('clear_api_queue'): 
                raise FlowException(f"Request cannot be performed and you didn't allow to clear requests queue.\n See: clear_api_queue parameter in global_config.json")
            else: 
                time.sleep(self.__class__.DEFAULT_REQUEST_SLEEP)
                log_list = LogList(self.counterId, self.token, self.logger)
                log_list.send_request()
                if log_list.response_code == self.__class__.SUCCESS_CODE and len(log_list.response_body) > 0: 
                    requests_deleted = 0
                    for request in log_list.response_body:
                        time.sleep(self.__class__.DEFAULT_REQUEST_SLEEP)
                        request_status = request.get('status')
                        request_id = request.get('request_id')
                        if request_status == 'created':
                            clear_old_request = CleanPendingLog(self.counterId, request_id, self.token, self.logger)
                        else:
                            clear_old_request = CleanProcessedLog(self.counterId, request_id, self.token, self.logger)
                        clear_old_request.send_request()
                        if clear_old_request.is_success: 
                            requests_deleted += 1
                        del clear_old_request
                    print(f"Deleted {requests_deleted} requests in queue.")
                    self.log_evaluation.send_request()
                    if not(self.log_evaluation.is_success): 
                        raise FlowException("The queue was cleared, but your request cannot be performed anyway.\n Please, make date range smaller or reduce params amount.")
                    else: 
                        print(f"Evaluation results: {self.log_evaluation.is_success}")
                else: 
                    raise FlowException(f"The queue is empty, but your request cannot be performed anyway.\n Please, make date range smaller or reduce params amount.")
        else: 
            print(f"Evaluation results: {self.log_evaluation.is_success}")
        return self
    
    def create_log_request(self, repeat = 0): 
        time.sleep(self.__class__.DEFAULT_REQUEST_SLEEP)
        if self.log_evaluation.is_success: 
            self.log_request = CreateLog(self.counterId, self.token, self.logger, params=self.params)
            self.log_request.send_request()
            if self.log_request.is_success: 
                self.request_id = self.log_request.request_id
                print(f"Log request with id: {self.request_id} was successfully created.")
                return self
            elif not(self.log_request.is_success) and repeat < self.__class__.DEFAULT_API_QUERY_RETRIES: 
                repeat+=1
                return self.create_log_request(repeat)
            else: 
                raise FlowException("Log creation request cannot be created for some reason. Please, try later.")
        else: 
            raise FlowException("The request wasn't evaluated or cannot be evaluated. Please, check sequence of methods calls, reduce dates range or reduce params amount.")
        
    def delete_log(self, repeat = 0):
        try: 
            del self.log_evaluation
            del self.log_request
            del self.status_request
            del self.download_log_part
        finally: 
            time.sleep(self.__class__.DEFAULT_REQUEST_SLEEP)
            self.deletion = CleanPendingLog(self.counterId, self.request_id, self.token, self.logger)
            self.deletion.send_request()
            if not(self.deletion.is_success): 
                self.deletion = CleanProcessedLog(self.counterId, self.request_id, self.token, self.logger)
                self.deletion.send_request()
            if self.deletion.is_success:
                print(f"Deletion of request {self.request_id} was {self.deletion.is_success}")
                del self.deletion
                return self 
            elif not(self.deletion.is_success) and repeat < self.__class__.DEFAULT_API_QUERY_RETRIES: 
                repeat+=1
                return self.delete_log(repeat)
            else: 
                print(f"Deletion of request {self.request_id} wasn't performed")
        
    def log_status_check(self, repeat=0):
        if self.log_request.is_success:
            if self.frequency*repeat <= self.status_timeout: 
                time.sleep(self.frequency)
                self.status_request = StatusLog(self.counterId, self.request_id, self.token, self.logger)
                self.status_request.send_request()
                print(f"Status check  of request {self.request_id}. Done times: {repeat+1}. Time: {(repeat+1)*self.frequency} sec. Status: {self.status_request.status}. Response code: {self.status_request.response_code}.\
                      \n Max wait time left: {(self.status_timeout - (repeat+1)*self.frequency)//60} mins.")
                if self.status_request.is_success: 
                    self.parts = self.status_request.parts
                    self.parts_amount = self.status_request.parts_amount
                    print(f"Status check of request {self.request_id} got posititve results. Status: {self.status_request.status}. Time spent on waiting: {(repeat+1)*self.frequency} secs.")
                    return self
                elif self.status_request.response_code == self.__class__.SUCCESS_CODE: 
                    if self.status_request.status in self.__class__.BAD_STATUS_CODES:
                        self.delete_log()
                        raise FlowException(f"Log wasn't processed well for some reason. It had status: {self.status_request.status}.")
                    else:
                        repeat+=1 
                        return self.log_status_check(repeat)
                elif self.status_request.response_code is not None:
                    if repeat > 3: 
                        raise FlowException(f"Endpoint of status query {self.status_request.url} is unreachable.") #Well, just not to wait half an hour just for incorrect requests.
                    repeat+=1
                    return self.log_status_check(repeat)
                else: 
                    raise FlowException(f"Endpoint of status query {self.status_request.url} is unreachable.")
            else: 
                raise FlowException(f"Log wasn't cooked for timeout time: {self.status_timeout/60} mins.")
            
        else: 
            raise FlowException("The request wasn't created. Please, check sequence of methods calls.")
        
    def log_downloader(self, repeat=0):
        if self.status_request.is_success: 
            if self.parts_amount > 0:
                self.download_log_part = DownloadLogPart(self.counterId, self.request_id, self.token, self.logger)
                for part in self.parts: 
                    time.sleep(self.__class__.DEFAULT_REQUEST_SLEEP)
                    self.download_log_part.send_request(part)
                    if self.download_log_part.is_success:
                        dt = datetime.now()
                        dt = dt.strftime("%Y-%m-%d-%H-%M-%S")
                        full_file = self.data_path + dt + '-'+ str(self.counterId) + '-' +str(self.api_settings.get('source')) + '-' + f"part{part}.tsv"
                        try: 
                            self.utilset.rewrite_file(self.download_log_part.response_body, full_file)
                            self.files.append(full_file)
                            self.parts.remove(part)
                        except(OSError, IOError):
                            continue
                
                description = f"Parts downloaded successfully: {self.parts_amount-len(self.parts)}. Parts not downloaded: {self.parts}."
                endpoint = self.__class__.DOWNLOAD_API_OPERATION_DEFAULT_ENDPOINT
                if len(self.parts) == 0 or (repeat == self.__class__.DEFAULT_API_QUERY_RETRIES and (len(self.parts)/self.parts_amount <= self.global_settings.get('data_loss_tolerance_perc',0)/100)): 
                    self.logger.add_to_log(response=self.__class__.SUCCESS_CODE, endpoint=endpoint, description=description).write_to_disk_incremental()
                    print(description)
                    return self
                elif repeat < self.__class__.DEFAULT_API_QUERY_RETRIES: 
                    repeat += 1
                    return self.log_downloader(repeat)
                else:
                    self.logger.add_to_log(response=self.__class__.DEFAULT_ERROR_CODE, endpoint=endpoint, description=description).write_to_disk_incremental()
                    for file in self.files:
                        self.utilset.delete_file(file)
                    raise  FlowException(f"Error of downloading data. Downloaded: {self.parts_amount - len(self.parts)} files.\n \
                                         That's {round(len(self.parts)/self.parts_amount*100, 2)} percent of total data.\n \
                                         Allowed tolerance is: {self.global_settings.get('data_loss_tolerance_perc',0)} percent.\n \
                                         All files were deleted. Re-run script instead")
            else: 
                print("Nothing to download")

        def write_data_to_db(self):
            pass

    



        
    

        
    
    

        


    


                
            

        

            






                


