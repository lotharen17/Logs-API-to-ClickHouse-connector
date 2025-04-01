from datetime import datetime
from datetime import timedelta
from .routines_utils import *
from .logger import Logger
from .database_utils import ClickHouseConnector
from .api_methods import * 
import time


class MainFlowWrapper: 
    """The class to create a flow of the programm. 

    Arguments:
        ch_credentials - dict, contains credentials for clickhouse from file configs/ch_credentials.json
        api_settings - dict, containts settings to perform api request from file configs/api_credentials.json 
        global_settings - dict, contains global config from file configs/global_config.json
        queries - dict, containts query types in keys and queries in values. 
        utilset - instance of Utilset class, to use utilities via aggregation. 
    
    Constants: 
        DEFAULT_SUCCESS_CODE - int, to write in log as default success code (used http codes even for non-networking operations). 
        DEFAULT_ERROR_CODE  - int, to write in log as default errror code (well, internal server error is chosen by default, but it has nothing to do with http in most cases here). 
        DOWNLOAD_API_OPERATION_DEFAULT_ENDPOINT - str, just to name endpoint for downloading operation in log. 
        LOAD_TO_DB_OPERATION_DEFAULT_ENDPOINT - str, just to name endpoint for loading data to database operation. 
        FINISH_OPERATION_DEFAULT_ENDPOINT - str, just to name endpoint for program finish.  
        LOG_TABLE_FIELDS - list of strings. List of strings of the log table headers, to create a log table. 
        DEFAULT_REQUEST_SLEEP - float, value to separate requests in time to meet quota of Logs API: https://yandex.com/dev/metrika/en/intro/quotas.
        DEFAULT_API_QUERY_RETRIES - int, value to re-try API queries and other operations to perform in case of not-successfull results. Log evaluation is exclusion and will be performed only once. 
        BAD_STATUS_CODES - list of str, statuses mean logs api data cannot be extracted. Source: https://yandex.com/dev/metrika/en/logs/openapi/getLogRequest#logrequest

    Properties: 
        ch_credentials - :dict with clickhouse credentials from ch_credentials.json config file. 
        api_settings - :dict with Logs API settings and auth credentials to properly access Logs API methods from file api_credentials.json
        global_settings - :dict with global settings from global_config.json file. 
        utilset  - :inst of class UtilsSet. Associated utilset for the MainFlowWrapper class' instance. Aggregational alias of globally existing utilset instance. 
        logger - :inst of class Logger. Logger for script flow. Example of composition, as it's created as a part of MainFlowWrapper class' instance. 
        data_path - :str, parsed string of directory to store downloaded datafiles. By default - root directory of the script (where main.py is located). 
        frequency - :int, seconds to check status of created log praparation process. Parsed from frequency_api_status_check_sec parameter of global_config.json. 
        status_timeout - :int, minutes to wait until timeout will be declared exceeded and script will be finished with an error. Taken from api_status_wait_timeout_min of global_config.json.
        queries - :dict. Dictionary with queries to perform database and tables checks. 
        counterId - :int. Id of counter of Yandex.Metrika. Parsed from api_settings dict. 
        token - :str. Parsed from api_settings dict. Authentication token.
        params - :dict. Params dictionary, copy of api_settings without token and counter. 
        is_log_table - :bool. Flag of successfull existance of log table (table to write a log of this script). 
        files - :list of str. List of files downloaded locally. 
        ch - :inst of class ClickHouseConnector. Compositional instance of class. More in api_methods.py module. 
        log_evaluation :inst of class LogEvaluation. Compostitional instance of class. More in api_mehods.py module. 
        log_request :inst of class LogRequest. Composititonal instance of class. More in api_mehods.py module.
        deletion :inst of either class CleanPendingLog or CleanProcessedLog deletes other instances of other API Logs classes. Composititonal instance of class. More in api_mehods.py module.
        status_request :inst of class StatusLog. Composititonal instance of class. More in api_mehods.py module.
        parts - :list of ints. List of parts to download. Obtained from status_request. 
        parts_amount - :int. Parts to download. Obtained from status_request. 
        download_log_part - :inst of class DownloadLogPart. Composititonal instance of class. More in api_mehods.py module.

    Methods: 
        dates_parameters_normalization(self) - creates or transforms start and end dates if abscent. Runs on init. 
        establish_db_connections(self)  - creates (if needed) ssh tunnel and db connection. Currently only login + password auth for ssh is working and only http protocol for db. Runs on init. Returns self.
        check_db_tables(self) - if there is parameter run_db_table_test=true in global log - checks if db table from ch credentials exists. The same for log table and run_log_table_test param. Runs on init. Returns self.
        check_log_evaluation(self) - safely creates, sends and then checks Logs API log evaluation possibility request. If fails: raises FlowException error. Returns self.
        create_log_request(self,repeat=0) - safely creates and checks Logs API log creation request. If fails, will be repeated DEFAULT_API_QUERY_RETRIES times. Returns self.
        delete_log(self,repeat=0) - safely deletes all the instances of Logs requests objects and deletes either Log in processing or processed Log.  If fails, will be repeated DEFAULT_API_QUERY_RETRIES times. Returns self.
        log_status_check(self,repeat=0) - safely checks if Logs API log request is prepared or not. If yes, checks it's status and can either delete it or continue script execution. Returns self. 
        log_downloader(self, repeat=0) - safely iterationally downloads and saves Logs API data to the local directory specified in temporary_data_path param of global_config. Repeats if fails. Returns self. 
        write_data_to_db(self, repeat=0, file_list=None) - file_list: list of str, None - if not None, determines files to load. Safely iterationally loads localy saved tsv data files of Logs API to clickhouse table. 
                        Repeats with file_list to repeat if fails. Calls delete_files method to delete successfully loaded temporary data. Returns self. 
        delete_files(self, exclusion_list=None) - safely tries to delete all the downloaded data files. exclusion_list :list of str determines files to exclude from deletion. Returns self. 
        write_log_to_db(self) - safely tries to load service log (log of the script run) to the table determined by logTable parameter of ch_credentials. Returns self. 
        final_log_record(self, success=False) - sucess: bool, False by default. Creates the final log record with /finish endpoint, just to parse then easily to find out needed script run results. 
        close_and_finish(self) - writes the last record of the service log(log of the script run), saves log to db and locally (last run log) and closes connections with successfull message. Returns self.  
    """

    DEFAULT_SUCCESS_CODE = 200 
    DEFAULT_ERROR_CODE = 500

    DOWNLOAD_API_OPERATION_DEFAULT_ENDPOINT = '/download'
    LOAD_TO_DB_OPERATION_DEFAULT_ENDPOINT = '/ch_load'
    FINISH_OPERATION_DEFAULT_ENDPOINT = '/finish'

    LOG_TABLE_FIELDS = ['datetime', 'response', 'endpoint', 'description']

    DEFAULT_REQUEST_SLEEP = 0.5
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
        """Method to fill in dates in case of their abscence."""
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
        return self

    def establish_db_connections(self): 
        """Method to establish connections with database. And, optionally, SSH tunnel."""
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
        """Method to check data table (in case there is run_db_table_test parameter set), 
        log table (in case both logTable in ch_credentials and run_log_table_test in global_config are set).

        In case if continue_on_columns_test_fail set false in global_config and columns test will fail, script will throw an exception. 
        The same will happen in case if continue_on_log_table_creation_fail is set false and log table wasn't created."""
        #Let's check data table shallowly if test parameter was set to true: 
        if self.global_settings.get('run_db_table_test'): 
            #Getting api fields from config
            api_fields = self.api_settings.get('fields').split(',')
            #Let's perform some queries:
            ch_dbs = self.ch.query_data(self.queries['db_query'], parameters = self.ch_credentials)
            ch_table = self.ch.query_data(self.queries['table_query'], parameters = self.ch_credentials)
            ch_columns = self.ch.query_data(self.queries['columns_query'], parameters = self.ch_credentials)

            #Check if there is data table and there are data columns and their amount is less or equal to fields in api config. 
            if ch_dbs is not None: 
                if len(ch_dbs) > 0:
                    if len(ch_table) > 0:
                        if len(ch_columns) > 0:
                            ch_cols_list = [col[0] for col in ch_columns]
                            if len(api_fields) != len(ch_cols_list):
                                if not self.global_settings.get('continue_on_columns_test_fail'): 
                                    self.logger.add_to_log(self.__class__.DEFAULT_ERROR_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('table')}", 
                                                    f"Table {self.ch_credentials.get('table')} has less columns, than API request.")
                                    self.logger.write_to_disk_incremental()
                                    self.final_log_record()
                                    self.logger.write_to_disk_last_run()
                                    raise DatabaseException("Sorry, you want to download different amount of fields, then there are in your table.")
                                else: 
                                    self.logger.add_to_log(self.__class__.DEFAULT_ERROR_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('table')}", 
                                                    f"Table {self.ch_credentials.get('table')} has less columns, than API request, but global config params allow to continue.")
                                    self.logger.write_to_disk_incremental()
                                    print(f"Table {self.ch_credentials.get('table')} of database: {self.ch_credentials.get('db')} didn't pass colymns test (more columns in API request than in table). \
                                          \n But continue_on_columns_test_fail parameter in global_config allows us to continue.")
                            else: 
                                self.logger.add_to_log(self.__class__.DEFAULT_SUCCESS_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('table')}", 
                                                    f"Table {self.ch_credentials.get('table')} passed shallow test successfully.")
                                self.logger.write_to_disk_incremental()
                                print(f"Table {self.ch_credentials.get('table')} of database: {self.ch_credentials.get('db')} passed shallow test successfully.")
                    else:
                        self.logger.add_to_log(self.__class__.DEFAULT_ERROR_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('table')}", 
                                                    f"Table doesn't exist")
                        self.logger.write_to_disk_incremental()
                        self.final_log_record()
                        self.logger.write_to_disk_last_run()
                        raise DatabaseException("Table doesn't exist.")
                else:
                    self.logger.add_to_log(self.__class__.DEFAULT_ERROR_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('table')}", 
                                                    f"Database doesn't exist")
                    self.logger.write_to_disk_incremental()
                    self.final_log_record()
                    self.logger.write_to_disk_last_run()
                    raise DatabaseException("Database doesn't exist.")
            else: 
                self.logger.add_to_log(self.__class__.DEFAULT_ERROR_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('table')}", 
                                                    f"Query wasn't performed properly.")
                self.logger.write_to_disk_incremental()
                self.final_log_record()
                self.logger.write_to_disk_last_run()
                raise DatabaseException("Query wasn't performed. Probably, not enough rights to perform SELECT query.")
            
        #Checking logTable now:
        if self.global_settings.get('run_log_table_test') and self.ch_credentials.get('logTable') and isinstance(self.ch_credentials.get('logTable'), str):
            #Check if log table exists and if columns of log table are those should be. 
            ch_log_table = self.ch.query_data(self.queries.get('log_table_query'), parameters = self.ch_credentials)
            ch_log_table_columns = self.ch.query_data(self.queries['log_table_query_columns'], parameters = self.ch_credentials)
            ch_log_columns = [col[0] for col in ch_log_table_columns] if ch_log_table_columns is not None else []
            if not(ch_log_table is None or ch_log_columns is None): 
                if (len(ch_log_table) == 0 or ch_log_columns != self.__class__.LOG_TABLE_FIELDS) and self.global_settings.get('create_log_table_on_fail'):
                    self.is_log_table = self.ch.create_table(self.queries['log_table_create'], self.ch_credentials.get('logTable'))
                    if not(self.is_log_table or self.global_settings.get('continue_on_log_table_creation_fail')): 
                        self.logger.add_to_log(self.__class__.DEFAULT_ERROR_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('logTable')}", 
                                                f"Table doesn't exist and couldn't be created.")
                        self.logger.write_to_disk_incremental()
                        self.final_log_record()
                        self.logger.write_to_disk_last_run()
                        raise DatabaseException(f"Sorry. Log Table didn't pass a check and a new one called {self.ch_credentials.get('logTable')} couldn't be created.")
                elif len(ch_log_table) > 0 and ch_log_columns == self.__class__.LOG_TABLE_FIELDS: 
                    self.is_log_table = True
                    self.logger.add_to_log(self.__class__.DEFAULT_SUCCESS_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('logTable')}", 
                                            f"Table {self.ch_credentials.get('logTable')} successfully passed shallow check.")
                    self.logger.write_to_disk_incremental()
                    print(f"Log table {self.ch_credentials.get('logTable')} of database {self.ch_credentials.get('db')} successfully passed shallow check.")
            else:
                print(f"Query of {self.ch_credentials.get('logTable')} wasn't successfull")
                self.logger.add_to_log(self.__class__.DEFAULT_ERROR_CODE, f"Database: {self.ch_credentials.get('db')}. Table: {self.ch_credentials.get('logTable')}", 
                                            f"Table {self.ch_credentials.get('logTable')} or its columns weren't queried. Probably, not enough rights or other query issue")
                self.logger.write_to_disk_incremental()
                if not(self.global_settings.get('continue_on_log_table_creation_fail')): 
                    self.final_log_record()
                    self.logger.write_to_disk_last_run()
                    raise DatabaseException(f"Table {self.ch_credentials.get('logTable')} or its columns weren't queried. Probably, not enough rights or other query issue")
                
    def check_log_evaluation(self): 
        self.log_evaluation = LogEvaluation(self.counterId, self.token, self.logger, self.params)
        self.log_evaluation.send_request()
        if not self.log_evaluation.is_success:
            if not self.global_settings.get('clear_api_queue'):
                self.final_log_record()
                self.logger.write_to_disk_last_run()
                self.write_log_to_db()
                raise FlowException(f"Request cannot be performed and you didn't allow to clear requests queue.\n See: clear_api_queue parameter in global_config.json")
            else: 
                time.sleep(self.__class__.DEFAULT_REQUEST_SLEEP)
                log_list = LogList(self.counterId, self.token, self.logger)
                log_list.send_request()
                if log_list.response_code == self.__class__.DEFAULT_SUCCESS_CODE and len(log_list.response_body) > 0: 
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
                        self.final_log_record()
                        self.logger.write_to_disk_last_run()
                        self.write_log_to_db()
                        raise FlowException("The queue was cleared, but your request cannot be performed anyway.\n Please, make date range smaller or reduce params amount.")
                    else: 
                        print(f"Evaluation success: {self.log_evaluation.is_success}")
                else: 
                    self.final_log_record()
                    self.logger.write_to_disk_last_run()
                    self.write_log_to_db()
                    raise FlowException(f"The queue is empty, but your request cannot be performed anyway.\n Please, make date range smaller or reduce params amount.")
        else: 
            print(f"Evaluation sucess: {self.log_evaluation.is_success}")
        return self
    
    def create_log_request(self, repeat = 0): 
        """Method to create request to download Logs API data for Logs API endpoint."""
        time.sleep(self.__class__.DEFAULT_REQUEST_SLEEP)
        if self.log_evaluation.is_success: 
            self.log_request = CreateLog(self.counterId, self.token, self.logger, params=self.params)
            self.log_request.send_request()
            if self.log_request.is_success: 
                self.request_id = self.log_request.request_id
                print(f"Log request with id: {self.request_id} was successfully created.")
                return self
            elif not(self.log_request.is_success) and repeat < (self.__class__.DEFAULT_API_QUERY_RETRIES - 1): 
                repeat+=1
                return self.create_log_request(repeat)
            else: 
                self.final_log_record()
                self.logger.write_to_disk_last_run()
                self.write_log_to_db()
                raise FlowException("Log creation request cannot be created for some reason. Please, try later.")
        else: 
            self.final_log_record()
            self.logger.write_to_disk_last_run()
            self.write_log_to_db()
            raise FlowException("The request wasn't evaluated or cannot be evaluated. Please, check sequence of methods calls, reduce dates range or reduce params amount.")
        
    def delete_log(self, repeat = 0):
        """Method to perform deletion of Logs API request_id and prepared or pending log."""
        try: 
            del self.log_evaluation
            del self.log_request
            del self.status_request
            del self.download_log_part
        finally:
            if self.global_settings.get('clear_created_logs_request'):
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
                elif not(self.deletion.is_success) and repeat < (self.__class__.DEFAULT_API_QUERY_RETRIES - 1): 
                    repeat+=1
                    return self.delete_log(repeat)
                else: 
                    print(f"Deletion of request {self.request_id} wasn't performed for unexpected reason.")
            else:
                print(f"Deletion of {self.request_id} wasn't performed according to global config.")
        
    def log_status_check(self, repeat=0):
        """Method to check status of created Logs API data log."""
        if self.log_request.is_success:
            if self.frequency*(repeat+1) <= self.status_timeout: 
                time.sleep(self.frequency)
                self.status_request = StatusLog(self.counterId, self.request_id, self.token, self.logger)
                self.status_request.send_request()
                print(f"Status check  of request {self.request_id}. Done times: {repeat+1}. Time: {(repeat+1)*self.frequency} sec. Status: {self.status_request.status}. Response code: {self.status_request.response_code}.\
                      \n Max wait time left: {(self.status_timeout - (repeat+1)*self.frequency)//60} mins.")
                if self.status_request.is_success: 
                    self.parts = self.status_request.parts
                    self.parts_amount = self.status_request.parts_amount
                    print(f"Status check of request {self.request_id} got posititve results. Status: {self.status_request.status}. Parts to download: {self.parts_amount}. Time spent on waiting: {(repeat+1)*self.frequency} secs.")
                    return self
                elif self.status_request.response_code == self.__class__.DEFAULT_SUCCESS_CODE: 
                    if self.status_request.status in self.__class__.BAD_STATUS_CODES:
                        self.delete_log()
                        self.final_log_record()
                        self.logger.write_to_disk_last_run()
                        self.write_log_to_db()
                        raise FlowException(f"Log wasn't processed well for some reason. It had status: {self.status_request.status}.")
                    else:
                        repeat+=1 
                        return self.log_status_check(repeat)
                elif self.status_request.response_code is not None:
                    if repeat > self.__class__.DEFAULT_API_QUERY_RETRIES:
                        self.delete_log()
                        self.final_log_record()
                        self.logger.write_to_disk_last_run()
                        self.write_log_to_db()
                        raise FlowException(f"Endpoint of status query {self.status_request.url} is unreachable.") #Well, just not to wait half an hour just for incorrect requests.
                    repeat+=1
                    return self.log_status_check(repeat)
                else: 
                    self.delete_log()
                    self.final_log_record()
                    self.logger.write_to_disk_last_run()
                    self.write_log_to_db()
                    raise FlowException(f"Endpoint of status query {self.status_request.url} is unreachable.")
            else: 
                self.delete_log()
                self.final_log_record()
                self.logger.write_to_disk_last_run()
                self.write_log_to_db()
                raise FlowException(f"Log wasn't cooked for timeout time: {self.status_timeout/60} mins.")
        else: 
            self.final_log_record()
            self.logger.write_to_disk_last_run()
            self.write_log_to_db()
            raise FlowException("The request wasn't created. Please, check sequence of methods calls.")
        
    def log_downloader(self, repeat=0):
        """Method to download Logs API prepared data."""
        if self.status_request.is_success: 
            if self.parts_amount > 0:
                self.download_log_part = DownloadLogPart(self.counterId, self.request_id, self.token, self.logger)
                re_download_list = []
                for part in self.parts: 
                    time.sleep(self.__class__.DEFAULT_REQUEST_SLEEP+repeat)
                    self.download_log_part.send_request(part)
                    if self.download_log_part.is_success:
                        dt = datetime.now()
                        dt = dt.strftime("%Y-%m-%d-%H-%M-%S")
                        full_file = self.data_path + dt + '-'+ str(self.counterId) + '-' +str(self.api_settings.get('source')) + '-' + f"part{part}.tsv"
                        try: 
                            self.utilset.rewrite_file(self.download_log_part.response_body, full_file)
                            self.files.append(full_file)
                        except(OSError, IOError):
                            re_download_list.append(part)
                    else: 
                        re_download_list.append(part)
                
                self.parts = re_download_list
                description = f"Parts downloaded successfully: {self.parts_amount-len(self.parts)}. Parts not downloaded: {self.parts}."
                endpoint = self.__class__.DOWNLOAD_API_OPERATION_DEFAULT_ENDPOINT
                if len(self.parts) == 0 or (repeat == (self.__class__.DEFAULT_API_QUERY_RETRIES - 1) and (len(self.parts)/self.parts_amount <= self.global_settings.get('data_loss_tolerance_perc',0)/100)): 
                    self.logger.add_to_log(response=self.__class__.DEFAULT_SUCCESS_CODE, endpoint=endpoint, description=description).write_to_disk_incremental()
                    print(description)
                    self.delete_log()
                    return self
                elif repeat < (self.__class__.DEFAULT_API_QUERY_RETRIES - 1): 
                    repeat += 1
                    return self.log_downloader(repeat)
                else:
                    self.logger.add_to_log(response=self.__class__.DEFAULT_ERROR_CODE, endpoint=endpoint, description=description).write_to_disk_incremental()
                    self.delete_files()
                    self.delete_log()
                    self.final_log_record()
                    self.logger.write_to_disk_last_run()
                    self.write_log_to_db()
                    raise  FlowException(f"Error of downloading data. Request ID: {self.request_id}. Downloaded: {self.parts_amount - len(self.parts)} files.\n \
                                         That's {round(len(self.parts)/self.parts_amount*100, 2)} percent of total data.\n \
                                         Allowed tolerance is: {self.global_settings.get('data_loss_tolerance_perc',0)} percent.\n \
                                         All files were deleted. Re-run script instead")
            else: 
                print("Nothing to download")

    def write_data_to_db(self, repeat=0, file_list=None):
        """Method to load previously downloaded data files to database. Deletes downloaded and successfully uploaded to db files."""
        settings = {"input_format_allow_errors_ratio": self.global_settings.get('bad_data_tolerance_perc', 0)/100,
                    "input_format_allow_errors_num": self.global_settings.get('absolute_db_format_errors_tolerance', 0), 
                    "input_format_with_names_use_header": self.global_settings.get('api_strict_db_table_cols_names')
        }
        failed_loads = []
        if file_list is None: 
            file_list = self.files
        
        for file in file_list:
                result = self.ch.insert_datafile(file, settings)
                if not result: 
                    failed_loads.append(file)

        description = f"Parts loaded into db successfully: {len(self.files) - len(failed_loads)}. Files not loaded: {failed_loads}."
        endpoint = self.__class__.LOAD_TO_DB_OPERATION_DEFAULT_ENDPOINT
        if len(failed_loads) == 0: 
            self.logger.add_to_log(response=self.__class__.DEFAULT_SUCCESS_CODE, endpoint=endpoint, description=description).write_to_disk_incremental()
            self.delete_files(failed_loads)
            print(f"All files were successfully written to the db table:{self.ch_credentials.get('db')}.{self.ch_credentials.get('table')}.")
            return self
        elif len(failed_loads) > 0 and repeat < (self.__class__.DEFAULT_API_QUERY_RETRIES-1): 
            repeat+= 1
            self.write_data_to_db(repeat, file_list = failed_loads)
        else: 
            self.logger.add_to_log(response=self.__class__.DEFAULT_ERROR_CODE, endpoint=endpoint, description=description).write_to_disk_incremental()
            self.delete_files(failed_loads)
            self.final_log_record()
            self.logger.write_to_disk_last_run()
            self.write_log_to_db()
            if not self.global_settings.get('delete_not_uploaded_to_db_temp_data'):
                raise FlowException(f"Not all Logs API downloaded files were properly written to {self.ch_credentials.get('db')}.{self.ch_credentials.get('table')}.\n \
                                Please, re-upload leftover files from {self.data_path}. Rest of files were successfully uploaded.")
            else: 
                raise FlowException(f"Not all Logs API downloaded files were properly written to {self.ch_credentials.get('db')}.{self.ch_credentials.get('table')}.\n \
                                Please, re-run run the script and perform FINAL deduplication in ClickHouse.")
        
    def delete_files(self, exclusion_list=None): 
        """Method to delete downloaded datafiles. Not used distinctly."""
        if self.global_settings.get('delete_temp_data'): 
            if exclusion_list is None or self.global_settings.get('delete_not_uploaded_to_db_temp_data'): 
                exclusion_list = []
            deleted_files = 0
            for file in self.files: 
                if file not in exclusion_list: 
                    self.utilset.delete_file(file)
                    deleted_files+= 1
            print(f"Deleted {deleted_files} logs api datafiles. Left to re-upload manually {len(self.files) - deleted_files} files.")
        return self
    
    def write_log_to_db(self): 
        """Method to write out last run log to database. Not used distinctly."""
        format = "%Y-%m-%d %H:%M:%S"
        if self.ch_credentials.get('logTable') and self.is_log_table:
            rowed_log = self.logger.log.split('\n')[:-1]
            rowed_log = [col.split('\t') for col in rowed_log]
            for row in rowed_log: 
                row[0] = datetime.strptime(row[0], format)
            result = self.ch.insert_data(self.ch.logTable, rowed_log)
            if result: 
                print(f"Log was succesfully written to table: {self.ch_credentials.get('db')}.{self.ch_credentials.get('logTable')}.")
            else: 
                self.final_log_record()
                self.logger.write_to_disk_last_run()
                raise FlowException(f"Log wasn't written to {self.ch_credentials.get('db')}.{self.ch_credentials.get('logTable')} for some reason.")
        return self
    
    def final_log_record(self, success=False):
        """Method to create last record in the log of script run."""
        if success: 
            response = self.DEFAULT_SUCCESS_CODE
            description = 'Script finished successfully.'
        else: 
            response = self.DEFAULT_ERROR_CODE
            description = "Script finished unsuccessfully."
        endpoint = self.__class__.FINISH_OPERATION_DEFAULT_ENDPOINT
        self.logger.add_to_log(response, endpoint, description)
        self.logger.write_to_disk_incremental()
        return self

    def close_and_finish(self):
        """Method to close connections and to write out last run log to db and disk."""
        self.final_log_record(True)
        self.write_log_to_db()
        self.ch.close_connections()
        self.logger.write_to_disk_last_run()
        print("Script finished successfully.") 
        return self 