from datetime import datetime
from datetime import timedelta
from .routines_utils import *
from .logger import Logger
from .database_utils import ClickHouseConnector
from .api_methods import * 


class MainFlowWrapper: 

    LOG_TABLE_FIELDS = ['datetime', 'response', 'endpoint', 'description']
    DEFAULT_ERROR_CODE = 500

    def __init__(self, ch_credentials, api_settings, global_settings, queries):
        self.logger = Logger()
        self.ch_credentials = ch_credentials
        self.api_settings = api_settings
        self.global_settings = global_settings
        self.queries = queries
        self.counterId = self.api_settings.get('counter')
        self.token = self.api_settings.get('token')
        self.params = api_settings
        self.params.pop('token', None)
        self.params.pop('counter', None)
        #Let's call connection establishing from the start. 
        self.establish_db_connections()
        self.is_log_table = False
        self.check_db_tables()
        self.dates_parameters_normalization()

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
                        raise DatabaseException("Table doesn't exist.")
                else:
                    raise DatabaseException("Database doesn't exist.")
            else: 
                raise DatabaseException("Query wasn't perform. Probably, not enough rights to perform SELECT query.")

        if self.global_settings.get('run_log_table_test') and self.ch_credentials.get('logTable'):
            #Check if log table exists and if columns of log table are those should be. 
            ch_log_table = self.ch.query_data(self.queries['log_table_query'], parameters = self.ch_credentials)
            ch_log_table_columns = self.ch.query_data(self.queries['log_table_query_columns'], parameters = self.ch_credentials)
            ch_log_columns = [col[0] for col in ch_log_table_columns] if ch_log_table_columns is not None else []
            if not(ch_log_table is None or ch_log_columns is None): 
                if (len(ch_log_table) == 0 or ch_log_columns != self.__class__.LOG_TABLE_FIELDS) and self.global_settings.get('create_log_table_on_fail'):
                    self.is_log_table = self.ch.create_table(self.queries['log_table_create'], self.ch_credentials.get('logTable'))
                    if not(self.is_log_table or self.global_settings.get('continue_on_log_table_creaion_fail')): 
                        raise DatabaseException(f"Sorry. Log Table didn't pass a check and a new one called {self.ch_credentials.get('logTable')} couldn't be created.")
                elif len(ch_log_table) > 0 and ch_log_columns == self.__class__.LOG_TABLE_FIELDS: 
                    self.is_log_table = True
            else:
                print(f"Query of {self.ch_credentials.get('logTable')} wasn't successfull")
                if self.global_settings.get('continue_on_log_table_creaion_fail'): 
                    raise DatabaseException(f"Table {self.ch_credentials.get('logTable')} or its columns weren't queried. Probably, not enough rights or other query issue")
                
            

        

            






                


