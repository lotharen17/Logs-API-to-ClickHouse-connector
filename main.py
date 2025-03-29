from utils.routines_utils import UtilsSet
from utils.api_methods import *
from utils.wrappers import MainFlowWrapper


#Creating utilitites set instance for script run
utilities = UtilsSet()

#Reading credentials and parameters from configs. 
ch_credentials = utilities.read_json_file("configs/ch_credentials.json")
api_settings = utilities.read_json_file("configs/api_credentials.json")
global_settings = utilities.read_json_file("configs/global_config.json")

#Reading queries and creating dictionary of queries to perform during program execution. 
queries = {}
queries['db_query'] = utilities.read_sql_file("queries/query_database.sql")
queries['table_query'] = utilities.read_sql_file("queries/query_table.sql")
queries['columns_query'] = utilities.read_sql_file("queries/query_columns.sql")
queries['log_table_query'] = utilities.read_sql_file("queries/query_log_table.sql")
queries['log_table_query_columns'] = utilities.read_sql_file("queries/query_log_table_columns.sql")
queries['log_table_create'] = utilities.read_sql_file("queries/create_log_table.sql")%ch_credentials

#Creating main_flow execution instance. It will establish CH(optionaly - with ssh tunnel) connection and perform db and tables checks if proper globa_config parameters are set. 
main_flow = MainFlowWrapper(ch_credentials, api_settings, global_settings, queries, utilities)
#Let's perform log evaluation request. 
main_flow.check_log_evaluation()
#Let's perform Logs API log creation request. 
main_flow.create_log_request()
#Let's perform log status recursional checks. 
main_flow.log_status_check()
#Let's download the Logs API log. 
main_flow.log_downloader()
#Let's load downloaded data to our CH instance. 
main_flow.write_data_to_db()
#Let's properly finish the script with log records. 
main_flow.close_and_finish()