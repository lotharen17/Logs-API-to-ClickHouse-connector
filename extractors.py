from  datetime import datetime
from datetime import timedelta

class Logger:
    """Creates a piece of log and then writes it to disk/db.

    Arguments: 
        path :str - path to log-file relatively to the current working directory.

    Properties: 
        log :str - atomic peace of log. 
    
    Methods: 
        add_to_log(response :str, endpoint :str, description :str) - adds new line to the log variable. 
        write_to_disk() - performs writing operation to the local disk. 
        write_to_db( :ClassInstanceOfDBConnection) - performs writing operation to the database. 
    """

    def __init__(self, path='logs.tsv', db_connection = None):
        self._path = path
        self._response = '\t'
        self._endpoint = '\t'
        self._description = '\t\n'
        self._log = ''

    @property
    def log(self): 
        return self._log

    def add_to_log(self, response='', endpoint='', desctiption='')
        full_response = response + self._response
        full_endpoint = endpoint + self._endpoint
        full_description = desctiption + self._description
        date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log += date_time + full_response + full_endpoint + full_description
        return self 
    
    def write_to_disk(self): 
        with open(self._path, "a", encoding="utf-8", newline='\n') as f:
            f.write(self._log)
            print('Log was successfully written to disk.')
    
    def write_to_db(self, table_connection): 
        pass
        

#File logger, to log results in log-file
def logger(response ='\t', endpoint='\t', description = '\t\n', path='logs.tsv'):
    """Logs data in file"""
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = date_time + response + endpoint +description
    with open(log_path, "a", encoding="utf-8", newline='\n') as f:
        f.write(log_line)
    return None