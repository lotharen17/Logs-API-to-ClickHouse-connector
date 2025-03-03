from  datetime import datetime
# from datetime import timedelta
import os 

class Logger:
    """Creates a piece of log and then writes it to disk/db.

    Arguments: 
        path :str - path to log-file relatively to the current working directory.

    Properties: 
        log :str - end-to-end peace of log during current programm run. 
    
    Methods: 
        add_to_log(response :str, endpoint :str, description :str) - adds new line to the log variable. 
        write_to_disk() - performs writing operation to the local disk. 
        write_to_db( :ClassInstanceOfDBConnection) - performs writing operation to the database. 
    """

    TAB_SEP = '\t'
    EOL_SEP = '\n'

    def __init__(self, path='logs/logs.tsv',  db_connection = None, run_log_path = None):
        self._run_log_path = run_log_path 
        self._path = path
        self._log = ''
        self._log_line = ''

    @property
    def log(self): 
        return self._log

    @property
    def path(self): 
        return self._path

    def add_to_log(self, response='', endpoint='', desctiption=''):
        """Method to add line to end-to-end log. Also assigns log line.
        
        Arguments: 
            response :str - response http code or special codes to signalize about some errrors.
            endpoint :str - url or special local endpoints. 
            description :str - text description of what happened.
        
        Returns: 
            self (suitable for methods chaining)
        """
        
        full_response = Logger.TAB_SEP + response
        full_endpoint = Logger.TAB_SEP + endpoint 
        full_description = Logger.TAB_SEP + desctiption + Logger.EOL_SEP
        date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log_line = date_time + full_response + full_endpoint + full_description
        self._log += self._log_line
        return self 
    
    def write_to_disk_incremental(self):
        """Method to make incremental writes to local logfile.
        
        Arguments: 
            self only, works with instance's global variables.
        
        Returns: 
            self (suitable for methods chaining)
        """

        def nested_writer(): 
            """Function which takes atomic part of log (one line) from global environment (self) and writes them to logfile."""
            with open(self._path, "a", encoding="utf-8", newline='\n') as f:
                f.write(self._log_line)
                print('Logline was successfully written to disk.')
                self._log_line = ''

        if os.path.exists(self._path): 
            print(f"Path {self._path} exists.")
            try: 
                nested_writer()
            except(OSError, IOError): 
                print(f"You probably don't have an acess to {self._path} file.")
        else: 
            print(f"Path {self._path} doesn't exist")
            try:
                if len(self._path.split('/')) > 1: 
                    dirpath = '/'.join(self._path.split('/')[:-1:])+'/'
                    if not os.path.isdir(dirpath): 
                        print(f"Directory {dirpath} doesn't exist")
                        try: 
                            os.makedirs(dirpath, exist_ok=True)
                            print(f"Directory {dirpath} created")
                        except(OSError, IOError): 
                            print(f"Directory {dirpath} wasn't created. Probably, you don't have proper permission." )
                
                nested_writer()
            except(OSError, IOError):
                print(f"File {self._path} wasn't created. Probably, you don't have proper permission.")
        return self
    
    def write_to_db(self, db_connection, table_name): 
        pass


        

#File logger, to log atomic (one row) results in log-file
# def logger(response ='\t', endpoint='\t', description = '\t\n', path='logs.tsv'):
#     """Logs data in file"""
#     date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     log_line = date_time + response + endpoint +description
#     with open(path, "a", encoding="utf-8", newline='\n') as f:
#         f.write(log_line)
#     return None



#File

