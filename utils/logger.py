from  datetime import datetime
from  .routines_utils import UtilsSet

class Logger:
    """Creates a piece of log and then writes it to disk/db.

    Arguments: 
        path :str - path to log-file relatively to the current working directory.
        run_log_path - path to save end-to-end log. 

    Properties: 
        log :str - end-to-end peace of log during current programm run. 
    
    Methods: 
        add_to_log(response :str, endpoint :str, description :str) - adds new line to the log variable. 
        write_to_disk() - performs writing operation to the local disk. 
        write_to_db( :ClassInstanceOfDBConnection) - performs writing operation to the database. 
    """

    TAB_SEP = '\t'
    EOL_SEP = '\n'

    def __init__(self, path_continous='logs/logs.tsv', path_last_run = None):
        self._path = path_continous
        self._path_last = path_last_run
        self._log = ''
        self._log_line = ''
        self.utils = UtilsSet()

    @property
    def log(self): 
        return self._log

    @property
    def path(self): 
        return self._path

    def add_to_log(self, response=200, endpoint='', description=''):
        """Method to add line to end-to-end log. Also assigns log line.
        
        Arguments: 
            response :int - response http code or special codes to signalize about some errrors.
            endpoint :str - url or special local endpoints. 
            description :str - text description of what happened.
        
        Returns: 
            self (suitable for methods chaining). 
            _log_line is a tsv row with datetime of the datetime type, int repsonse code, str endpoint and str description (text). The _log is the 
            cummulative concatanation of _log_lines. 
        """
        
        full_response = Logger.TAB_SEP + str(response)
        full_endpoint = Logger.TAB_SEP + endpoint 
        full_description = Logger.TAB_SEP + description + Logger.EOL_SEP
        date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._log_line = date_time + full_response + full_endpoint + full_description
        self._log += self._log_line
        return self 
    
    def write_to_disk_incremental(self, classMethod = 'None'):
        """Method to make incremental writes to local logfile.
        
        Arguments: 
            self, works with instance's global variables. Uses _path variable. If it's none - incremental log won't be written. 
            classMethod :str , default None - name of class+method to print, where logger was called. 
        
        Returns: 
            self (suitable for methods chaining)
        """
        if self._path is not None: 
            try: 
                self.utils.write_to_file(self._log_line, self._path)
            except(OSError, IOError): 
                print(f"You probably don't have and access to {self._path} or to create this file even in working directory.")
                print(f"Logline of {classMethod} job wasn't written to disk.")
            self._log_line = ''
        return self
    
    def write_to_disk_last_run(self): 
        """Method to write last run's log to disk
        """
        if self._path_last is not None: 
            try: 
                self.utils.write_to_file(self._log, self._path_last)
                print(f"Full log of this run has been written.")
            except(OSError, IOError): 
                print(f"You probably don't have an access to {self._path_last} or to create this file even in working directory.")
        return self

