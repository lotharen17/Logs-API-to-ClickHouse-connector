import requests


class AbstractRequest:
    """Abstract class to send requests. Will be inherited by concrete classes
    
    Arguments: 
        counterId - :str, id of a counter from credentials config. 
        token - :str, OAuth token fro authorisation in Logs API. 

    Constants: 
        BASE_URL - base URL of Yandex Metrica's Logs API endpoint with ternary macro for counterId: 'https://api-metrika.yandex.net/management/v1/counter/%s' . 
        OAuth - string with ternary macro to store OAuth authorization token for Metrica's Logs API requests. 
        HTTP_METHOD - GET by default. Will be rewritten in separate distinct child-classes. 
        SPECIFIC_URL - specific constant end-part of URL. Will be rewritten for each child class. 

    Properties: 
        counterId - Metrica counter id from arguments. 
        headers - headers for request. By default - only Authorization header. But will be expanded in child classes. 
        params - parameters for requests. By default - None. 
        method - see class' constants. 
        url - resultive url, consists of class' constant BASE_URL + variable part + class' constant SPECIFIC_URL.
        is_success - result if both request and its parsing returned success resutls (code 200 + there is a necessary data we expected). 
        response_code - response code to our request from Metrica's Logs API server. 
        response_body - body (dict or string, None by default) from Logs API server. 

    Methods: 
        __init__(self, counterId, token, params=None) - initialization of instance of class. 
        send_request(self, method = HTTP_METHOD) - to send request to Logs API. Also calls parse_response method. Returns self. 
        parse_response(self, response) - to parse response (get it's status code and body and stores it as instance's properties.)
    """

    BASE_URL = 'https://api-metrika.yandex.net/management/v1/counter/%s/'
    SPECIFIC_URL = ''
    OAuth = 'OAuth %s' 
    HTTP_METHOD = 'GET' 
    SUCCESS_CODE = 200
    ERROR_MESSAGE_KEY = 'message'

    def __init__(self, counterId, token, log_writer=None, params=None):
        self.counterId = counterId
        self.headers =  { 'Authorization': __class__.OAuth%token}
        self.params = params
        self.method = self.__class__.HTTP_METHOD
        self.url = __class__.BASE_URL%counterId +self.__class__.SPECIFIC_URL
        self.is_success = False 
        self.response_code = None 
        self.response_body = None
        self.log = log_writer
    
    def send_request(self):
        raw_response = requests.request(self.method, self.url, headers=self.headers, params=self.params)
        self.parse_response(raw_response)
        self.deep_parse_response()
        self.is_success_logic()
        self.log_it()
        return self

    def parse_response(self, response):
        self.response_code = response.status_code
        try: 
            self.response_body = response.json()
        except(ValueError): 
            self.response_body = response.text
        return self
    
    def deep_parse_response(self): 
        if self.response_code != self.__class__.SUCCESS_CODE: 
            try: 
                self.response_body = self.response_body.get(__class__.ERROR_MESSAGE_KEY)
            except AttributeError: 
                try: 
                    self.response_body = self.response_body[:100]
                except: 
                    print("There is no body in response.")
        
    def is_success_logic(self):
        pass

    def log_it(self): 
        if self.log is not None: 
            classmethod = f"Class: {self.__class__.__name__}. Method: {self.__class__.log_it.__name__}"
            description = str(self.response_body)[:50]
            endpoint = self.url.removeprefix(__class__.BASE_URL)
            self.log.add_to_log(response=self.response_code, endpoint=endpoint, description=description).write_to_disk_incremental(classmethod)
        else: 
            print("Log doesn't exist.")


class LogList(AbstractRequest): 

    SPECIFIC_URL = 'logrequests'
    MAX_REQUESTS_QUEUE = 10
    SUCCESS_RESPONSE_KEY  = 'requests'

    def __init__(self, counterId, token, log_writer = None, params=None):
        super().__init__(counterId, token, log_writer, params)

    def deep_parse_response(self):
        super().deep_parse_response()
        if self.response_code == self.__class__.SUCCESS_CODE: 
            self.response_body = self.response_body.get(self.__class__.SUCCESS_RESPONSE_KEY)
        return self
          
    def is_success_logic(self):
        self.is_success = self.response_code == self.__class__.SUCCESS_CODE and len(self.response_body) < self.__class__.MAX_REQUESTS_QUEUE
        return self
    

class LogEvaluation(AbstractRequest): 

    SPECIFIC_URL = 'logrequests/evaluate'
    SUCCESS_RESPONSE_KEY  = 'log_request_evaluation'
    SUCCESS_CONDITION_KEY = 'possible'

    def __init__(self, counterId, token, log_writer=None, params=None):
        super().__init__(counterId, token, log_writer, params)

    def deep_parse_response(self):
        super().deep_parse_response()
        if self.response_code == self.__class__.SUCCESS_CODE: 
            self.response_body = self.response_body.get(self.__class__.SUCCESS_RESPONSE_KEY)
        return self

    def is_success_logic(self):
        self.is_success = self.response_code == self.__class__.SUCCESS_CODE and self.response_body.get(self.__class__.SUCCESS_CONDITION_KEY)
        return self

    

class CreateLog(AbstractRequest):

    HTTP_METHOD = "POST"
    SPECIFIC_URL = "logrequests"
    SUCCESS_RESPONSE_KEY  = 'log_request'
    SUCCESS_CONDITION_KEY = 'request_id'

    def __init__(self, counterId, token, log_writer=None, params=None):
        super().__init__(counterId, token, log_writer, params)
        self.request_id = None

    def deep_parse_response(self):
        super().deep_parse_response()
        if self.response_code == self.__class__.SUCCESS_CODE: 
            self.response_body = self.response_body.get(self.__class__.SUCCESS_RESPONSE_KEY)
            self.request_id = self.response_body.get(self.__class__.SUCCESS_CONDITION_KEY)
        return self
       
    def is_success_logic(self):
        self.is_success = self.response_code == self.__class__.SUCCESS_CODE and self.request_id is not None
        return self
    

class CleanProcessedLog(AbstractRequest): 

    HTTP_METHOD = "POST"
    SPECIFIC_URL = "logrequest/%s/clean"
    SUCCESS_RESPONSE_KEY  = 'log_request'
    SUCCESS_CONDITION_KEY = 'request_id'
    SUCCESS_STATUS_KEY = 'status'

    def __init__(self, counterId, request_id,  token, log_writer=None, params=None):
        super().__init__(counterId, token, log_writer, params)
        self.url = self.__class__.BASE_URL%counterId + self.__class__.SPECIFIC_URL%request_id
        self.request_id = request_id
        self.cleared_request_id = None
        self.status = None

    def deep_parse_response(self):
        super().deep_parse_response()
        if self.response_code == self.__class__.SUCCESS_CODE: 
            self.response_body = self.response_body.get(self.__class__.SUCCESS_RESPONSE_KEY)
            self.cleared_request_id = self.response_body.get(self.__class__.SUCCESS_CONDITION_KEY)
            self.status = self.response_body.get(self.__class__.SUCCESS_STATUS_KEY)
        return self
    
    def is_success_logic(self):
        self.is_success = self.response_code == self.__class__.SUCCESS_CODE and self.request_id == self.cleared_request_id
        return self 
    

class CleanPendingLog(CleanProcessedLog):

    SPECIFIC_URL = "logrequest/%s/cancel"

    def __init__(self, counterId, request_id, token, log_writer=None, params=None):
        super().__init__(counterId, request_id, token, log_writer, params)


class StatusLog(CleanProcessedLog): 

    SPECIFIC_URL = "logrequest/%s"
    HTTP_METHOD = "GET"
    SUCCESS_PARTS_KEY = "parts"
    SUCCESS_STATUS_TO_DOWDNLOAD = "processed"
    PART_INDEX_KEY = "part_number"

    def __init__(self, counterId, request_id, token, log_writer=None, params=None):
        super().__init__(counterId, request_id, token, log_writer, params)
        del self.cleared_request_id
        self.parts = []
        self.parts_amount = 0
    
    def deep_parse_response(self):
        super().deep_parse_response()
        if self.response_code == self.__class__.SUCCESS_CODE: 
            del self.cleared_request_id
            if self.status == self.__class__.SUCCESS_STATUS_TO_DOWDNLOAD:
                self.parts = [part.get(self.__class__.PART_INDEX_KEY) for part in self.response_body.get(self.__class__.SUCCESS_PARTS_KEY)]
                self.parts_amount = len(self.parts)
    
    def is_success_logic(self):
        self.is_success = self.response_code == self.__class__.SUCCESS_CODE and self.status == self.__class__.SUCCESS_STATUS_TO_DOWDNLOAD

    def log_it(self):
        if self.log is not None: 
            classmethod = f"Class: {self.__class__.__name__}. Method: {self.__class__.log_it.__name__}"
            description = f"Request id: {self.request_id}. Status: {self.status}. Parts amount: {self.parts_amount}."
            endpoint = self.url.removeprefix(__class__.BASE_URL)
            self.log.add_to_log(response=self.response_code, endpoint=endpoint, description=description).write_to_disk_incremental(classmethod)
        else: 
            print("Log doesn't exist.")
    

class DownloadLogPart(AbstractRequest):

    SPECIFIC_URL = 'logrequest/%s/part/'
    VARIABLE_PART_URL = '%s/download'
    
    def __init__(self, counterId, request_id, token, log_writer=None, params=None):
        super().__init__(counterId, token, log_writer, params)
        self.url_const = self.url%request_id

    def send_request(self, part):
        self.url = self.url_const + self.__class__.VARIABLE_PART_URL%part
        super().send_request()

    def log_it(self):
        pass

    def is_success_logic(self):
        self.is_success = self.response_code == self.__class__.SUCCESS_CODE
    
    





    




    


    


    




    


# def requests_sender(method, url, headers, params='', data_format='json', logging=True):
#     """Function to send requests. Args: method, url, params, kwarg = params('' by default)"""
#     r = requests.request(method, url, headers = headers, params = params)
#     response_code = r.status_code
#     endpoint = url.removeprefix('https://api-metrika.yandex.net/management')
#     if response_code == 200:
#         if data_format == 'json':
#             response = r.json()
#         else: 
#             response = r.text
#         success = True
#         if logging: 
#             logger(response =f'\t{response_code}', endpoint=f'\t{endpoint}', description = '\tSuccess\n', path = log_path)
#     else: 
#         response = None
#         success = False
#         if logging:
#             logger(response =f'\t{response_code}', endpoint=f'\t{endpoint}', description = '\tNot success\n', path = log_path)
#     return response, success