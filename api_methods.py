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
    OAuth = 'OAuth %s' 


    def __init__(self, counterId, token, params=None, method = 'GET'):
        self.counterId = counterId
        self.headers =  { 'Authorization': __class__.OAuth%token}
        self.params = params
        self.method = method
        self.url = __class__.BASE_URL%counterId
        self.is_success = False 
        self.response_code = None 
        self.response_body = None
    
            
    def send_request(self):
        raw_response = requests.request(self.method, self.url, headers=self.headers, params=self.params)
        self.parse_response(raw_response)
        return self


    def parse_response(self, response):
        self.response_code = response.status_code
        if self.response_code == 200: 
            self.response_body = response.json()
            self.is_success = True
        else: 
            self.response_body = response.text
        return self
    
    def is_success_logic(self):
        pass



class LogListRequests(AbstractRequest): 

    HTTP_METHOD = 'GET'
    SPECIFIC_URL = 'logrequests'

    def __init__(self, counterId, token, params=None):
        super().__init__(counterId, token, params)
        self.method = __class__.HTTP_METHOD
        self.url+= __class__.SPECIFIC_URL



    

myrequest = LogListRequests(14112952, 'TTTT')#"y0_AgAAAAABvdAPAAzDOgAAAAEYJr3cAACIUxevu5dFWrC6TvDR78ChYJfR6w")
myrequest.send_request()

print(myrequest.url)

print(myrequest.response_code)
print(myrequest.response_body)
print(myrequest.method)

abstract_request = AbstractRequest(111, 'Auth343')

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