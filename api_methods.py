class AbstractRequest:
    BaseURL = 'https://api-metrika.yandex.net/management/v1/counter/'

    def __init__(self, counterId, authorization):
        self.counterId = counterId
        self.authorization = authorization


    def send_request(self, method):
        pass

        