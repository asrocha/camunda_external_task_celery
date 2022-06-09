
class BusinessException(Exception):
    """
    Raise a Business Exception
    """

    def __init__(self, error_code=0,error_message="BPMN Error", *args):

        self.message = error_message
        self.code = error_code
        self.error_code = error_code
        self.error_message=error_message
    def __str__(self):
        return F"error_code:{self.error_code}, error_message:{self.error_message}"
