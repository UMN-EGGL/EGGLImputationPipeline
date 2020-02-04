class BeagleTimeoutError(Exception):
    def __init__(self, msg=''):
        super().__init__(msg)

class BeagleHeapError(Exception):
    def __init__(self, msg=''):
        super().__init__(msg)
