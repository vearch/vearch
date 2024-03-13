class VearchException(Exception):
    def __init__(self, code: int, message: str):
        super().__init__()
        self._code = code
        self._msg = message

    @property
    def code(self):
        return self._code

    @property
    def message(self):
        return self._msg


class SpaceException(VearchException):
    """raise when use space operation occur error"""
    pass


class DatabaseException(VearchException):
    """ raise when use database operation occur error """
    pass


class DocumentException(VearchException):
    """
    raise where use document operation occur error
    """
    pass
