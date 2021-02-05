class SpliceMachineException(Exception):
    def __init__(self, status_code: int, code: str, message: str):
        self.status_code = status_code
        self.code = code
        self.message = message

class ExceptionCodes:
    """
    Enum for SpliceMachineException codes
    """
    ALREADY_DEPLOYED: str = 'ALREADY_DEPLOYED'
    ALREADY_EXISTS: str = 'ALREADY_EXISTS'
    BAD_ARGUMENTS: str = 'BAD_ARGUMENTS'
    DOES_NOT_EXIST: str = 'DOES_NOT_EXIST'
    INVALID_FORMAT: str = 'INVALID_FORMAT'
    INVALID_SQL: str = 'INVALID_SQL'
    INVALID_TYPE: str = 'INVALID_TYPE'
    MISSING_ARGUMENTS: str = 'MISSING_ARGUMENTS'
    UNKNOWN: str = 'UNKNOWN'