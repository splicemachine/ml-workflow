
from shared.logger.logging_config import logger


def toDict(resultproxy):
    return [{column: value for column, value in rowproxy.items()} for rowproxy in resultproxy]

def handle_exception(exc):
    logger.error(exc)
    return JSONResponse(status_code=400, content={ "detail": str(exc) })