import functools

from fastapi import Depends
from sqlalchemy.orm import Session
from shared.db.connection import SQLAlchemyClient
from shared.logger.logging_config import logger


def managed_transaction(func):
    """
    Managed Transaction with automatic rollback, flushing, and exception handling
    """
    @functools.wraps(func)
    def wrap_func(*args, db: Session = Depends(SQLAlchemyClient().get_session), **kwargs):
        try:
            result = func(*args, db=db, **kwargs)
            logger.info("Committing...")
            db.commit()
            logger.info("Committed")
        except Exception as e:
            logger.error(e)
            logger.warning("Rolling back...")
            db.rollback()
            # if not isinstance(e, (RequestValidationError, SpliceMachineException, StarletteHTTPException)):
            #     e = SpliceMachineException(status_code=status.HTTP_400_BAD_REQUEST, code=ExceptionCodes.UNKNOWN,
            #         message=str(e))
            raise e
        finally:
            logger.info("Flushing...")
            db.flush()
            logger.info("Flushed")
        # don't close session here, or you won't be able to response
        return result

    return wrap_func
