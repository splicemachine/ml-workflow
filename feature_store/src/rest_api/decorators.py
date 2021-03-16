import functools
import inspect

from fastapi import Depends
from sqlalchemy.orm import Session

from .crud import get_db
from shared.logger.logging_config import logger

def managed_transaction(func):

    @functools.wraps(func)
    def wrap_func(*args, db: Session = Depends(get_db), **kwargs):
        try:
            result = func(*args, db=db, **kwargs)
            logger.info("Committing...")
            db.commit()
            logger.info("Committed")
        except Exception as e:
            logger.error(e)
            logger.warning("Rolling back...")
            db.rollback()
            raise e
        finally:
            logger.info("Flushing...")
            db.flush()
        # don't close session here, or you won't be able to response
        return result

    return wrap_func
