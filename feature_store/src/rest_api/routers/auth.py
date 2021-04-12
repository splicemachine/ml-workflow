from fastapi import Depends, status, Header
from fastapi.security import HTTPBasic, HTTPBasicCredentials, OAuth2PasswordBearer
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from shared.services.authentication import Authentication
from shared.logger.logging_config import logger
import base64

security = HTTPBasic()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
 
def verify_jwt(token: str = Depends(oauth2_scheme)):
    logger.info('Verifying JWT Token')
    credentials_exception = SpliceMachineException(status_code=status.HTTP_401_UNAUTHORIZED, code=ExceptionCodes.NOT_AUTHORIZED,
                                        message="Could not validate credentials")
    try:
        email = Authentication.validate_token(token)
        if email is None:
            raise credentials_exception
    except:
        raise credentials_exception
    return email

def verify_basic(credentials: HTTPBasicCredentials = Depends(security)):
    logger.info('Verifying Basic Auth')
    credentials_exception = SpliceMachineException(status_code=status.HTTP_401_UNAUTHORIZED, code=ExceptionCodes.NOT_AUTHORIZED,
                                        message="Could not validate credentials")

    try:
        username, password = base64.b64decode(credentials).decode('ascii').split(':', 1)
        if username and password:
                return Authentication.validate_auth(username, password)
        else:
            raise credentials_exception
    except:
        raise credentials_exception

def authenticate(authorization = Header(None)):
    if authorization.startswith("Bearer "):
        email = verify_jwt(authorization.replace('Bearer ', ''))
    elif authorization.startswith("Basic "):
        email = verify_basic(authorization.replace('Basic ', ''))

    if not email:
        raise SpliceMachineException(status_code=status.HTTP_401_UNAUTHORIZED, code=ExceptionCodes.NOT_AUTHORIZED,
                                    message="Could not validate credentials")

    if not check_permission(email):
        raise SpliceMachineException(status_code=status.HTTP_403_FORBIDDEN, code=ExceptionCodes.NOT_AUTHORIZED,
                            message="You do not have permission to do this")

def check_permission(email: str):
    return True
