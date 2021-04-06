from fastapi import Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials, OAuth2PasswordBearer
from shared.api.exceptions import SpliceMachineException, ExceptionCodes
from shared.services.authentication import Authentication

SECRET_KEY = 'secret'
ALGORITHM = 'HS256'

# security = HTTPBasic()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def authenticate(token: str = Depends(oauth2_scheme)):
    credentials_exception = SpliceMachineException(status_code=status.HTTP_401_UNAUTHORIZED, code=ExceptionCodes.NOT_AUTHORIZED,
                                        message="Could not validate credentials")
    try:
        # email = 'test'
        email = Authentication.validate_token(token)
        # email = Authentication.validate_auth('splice', 'admin')
        if email is None:
            raise credentials_exception
    except:
        raise credentials_exception
    
    if not check_permission(email):
        raise SpliceMachineException(status_code=status.HTTP_403_FORBIDDEN, code=ExceptionCodes.NOT_AUTHORIZED,
                            message="You do not have permission to do this")

def check_permission(email: str):
    return True
