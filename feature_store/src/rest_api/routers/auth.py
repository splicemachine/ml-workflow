from fastapi import Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from shared.api.exceptions import SpliceMachineException, ExceptionCodes

security = HTTPBasic()

async def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if not check_permission(credentials.username,  credentials.password):
        raise SpliceMachineException(status_code=status.HTTP_403_FORBIDDEN, code=ExceptionCodes.NOT_AUTHORIZED,
                            detail="You do not have permission to do this")

def check_permission(username: str, password: str):
    return True