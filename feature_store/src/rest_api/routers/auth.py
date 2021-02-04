from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()

async def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if not check_permission(credentials.username,  credentials.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="You do not have permission to do this")

def check_permission(username: str, password: str):
    return True