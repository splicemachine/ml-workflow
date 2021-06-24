"""
Dependency on Auth for FastAPI
"""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasicCredentials, HTTPBasic
from shared.api.models import AuthUser
from shared.services.authentication import Authentication

security = HTTPBasic()

def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if not Authentication.validate_auth(credentials.username, credentials.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            headers={'WWW-Authenticate': 'Basic realm="Login!"'},
                            detail="Credentials specified are invalid.")
    return AuthUser(username=credentials.username)