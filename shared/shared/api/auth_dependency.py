"""
Dependency on Auth for FastAPI
"""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from shared.services.authentication import Authentication
from shared.api.models import AuthUser
security = HTTPBasic()


def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    """
    FastAPI Dependency to carry out Basic Auth and return an "AuthUser" if the user
    is valid.
    In the future, this should authenticate JWTs and the AuthUser "scopes" and "roles" attributes can be used
    """
    if not Authentication.validate_auth(credentials.username, credentials.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            headers={'WWW-Authenticate': 'Basic realm="Login!"'},
                            detail="Credentials specified are invalid.")
    return AuthUser(username=credentials.username)
