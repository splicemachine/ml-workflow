"""
Dependency on Auth for FastAPI
"""
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasicCredentials, HTTPBasic
from shared.api.models import AuthUser
from shared.services.authentication import Authentication

security = HTTPBasic()
#
# class AuthScheme:
#     """
#     An authentication scheme for FastAPI
#     """
#     def __init__(self, scheme_name: str, creator: Callable[[str], AuthUser]):
#
#
# #
# def verify_jwt(jwt):
#     # extract claim set from JWT
#     return {'username': 'amrit', 'roles': ['crud_exp_1', 'crud_exp_2']}
#
def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if not Authentication.validate_auth(credentials.username, credentials.password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            headers={'WWW-Authenticate': 'Basic realm="Login!"'},
                            detail="Credentials specified are invalid.")
    return AuthUser(username=credentials.username)
#
# def authenticate(authorization: Header(None)):
#     """
#     FastAPI Dependency to carry out Basic Auth and return an "AuthUser" if the user
#     is valid.
#     """
#     if authorization:
#         username, password = decode_b64_auth(authorization)
#         if Authentication.validate_auth(username, password):
#             return AuthUser(username=username)
# #
#
# def authenticate_jwt(authorization: str = Header(None)):
#     """
#     FastAPI Dependency to carry rout JWT auth and return an "AuthUser" if the claims are verified.
#     """
#     if authorization:
#         claims = verify_jwt(jwt=authorization.split('Bearer ')[1])
#         if claims:
#             return AuthUser(username=claims['username'], roles=claims['roles'])
#
#
# async def authenticate(basic_auth_result=Depends(authenticate_basic), jwt_result=Depends(authenticate_jwt)):
#     """
#     If either basic auth or jwt passes, return an AuthUser. Otherwise, throw an HTTP exception
#     """
#     if not (basic_auth_result or jwt_result):
#         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
#                             headers={'WWW-Authenticate': 'Basic realm="Login!"'},
#                             detail="Credentials specified are invalid.")
#     return jwt_result or basic_auth_result  # if both are passed we should use jwt because it contains more info
