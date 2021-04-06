from functools import wraps
from flask import Response, request
from flask_login import UserMixin
from shared.services.authentication import Authentication
from shared.api.responses import HTTP
from os import environ as env_vars
import jwt

AUTH0_CERT = env_vars['JUP_AUTH0_CERT']

class User(UserMixin):
    """
    Class to represent a logged in user
    """

    def __init__(self, username: str):
        """
        :param username: (str) The username of the validated user
        """
        self.id = username

def auth_required(f) -> object:
    """
    Decorator that ensures basic authentication
    credentials are valid before executing Flask Route

    :param f: (function) callable to wrap (flask route)
    :return: (Response) either a 401 unauthorized response
        or route response
    """

    @wraps(f)
    def wrapper(*args: tuple, **kwargs: dict) -> Response:
        """
        :param args: (tuple) parameter arguments for route
        :param kwargs: (dict) keyword arguments for route
        :return: (Response) flask response or 401 response
        """
        auth = request.authorization
        if auth and auth.username and auth.password and Authentication.validate_auth(auth.username, auth.password):
            return f(*args, **kwargs)

        token = request.headers.get('Authorization')
        if token:
            token = token.replace('Bearer ', '')
            if jwt.decode(token, AUTH0_CERT, verify=False).get('email'):
                return f(*args, **kwargs)

        return Response('Access Denied. Basic Auth Credentials Denied.',
                        HTTP.codes['unauthorized'],
                        {'WWW-Authenticate': 'Basic realm="Login!"'})

    return wrapper