"""
Constructs for Authentication
"""
import logging
from functools import wraps

from flask import request, Response
from flask_login import UserMixin
from retrying import retry
from os import environ as env_vars, system as bash, popen as bash_popen
from py4j.java_gateway import java_import, JavaGateway
from py4j.protocol import Py4JJavaError, Py4JNetworkError
from .responses import HTTP
from time import sleep as delay

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja, Ben Epstein"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja, Ben Epstein"
__email__: str = "abaveja@splicemachine.com"

LOGGER = logging.getLogger(__name__)


class User(UserMixin):
    """
    Class to represent a logged in user
    """

    def __init__(self, username: str):
        """
        :param username: (str) The username of the validated user
        """
        self.id = username


class Py4JUtils:
    """
    Utilities for interacting with py4j
    """

    @staticmethod
    def retry_on_py4j_network(exc: Exception) -> bool:
        """
        Returns whether or not the exception thrown is a py4j network error
            """
        return isinstance(exc, Py4JNetworkError)


class Authentication:
    """
    Utilities to assist with Authentication
    """

    @staticmethod
    @retry(stop_max_attempt_number=3, wait_fixed=1,
           retry_on_exception=Py4JUtils.retry_on_py4j_network)
    def create_gateway():
        """
        Starts the java gateway server if it doesn't exist and creates the gateway
        :return: (Gateway) java gateway object
        """
        # Start the gateway to connect with py4j
        if 'gateway' not in bash_popen('jps').read():
            bash('java gateway &')
            delay(0.5)
            LOGGER.debug('Started Java gateway')
        # Connect to gateway getting jvm object
        LOGGER.debug('Connecting to gateway from py4j')
        gate = JavaGateway()
        return gate

    @staticmethod
    def basic_auth_required(f) -> object:
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
            auth: object = request.authorization
            if not auth or not auth.username or not auth.password or not \
                    Authentication.validate_auth(auth.username, auth.password):
                return Response('Access Denied. Basic Auth Credentials Denied.',
                                HTTP.codes['unauthorized'],
                                {'WWW-Authenticate': 'Basic realm="Login!"'})
            return f(*args, **kwargs)

        return wrapper

    @staticmethod
    def validate_auth(username: str, password: str) -> str:
        """
        This function uses the Shiro 
        authentication API and retrieves whether
        or not the user was authenticated or not.

        :param username: (str) the username to validate
        :param password: (str) the password to validate
        :return: (bool) whether or not the user is authenticated
        """
        gate = Authentication.create_gateway()
        java_import(gate.jvm, "com.splicemachine.shiro.SpliceDatabaseRealm.*")
        realm = gate.jvm.com.splicemachine.shiro.SpliceDatabaseRealm()
        LOGGER.debug('Connection successful')
        realm.setServerName(env_vars['DB_HOST'])
        realm.setServerPort("1527")
        realm.setDatabaseName("splicedb")
        # when shrio authentication fails, it throws an error
        try:
            LOGGER.debug('Attempting login')
            realm.initialize(username, password)
        except Py4JJavaError as e:
            LOGGER.info('Login Failed')
            LOGGER.info(e.errmsg, '\n', type(e).__name__, '\n', e.java_exception)
            return None
        LOGGER.debug('Login successful')
        return username
