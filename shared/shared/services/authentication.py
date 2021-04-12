"""
Constructs for Authentication
"""
import logging
from os import environ as env_vars
from os import popen as bash_popen
from os import system as bash
from time import sleep as delay
from typing import Optional

from py4j.java_gateway import JavaGateway, java_import
from py4j.protocol import Py4JJavaError, Py4JNetworkError
from retrying import retry
from shared.logger.logging_config import logger

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja, Ben Epstein"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja, Ben Epstein"
__email__: str = "abaveja@splicemachine.com"

LOGGER = logging.getLogger(__name__)


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
    def validate_auth(username: str, password: str) -> Optional[str]:
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
        # when shiro authentication fails, it throws an error
        try:
            LOGGER.debug('Attempting login')
            u = f'"{username}"' if '@' in username else username
            realm.initialize(u, password)
        except Py4JJavaError as e:
            LOGGER.info('Login Failed')
            LOGGER.info(f'{e.errmsg}-{type(e)}: {e.java_exception}')
            return None
        LOGGER.debug('Login successful')
        return username

    @staticmethod
    def validate_token(token: str) -> Optional[str]:
        """
        This function uses the Shiro 
        authentication API and retrieves whether
        or not the user was authenticated or not.

        :param username: (str) the username to validate
        :param password: (str) the password to validate
        :return: (bool) whether or not the user is authenticated
        """
        gate = Authentication.create_gateway()
        java_import(gate.jvm, "com.splicemachine.shiro.SpliceDatabaseJWTRealm.*")
        realm = gate.jvm.com.splicemachine.shiro.SpliceDatabaseJWTRealm()
        logger.debug('Connection successful')
        realm.setServerName(env_vars['DB_HOST'])
        realm.setServerPort("1527")
        realm.setDatabaseName("splicedb")

        java_import(gate.jvm, "com.splicemachine.shiro.jwt.JWTAuthenticationToken.*")
        java_import(gate.jvm, "com.splicemachine.shiro.filter.SpliceAuthenticatingFilter.*")
        filter = gate.jvm.com.splicemachine.shiro.filter.SpliceAuthenticatingFilter()
        logger.info('Create Token')
        jwt = filter.createToken(token)
        logger.info('Token created')
        # when shiro authentication fails, it throws an error
        username = jwt.getUserId()
        try:
            logger.info('Attempting login')
            u = f'"{username}"' if '@' in username else username
            realm.initialize(u, jwt.getToken())
        except Py4JJavaError as e:
            logger.info('Login Failed')
            logger.info(f'{e.errmsg}-{type(e)}: {e.java_exception}')
            return None
        logger.info('Login successful')
        return username
