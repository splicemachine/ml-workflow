from abc import abstractmethod
from os import environ as env_vars

_author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


# Definition for Constant Classes

class Definition:
    """
    Base class for other classes
    defining valid terms
    """

    @staticmethod
    @abstractmethod
    def get_valid() -> tuple:
        pass


class CloudEnvironment:
    """
    Class representing an environment
    (e.g. AWS, Azure, GCP, Heroku etc.)
    """

    def __init__(self, name: str, fullname: str, handler_mapping: dict) -> None:
        """
        :param name: (str) the short abbreviation for the environment
        :param fullname: (str) the full name of the environment
        :param handler_mapping: (dict) mapping for pages to the handler name
            when the page form is submitted
        """
        self.name: str = name
        self.fullname: str = fullname
        self.handler_mapping: dict = handler_mapping

    def __eq__(self, name: object) -> bool:
        """
        Check if an environment is equal to a
        string

        :param name: (str) string to check against
        :return: (bool) whether or not the specified string
        is equal to the name or full name of this cloud environment
        """
        if isinstance(name, str):
            return name.lower() in [self.name.lower(), self.fullname.lower()]

        return super().__eq__(name)


class CloudEnvironments(Definition):
    """
    Class containing valid environments
    """

    aws: CloudEnvironment = CloudEnvironment(
        name="AWS",
        fullname="Amazon Web Services",
        handler_mapping={
            'deploy': 'DEPLOY_AWS'
            # deploy attribute is generalized to HandlerNames.deploy_csp handler
        }
    )

    azure: CloudEnvironment = CloudEnvironment(
        name="Azure",
        fullname="Microsoft Azure",
        handler_mapping={
            'deploy': 'DEPLOY_AZURE'
        },
    )

    @staticmethod
    def get_valid() -> tuple:
        """
        Get valid environments
        :return: (tuple)) valid environment names
        """
        return (
            CloudEnvironments.aws,
            CloudEnvironments.azure
        )

    @staticmethod
    def get_current_name() -> str:
        """
        Return a string representation of the current
        environment
        :return: (str) string of current environment
        """
        return env_vars['ENVIRONMENT']

    @staticmethod
    def get_current() -> CloudEnvironment:
        """
        Get the current Cloud Environment
        :return: (CloudEnvironment) The current cloud environment
        """
        return CloudEnvironments.__dict__[env_vars['ENVIRONMENT']]

