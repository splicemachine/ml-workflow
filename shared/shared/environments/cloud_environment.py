from os import environ as env_vars


class CloudEnvironment:
    """
    Class representing an environments
    (e.g. AWS, Azure, GCP, Heroku etc.)
    """

    def __init__(self, name: str, fullname: str, handler_mapping: dict, can_deploy: bool = False) -> None:
        """
        :param name: (str) the short abbreviation for the environments
        :param fullname: (str) the full name of the environments
        :param handler_mapping: (dict) mapping for pages to the handler name
            when the page form is submitted
        :param can_deploy: (bool) whether deployment to that service is available
        """
        self.name: str = name
        self.fullname: str = fullname
        self.handler_mapping: dict = handler_mapping
        self.can_deploy = can_deploy

    def __eq__(self, name: object) -> bool:
        """
        Check if an environments is equal to a
        string

        :param name: (str) string to check against
        :return: (bool) whether or not the specified string
        is equal to the name or full name of this cloud environments
        """
        if isinstance(name, str):
            return name.lower() in [self.name.lower(), self.fullname.lower()]

        return super().__eq__(name)


class CloudEnvironments:
    """
    Class containing valid environments
    """

    aws: CloudEnvironment = CloudEnvironment(
        name="AWS",
        fullname="Amazon Web Services",
        handler_mapping={
            'deploy': 'DEPLOY_AWS'
            # deploy attribute is generalized to HandlerNames.deploy_csp handler
        },
        can_deploy=True
    )

    azure: CloudEnvironment = CloudEnvironment(
        name="Azure",
        fullname="Microsoft Azure",
        handler_mapping={
            'deploy': 'DEPLOY_AZURE'
        },
        can_deploy=True
    )

    gcp: CloudEnvironment = CloudEnvironment(
        name="GCP",
        fullname="Google Cloud",
        handler_mapping={
            'deploy': 'NONE'
        },
        can_deploy=False
    )

    openstack: CloudEnvironment = CloudEnvironment(
        name="OpenStack",
        fullname="OpenStack",
        handler_mapping={
            'deploy': 'NONE'
        },
        can_deploy=False
    )

    @staticmethod
    def get_valid() -> tuple:
        """
        Get valid environments
        :return: (tuple)) valid environments names
        """
        return (
            CloudEnvironments.aws,
            CloudEnvironments.azure,
            CloudEnvironments.gcp,
            CloudEnvironments.openstack
        )

    @staticmethod
    def get_current_name() -> str:
        """
        Return a string representation of the current
        environments
        :return: (str) string of current environments
        """
        return env_vars['ENVIRONMENT']

    @staticmethod
    def get_current() -> CloudEnvironment:
        """
        Get the current Cloud Environment
        :return: (CloudEnvironment) The current cloud environments
        """
        return CloudEnvironments.__dict__[env_vars['ENVIRONMENT']]
