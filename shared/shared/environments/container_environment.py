"""
Container Environment Configuration
"""
from os import environ as env_vars


class RoleConfig:
    """
    Role Configuration for table creation
    """
    creator: bool = env_vars['TASK_NAME'] == 'bobby-0'

    # Future Roles can go here

    @staticmethod
    def get_valid() -> tuple:
        """
        Get valid roles
        :return:tuple of active roles
        """
        return RoleConfig.creator,

    @staticmethod
    def has_role(role_name: str):
        """
        Return whether or not the running environments
        has a certain role
        :param role_name: the role to check
        :return: whether or the system has the role specified
        """
        return RoleConfig.__dict__.get(role_name)
