"""
Login to Azure and store the
returned Subscription ID
"""
from json import loads as parse_json
from os import environ as env_vars
from subprocess import SubprocessError
from subprocess import check_output as capture_shell_cmd

from shared.logger.logging_config import logger


def main():
    """
    Execute `az login --identity` and
    parses the returned JSON and echos
    the returned subscription ID to stdout
    """
    if env_vars['MODE'] == 'development':
        azure_login_cmd = (
            'az', 'login', '--username', env_vars['AZURE_USERNAME'], '--password',
            env_vars['AZURE_PASSWORD']
        )  # use Azure AD
    else:
        azure_login_cmd = ('az', 'login', '--identity')  # use managed instance profile

    try:
        login_output: dict = parse_json(capture_shell_cmd(azure_login_cmd))
        logger.info(f"Azure ID: {login_output[0]['id']}")

    except SubprocessError:
        logger.exception(f"Unable to log into Azure")
        raise


if __name__ == "__main__":
    main()
