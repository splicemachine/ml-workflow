"""
Login to Azure and store the
returned Subscription ID
"""
from json import loads as parse_json
from subprocess import check_output as capture_shell_cmd, SubprocessError
from sys import stdout, stderr
from traceback import format_exc as get_traceback


def main() -> None:
    """
    Execute `az login --identity` and
    parses the returned JSON and echos
    the returned subscription ID to stdout
    """
    azure_login_cmd: tuple = ('az', 'login', '--identity')
    try:
        login_output: dict = parse_json(capture_shell_cmd(azure_login_cmd))
        print(login_output[0]['id'], file=stdout)

    except SubprocessError:
        print("An unexpected error was encountered while logging into Azure:", file=stderr)
        print(get_traceback(), file=stderr)
        raise


if __name__ == "__main__":
    main()
