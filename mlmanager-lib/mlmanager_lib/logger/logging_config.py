"""
Logging Configuration for MLManager Components
"""
import logging.config
import pkg_resources

from os import environ as env_vars
from json import loads
from re import compile, sub

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"

LOGGER_CONFIG_FILE: str = pkg_resources.resource_filename('mlmanager_lib', 'logger/logging.json')

ENV_CHARACTER: str = '`'
ENV_REGEX = compile('{chr}(.*?){chr}'.format(chr=ENV_CHARACTER))


# We have to setup logger above the imports
# to other modules so that they propagate properly
def retrieve_logger_configuration() -> dict:
    """
    Retrieves and Formats the JSON Logger Configuration
    specified in config/logger.json.
    All values surrounded with `` (e.g. `FRAMEWORK_NAME`)
    will be replaced with their associated environment
    variable values.

    :return: (dict) logger configuration
    """

    with open(LOGGER_CONFIG_FILE, 'r', encoding='utf-8') as config_file:
        json_data: str = config_file.read()

    # extract the corresponding environment variable for each string that the regex matches
    formatted_json_data: str = sub(ENV_REGEX, lambda match: env_vars[match.group()[1:-1]],
                                   json_data)
    return loads(formatted_json_data)


logging.config.dictConfig(retrieve_logger_configuration())
