import logging.config
from os import environ as env_vars
from json import loads
from re import compile, sub

ENV_CHARACTER: str = '`'
ENV_REGEX = compile('{chr}(.*?){chr}'.format(chr=ENV_CHARACTER))


# We have to setup logging above the imports
# to other modules so that they propagate properly
def retrieve_logger_configuration() -> dict:
    """
    Retrieves and Formats the JSON Logger Configuration
    specified in config/logging_config.json.
    All values surrounded with `` (e.g. `FRAMEWORK_NAME`)
    will be replaced with their associated environment
    variable values.

    :return: (dict) logging configuration
    """

    with open(f'{env_vars["BOBBY_SRC_HOME"]}/config/logging_config.json', 'r', encoding='utf-8') as config_file:
        json_data: str = config_file.read()

    # extract the corresponding environment variable for each string that the regex matches
    formatted_json_data: str = sub(ENV_REGEX, lambda match: env_vars[match.group()[1:-1]], json_data)
    return loads(formatted_json_data)


logging.config.dictConfig(retrieve_logger_configuration())
