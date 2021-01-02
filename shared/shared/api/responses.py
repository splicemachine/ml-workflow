"""
HTTP Utilities for Flask APIs
"""
from functools import partial, wraps
from traceback import format_exc as get_exception

from flask import Response
from flask import jsonify as create_json
from flask import make_response
from flask import render_template as show_html
from werkzeug.wrappers.response import Response as WerkzeugResponse

from shared.api.models import APIStatuses
from shared.logger.logging_config import logger

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Ben Epstein"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class _HTTP:
    """
    Utilities for the HTTP Class
    """

    @staticmethod
    def _get_response_function(code: int, payload: object) -> Response:
        """
        Get the correct response function

        :param code: (int) HTTP Status code
        :param payload: (object) Response HTTP
        :return: (Response) Response Object
        """
        return make_response(payload, code)


class HTTP:
    """
    Utilities to assist with HTTP Responses/Codes
    """
    codes: dict = {  # rather than implementing this as a definition, (see models.py)
        # we implement this as an iterable so that we can create response functions
        'success': 200,
        'malformed': 400,
        'unauthorized': 401,
        'forbidden': 403,
        'not_found': 404,
        'unexpected': 500
    }

    html_statuses: dict = {  # these can be changed in Jinja Template
        'success': 'success',
        'failure': 'failure'
    }

    responses: dict = {
        status: partial(_HTTP._get_response_function, code) for status, code in codes.items()
    }

    # correspond to variables in ml-workflow/mlflow/src/app/templates/index.html

    home_status_jinja_variable: str = 'job_status'
    home_traceback_jinja_variable: str = 'error'
    home_jinja_template: str = 'index.html'

    @staticmethod
    def generate_html_in_home_response(func):
        """
        Formats the appropriate HTML on the
        home page in the case of success/error.

        :param func: (function) function to wrap
        :return: (function) wrapped execution context
        """

        @wraps(func)
        def wrapped(*args, **kwargs) -> Response:
            """
            Run the wrapped function and format success/error
            traceback on the home page

            :param args: (iterable) parameter arguments for this function
            :param kwargs: (dict) keyword arguments for this function
            :return: (Response) Jinja2 Template Response
            """
            logger.debug(f"Endpoint {func.__name__} received data")
            try:
                func(*args, **kwargs)
                logger.debug("Endpoint completed with no errors")
                return show_html(HTTP.home_jinja_template, **{  # god bless Python3
                    HTTP.home_status_jinja_variable: HTTP.html_statuses['success'],
                })
            except Exception:
                logger.exception("An unexpected error occurred while processing the request")
                return show_html(HTTP.home_jinja_template, **{
                    HTTP.home_status_jinja_variable: HTTP.html_statuses['failure'],
                    HTTP.home_traceback_jinja_variable: f'<pre>{get_exception(limit=100)}</pre>'
                })

        return wrapped

    # we need to reverse the order of the arguments
    @staticmethod
    def generate_json_response(func):
        """
        Formats the appropriate response
        for a given endpoint-- it will
        return a failure response with the
        traceback embedded (to CHANGE) on error
        or success response with returned dictionary
        from decorated function.

        :param func: (function) function to wrap
        :return: (function) wrapped execution context
        """

        @wraps(func)
        def wrapped(*args, **kwargs) -> Response:
            """
            Run the wrapped function and return the appropriate
            response based on errors (if they occur) or the
            dictionary returned if the endpoint is successful

            :param args: (iterable) parameter arguments for function
            :param kwargs: (dict) keyword arguments for function
            :return: (Response) HTTP Response for User
            """
            logger.debug(f"Endpoint {func.__name__} received data")
            try:
                success_response: dict = func(*args, **kwargs)
                if isinstance(success_response, Response) or isinstance(success_response,
                                                                        WerkzeugResponse):
                    return success_response  # pass through HTTP Redirects
                logger.debug("Endpoint completed handle with no errors")
                return HTTP.responses['success'](
                    create_json(status=APIStatuses.success, **success_response)
                )

            except KeyError as e:  # key errors are caused by missing JSON data 99% of the time
                msg: str = f"The JSON payload received is malformed-- parameter {e} is missing/unknown"
                logger.exception(msg)
                return HTTP.responses['malformed'](
                    create_json(status=APIStatuses.failure, msg=msg))

            except Exception as e:
                logger.exception("An unexpected error occurred while processing the request")
                return HTTP.responses['unexpected'](
                    create_json(status=APIStatuses.error, msg=str(e))
                )

        return wrapped
