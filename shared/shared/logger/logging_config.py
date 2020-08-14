#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Custom Logging Configuration so we can control Logging and use
Loguru for increased performance and flexibility
"""
import logging
import time
from contextlib import contextmanager
from json import load as read_json_file
from os import environ as env_vars
from sys import stdout

from loguru import logger as loguru_logger
from pkg_resources import resource_filename


class InterceptHandler(logging.Handler):
    """
    Intercept logging calls (with standard logging) into our Loguru Sink
    See: https://github.com/Delgan/loguru#entirely-compatible-with-standard-logging
    """

    def emit(self, record):
        """
        Intercept a record into the loguru sink
        :param record: record to intercept
        """
        level = loguru_logger.level(record.levelname).name
        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        log = loguru_logger.bind(request_id='app')
        log.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


class SpliceLogger:
    """
    Custom Logger for Splice to simplify configuration options
    """
    # List of standard logging handlers we should intercept and redirect to our loguru sink
    INTERCEPTION_TARGETS = ['gunicorn.error', 'gunicorn.access', 'boto3', 'werkzeug', 'botocore', 'boto', 'gunicorn',
                            'sqlalchemy.engine', 'alembic', 'mlflow_handler', 'sqlalchemy.log', 'bobby', 'director']

    def __init__(self, config_path: str, debug=env_vars['MODE'] == 'development'):
        """
        :param config_path: path to json configuration for logger
        :param debug: whether in testing mode (default=False)
        """
        self.debug = bool(debug)
        with open(config_path) as config_file:
            self.config: dict = read_json_file(config_file)['development' if debug else 'production']

    @staticmethod
    def replace_info_in_format(logging_format):
        """
        Format task name and framework name into the logging format
        :param logging_format: logging format to replace into
        :return: formatted logging format
        """
        return logging_format \
            .replace("INSERT_TASK_NAME", env_vars.get("TASK_NAME", "Unspecified Task")) \
            .replace("INSERT_FRAMEWORK_NAME", env_vars.get("FRAMEWORK_NAME", "Unspecified Framework Name"))

    def create_logger(self):
        """
        Create Loguru Logger for FastAPI
        :return: logger object
        """
        loguru_logger.remove()
        if self.config.get('console'):
            self.config['console']['format'] = SpliceLogger.replace_info_in_format(self.config['console']['format'])
            loguru_logger.add(stdout, **self.config['console'])
        if self.config.get("logfile"):
            self.config['logfile']['format'] = SpliceLogger.replace_info_in_format(self.config['logfile']['format'])
            loguru_logger.add(self.config['logfile'].pop("path"), **self.config['logfile'])

        logging.basicConfig(handlers=[InterceptHandler()], level=logging.DEBUG)

        for module in SpliceLogger.INTERCEPTION_TARGETS:
            module_logger = logging.getLogger(module)
            module_logger.propagate = False
            module_logger.handlers = [InterceptHandler()]

        return loguru_logger.bind(request_id=None, method=None)


logger = SpliceLogger(config_path=resource_filename('shared', 'logger/logging.json')).create_logger()


@contextmanager
def log_operation_status(operation, logger_obj=logger):
    """
    Log when an operation starts and finishes
    :param operation: the operation this is managing
    :param logger_obj: override the logger
    """
    # noinspection PyBroadException
    try:
        start_time = time.time()
        logger_obj.info(f"Starting '{operation}'...", send_db=True)
        yield
        logger_obj.info(f"Done with '{operation}' [in {(time.time() - start_time) * 1000} ms]", send_db=True)
    except Exception:
        logger_obj.exception(f"Failed to execute '{operation}'", send_db=True)
        raise
