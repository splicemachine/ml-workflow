"""
Basic Auth Manager for FastAPI Routes
"""
from fastapi import Depends, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from shared.api.exceptions import ExceptionCodes, SpliceMachineException

security = HTTPBasic()


