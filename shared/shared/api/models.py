"""
Definitions for Flask API
"""
from typing import List

from pydantic import BaseModel

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class AuthUser(BaseModel):
    """
    Class Representing an Authed In User
    """
    username: str
    # Below attributes are not currently used, but will be used to store privileges of authorized users
    scopes: List[str] = []
    roles: List[str] = []


class APIStatuses:
    """
    Class containing valid
    HTTP Response statuses from Flask
    """
    success: str = "success"
    failure: str = "failure"
    pending: str = "submitted"
    error: str = "error"
