from .base_modifier_handler import BaseModifierHandler

from shared.services.handlers import HandlerNames

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class EnableServiceHandler(BaseModifierHandler):
    """
    Handle ENABLE_HANDLER Jobs
    """

    def __init__(self, task_id: int) -> None:
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task_id: (int) the id of the enable
            handler job to process

        """
        BaseModifierHandler.__init__(self, task_id, HandlerNames.enable_service)

    def modify(self) -> None:
        """
        Enable the handler
        """
        self.target_handler_object.enable()
