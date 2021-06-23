from shared.services.handlers import HandlerNames

from .base_modifier_handler import BaseModifierHandler

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja"]

__license__: str = "Proprietary"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"


class DisableServiceHandler(BaseModifierHandler):
    """
    Handle DISABLE_HANDLER Jobs
    """

    def __init__(self, task_id: int) -> None:
        """
        Initialize Base Handler
        constructor (set instance variables
        etc.)

        :param task_id: (int) the id of the disable
            handler job to process

        """
        BaseModifierHandler.__init__(self, task_id, HandlerNames.disable_service)

    def modify(self) -> None:
        """
        Enable the handler
        """
        self.target_handler_object.disable()
