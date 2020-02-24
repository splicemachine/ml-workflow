from mlflow.store.model_registry.sqlalchemy_store import SqlAlchemyStore
from sqlalchemy.orm import sessionmaker
from mlflow.store.db.utils import _get_managed_session_maker

from mlmanager_lib.logger.logging_config import logging
from mlmanager_lib.database.models import ENGINE

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2018, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"

LOGGER = logging.getLogger(__name__)


class SpliceMachineModelRegistry(SqlAlchemyStore):
    """
    Setup Model Registry Entrypoint
    """

    def __init__(self, store_uri: str = None):
        """
        Override the mlflow registry to support
        using splicemachine as a backend storage mechanism
        :param db_uri: variable containing dummy tracking uri
        """
        self.db_type: str = 'splicemachinesa'
        self.engine = ENGINE
        SessionMaker: sessionmaker = sessionmaker(bind=ENGINE)
        self.ManagedSessionMaker = _get_managed_session_maker(SessionMaker)
