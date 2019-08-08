from sm_mlflow.tracking import SpliceMachineTrackingStore
from sm_mlflow.artifact_repo import SpliceMachineArtifactStore
from mlmanager_lib.database.constants import Database


def add_schemas_to_tables(tables: tuple) -> None:
    """
    These tables should be under the MLMANAGER
    schema in Splicemachine, not under the default
    schema
    :param tables: (list) SQLAlchemy table objects
        add the schema to
    # FIXME: Broken!
    """
    for table in tables:
        table.__table_args__ += ({'schema': Database.database_schema},)
