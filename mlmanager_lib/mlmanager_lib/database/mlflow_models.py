"""
SQLAlchemy Tables for MLFlow
that are not specified in their source code
"""
from sqlalchemy_views import CreateView
from mlflow.store.tracking.dbmodels.models import SqlRun
from .models import ENGINE, Base
from sqlalchemy import Column, String, Integer, LargeBinary, PrimaryKeyConstraint, ForeignKey, DateTime, Boolean, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql import text
from typing import Dict
from datetime import datetime
import pytz

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Ben Epstein", "Murray Brown", "Monte Zweben"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Amrit Baveja"
__email__: str = "abaveja@splicemachine.com"
__status__: str = "Quality Assurance (QA)"


class SqlArtifact(Base):
    """
    This table MUST be updated to conform
    to the last alembic revision of SqlRun
    in MlFlow repo.

    See in:
    https://github.com/mlflow/mlflow/blob/master/mlflow/store/dbmodels/models.py

    What changes in that file need to be updated here?
    ------------------------
    1) Changes to the datatype/size of run_uuid. Our database
        will throw an error if the type does not exactly match.
    2) Changes to the name of run_uuid (must update in PK Constraint,
        FK constraint)
    """
    __tablename__: str = "artifacts"

    run_uuid: Column = Column(String(32), ForeignKey(SqlRun.run_uuid))
    name: Column = Column(String(500), nullable=False)
    size: Column = Column(Integer, nullable=False)
    # in Python 2, this object passed to this must be of type
    # bytearray as the bytes object is an alias for str. However,
    # in Python 3, the bytes object can be passed in (byte stream)
    binary: Column = Column(LargeBinary(length=int(2e9)), nullable=False)
    run: relationship = relationship(SqlRun, backref=backref('artifacts', cascade='all'))
    file_extension: Column = Column(String(10), nullable=False)

    __table_args__: tuple = (
        PrimaryKeyConstraint('run_uuid', 'name', name='artifact_pk'),
    )


## We haven't fully implemented reflection in our sqlalchemy driver so we can manually reflect the system tables we
## need to reference
class Models(Base):
    """
    Table for storing deployed models into the DB
    """
    __tablename__: str = "models"
    run_uuid: Column = Column(String(32), ForeignKey(SqlRun.run_uuid), primary_key=True)
    model: Column = Column(LargeBinary(length=int(2e9)), nullable=False)
    library: Column = Column(String(32), nullable=False)
    version: Column = Column(String(32), nullable=False)
    run: relationship = relationship(SqlRun, backref=backref('models', cascade='all'))
    __table_args__: tuple = (
        PrimaryKeyConstraint('run_uuid', name='models_pk'),
    )

class SysTables(Base):
    """
    System Table for managing tables
    """
    __tablename__: str = "systables"
    __table_args__: Dict[str,str] = {"schema": "sys"}
    TABLEID: Column = Column(String(1), nullable=False, primary_key=True)
    TABLENAME: Column = Column(String(100), nullable=False)
    TABLETYPE: Column = Column(String(1), nullable=False)
    SCHEMAID: Column = Column(String(1), nullable=False)
    LOCKGRANULARITY: Column = Column(String(100), nullable=False)
    VERSION: Column = Column(String(100), nullable=True)
    COLSEQUENCE: Column = Column(String(100), nullable=False)
    DELIMITED: Column = Column(Integer, nullable=True)
    ESCAPED: Column = Column(String(100), nullable=True)
    LINES: Column = Column(String(100), nullable=True)
    STORED: Column = Column(String(100), nullable=True)
    LOCATION: Column = Column(String(100), nullable=True)
    COMPRESSION: Column = Column(String(100), nullable=True)
    IS_PINNED: Column = Column(Boolean, nullable=False)
    PURGE_DELETED_ROWS: Column = Column(Boolean, nullable=False)


class SysUsers(Base):

    """
    System Table for managing users
    """
    __tablename__: str = "sysusers"
    __table_args__: Dict[str,str] = {"schema": "sys"}
    USERNAME: Column = Column(String(100), nullable=False)
    HASHINGSCHEME: Column = Column(String(5000), nullable=False, primary_key=True)
    PASSWORD: Column = Column(String(5000), nullable=False)
    LASTMODIFIED: Column = Column(DateTime, nullable=False)


class SysTriggers(Base):
    """
    System Table for managing triggers
    """
    __tablename__: str = "systriggers"
    __table_args__: Dict[str,str] = {"schema": "sys"}
    TRIGGERID: Column = Column(String(1), primary_key=True, nullable=True)
    TRIGGERNAME: Column = Column(String(1000), nullable=True)
    SCHEMAID: Column = Column(String(1), nullable=True)
    CREATIONTIMESTAMP: Column = Column(DateTime, nullable=True)
    EVENT: Column = Column(String(1), nullable=True)
    FIRINGTIME: Column = Column(String(1), nullable=True)
    TYPE: Column = Column(String(1), nullable=True)
    STATE: Column = Column(String(1), nullable=True)
    TABLEID: Column = Column(String(1), nullable=True)
    WHENSTMTID: Column = Column(String(1), nullable=False)
    ACTIONSTMTID: Column = Column(String(1), nullable=False)
    REFERENCEDCOLUMNS: Column = Column(String(5000), nullable=False)
    TRIGGERDEFINITION: Column = Column(String(5000), nullable=False)
    REFERENCINGOLD: Column = Column(Boolean, nullable=False)
    REFERENCINGNEW: Column = Column(Boolean, nullable=False)
    OLDREFERENCINGNAME: Column = Column(String(100), nullable=False)
    NEWREFERENCINGNAME: Column = Column(String(100), nullable=False)
    WHENCLAUSETEXT: Column = Column(String(5000), nullable=False)


class ModelMetadata(Base):
    """
    Table for storing metadata information about the deployed models.
    """
    __tablename__: str = "model_metadata"
    run_uuid: Column = Column(String(32), ForeignKey(SqlRun.run_uuid), primary_key=True)
    action: Column = Column(String(50), nullable=False) # Deployed, Deleted
    tableid: Column = Column(String(250), nullable=False, primary_key=True) # TableID of the deployed table
    trigger_type: Column = Column(String(250), nullable=False) # What causes prediction? INSERT/UPSERT/UPDATE/DELETE
    triggerid: Column = Column(String(250), nullable=False)
    triggerid_2: Column = Column(String(250), nullable=True) # Some models have 2 triggers
    db_env: Column = Column(String(100), nullable=True) # Dev, QA, Prod etc
    db_user: Column = Column(String(250), nullable=False) # Current user
    action_date: Column = Column(DateTime, default=datetime.now(tz=pytz.utc), nullable=False)

    run: relationship = relationship(SqlRun, backref=backref('model_metadata', cascade='all'))


live_model_status = Table('live_model_status', Base.metadata)
definition = text("""
select mm.RUN_UUID, mm.action,
CASE when ((sta.tableid is null or st.triggerid is NULL or (mm.TRIGGERID_2 is not NULL and st2.triggerid is NULL)) and mm.ACTION='DEPLOYED')
then 'Table or Trigger Missing' else mm.ACTION
end as deployment_status,
mm.TABLEID, mm.TRIGGER_TYPE, mm.TRIGGERID, mm.TRIGGERID_2, mm.DB_ENV, mm.db_user, mm.action_date

from mlmanager.model_metadata mm
left outer join sys.systables sta using (tableid)
left outer join sys.systriggers st on (mm.triggerid=st.triggerid)
left outer join sys.systriggers st2 on (mm.triggerid_2=st2.triggerid)
""")
live_model_status_view = CreateView(live_model_status, definition)
