"""
SQLAlchemy Tables for MLFlow
that are not specified in their source code
"""
from mlflow.store.tracking.dbmodels.models import SqlRun
from .models import ENGINE, Base
from sqlalchemy import Column, String, Integer, LargeBinary, PrimaryKeyConstraint, ForeignKey, DateTime, Table
from sqlalchemy.orm import relationship, backref
from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base
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

# Reflection for sys tables
m = MetaData(schema='SYS')
a_base = automap_base(metadata=m)
a_base.prepare(ENGINE, reflect=True)
Triggers = a_base.classes.systriggers
Users = a_base.classes.sysusers
Tables = a_base.classes.systables

# triggers: Table = Table('sys.systriggers', Base.metadata)#, autoload=True, autoload_with=ENGINE)
# users: Table = Table('sys.sysusers', Base.metadata)#, autoload=True, autoload_with=ENGINE)
# tables: Table = Table('sys.systables', Base.metadata)#, autoload=True, autoload_with=ENGINE)

class ModelMetadata(Base):
    """
    Table for storing metadata information about the deployed models.
    """
    __tablename__: str = "model_metadata"
    run_uuid: Column = Column(String(32), ForeignKey(SqlRun.run_uuid), primary_key=True)
    status: Column = Column(String(50), nullable=False)
    deployed_to: Column = Column(String(250), ForeignKey(Tables.tableid), nullable=False) #FIXME: foreign key sys.systables
    trigger_id: Column = Column(String(250), ForeignKey(Triggers.triggerid), nullable=False) #FIXME: foreign key sys.systriggers
    trigger_id_2: Column = Column(String(250), ForeignKey(Triggers.triggerid), nullable=True) #FIXME: foreign key sys.systriggers
    db_env: Column = Column(String(100), nullable=True) # Dev, QA, Prod etc
    deployed_by: Column = Column(String(250), ForeignKey(Users.username), nullable=False) #FIXME: foreign key sys.sysusers
    deployed_date: Column = Column(DateTime, default=datetime.now(tz=pytz.utc), nullable=False)

    run: relationship = relationship(SqlRun, backref=backref('model_metadata', cascade='all'))
    deploy_endpoint: relationship = relationship(Tables, backref=backref('model_metadata', cascade_backefs=False))
    trigger_1: relationship = relationship(Triggers, backref=backref('model_metadata', cascade_backefs=False))
    trigger_2: relationship = relationship(Triggers, backref=backref('model_metadata', cascade_backefs=False))
    deploy_user: relationship = relationship(Users, backref=backref('model_metadata', cascade_backefs=False))


