"""
SQLAlchemy Tables for MLFlow
that are not specified in their source code
"""
from mlflow.store.tracking.dbmodels.models import SqlRun
from mlflow.store.db.base_sql_model import Base
from sqlalchemy import Column, String, Binary, Integer, PrimaryKeyConstraint, ForeignKey
from sqlalchemy.orm import relationship, backref

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Amrit Baveja", "Murray Brown", "Monte Zweben"]

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
    binary: Column = Column(Binary, nullable=False)
    run: relationship = relationship(SqlRun, backref=backref('artifacts', cascade='all'))
    file_extension: Column = Column(String(10), nullable=False)

    __table_args__: tuple = (
        PrimaryKeyConstraint('run_uuid', 'name', name='artifact_pk'),
    )


class Models(Base):
    """
    Table for storing deployed models into the DB which are serialized MLeap models
    """
    __tablename__: str = "models"
    run_uuid: Column = Column(String(32), ForeignKey(SqlRun.run_uuid), primary_key=True)
    model: Column = Column(Binary, nullable=False)
    library: Column = Column(String(32), nullable=False)
    version: Column = Column(String(32), nullable=False)
    run: relationship = relationship(SqlRun, backref=backref('models', cascade='all'))
    __table_args__: tuple = (
        PrimaryKeyConstraint('run_uuid', name='models_pk'),
    )
