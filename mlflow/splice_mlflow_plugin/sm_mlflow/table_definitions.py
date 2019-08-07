from mlflow.store.dbmodels.models import SqlRun
from sqlalchemy import Column, String, Binary, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

SqlRun = SqlRun  # TODO remove! just for intellij to not remove import

Base = declarative_base()


class SqlArtifact(Base):
    __tablename__: str = "experiments"

    run_uuid: Column = Column(String(100), nullable=False)
    path: Column = Column(String(500), nullable=False)
    # in Python 2, this object passed to this must be of type
    # bytearray as the bytes object is an alias for str. However,
    # in Python 3, the bytes object can be passed in (byte stream)
    binary: Column = Column(Binary, nullable=False)
    run: relationship = relationship('SqlRun', backref=backref('artifacts', cascade='all'))

    __table_args__: tuple = (
        PrimaryKeyConstraint('run_uuid', 'path', name='artifact_pk'),
    )
