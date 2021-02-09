import pytest
import tempfile
from fastapi.testclient import TestClient
from pytest_postgresql import factories
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, session, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import event
from shared.models.feature_store_models import (Feature, FeatureSet, FeatureSetKey, TrainingView,
                                                TrainingViewKey, TrainingSet, TrainingSetFeature,
                                                TrainingSetFeatureStats, Deployment, DeploymentHistory,
                                                DeploymentFeatureStats)
from shared.services.database import SQLAlchemyClient
from mlflow.store.tracking.dbmodels.models import SqlRun, SqlExperiment
from shared.logger.logging_config import logger

from ..rest_api.crud import get_db
from ..rest_api.main import APP

# Add SqlRun to the featurestore schema
SqlRun.__table_args__ += ({'schema': 'featurestore'},)
SqlExperiment.__table_args__ += ({'schema': 'featurestore'},)
TABLES = [t.__table__ for t in (Feature, FeatureSet, FeatureSetKey, TrainingView,
                                TrainingViewKey, TrainingSet, TrainingSetFeature,
                                TrainingSetFeatureStats, Deployment, DeploymentHistory,
                                DeploymentFeatureStats, SqlRun, SqlExperiment)]

# Create sqlite database for testing
Base = declarative_base()

# Using the factory to create a postgresql instance
socket_dir = tempfile.TemporaryDirectory()
postgresql_my_proc = factories.postgresql_proc(
    port=None, unixsocketdir=socket_dir.name)
postgresql_my = factories.postgresql('postgresql_my_proc')

# Override the SpliceDB connection with sqlite
@pytest.fixture(scope='function')
def override_get_db(postgresql_my):
    logger.info("Creating postgres engine")

    def dbcreator():
        return postgresql_my.cursor().connection

    # Connect to local sqlite db
    engine = create_engine('postgresql+psycopg2://', creator=dbcreator)
    engine.execute('create schema featurestore')

    # Point SQLAlchemy Client to local engine
    SQLAlchemyClient.SpliceBase.metadata.bind = engine

    logger.info("Creating tables")
    Base.metadata.create_all(engine, checkfirst=True, tables=TABLES)

    # Create local sqlite session maker
    logger.info("Creating session")
    TestingSessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    ScopedSessionLocal = scoped_session(TestingSessionLocal)

    logger.info("Done")
    try:
        db = ScopedSessionLocal()
        yield db
    finally:
        db.close()


def cleanup(sess):
    logger.info('cleanup')
    Base.metadata.drop_all(sess.get_bind(), tables=TABLES)

    logger.info("Creating tables")
    Base.metadata.create_all(sess.get_bind(), checkfirst=True, tables=TABLES)

@pytest.fixture
def test_app():
    client = TestClient(APP)
    yield client

@pytest.fixture(scope='function')
def feature_create_1(override_get_db):
    logger.info("getting postgres database")
    sess = override_get_db

    cleanup(sess)

    logger.info("Done. Adding feature set entry")
    sess.add(FeatureSet(schema_name='TEST_FS', table_name='FSET_1', description='Test Fset 1', deployed=False))
    sess.flush()
    logger.info('Done')
    yield sess

@pytest.fixture(scope='function')
def feature_create_2(override_get_db):
    logger.info("getting postgres database")
    sess = override_get_db

    cleanup(sess)

    logger.info("Done. Adding feature set entry")
    sess.add(FeatureSet(schema_name='TEST_FS', table_name='FSET_1', description='Test Fset 1', deployed=False))
    sess.flush()
    logger.info('Done')


def get_my_session(postgresql_my):
    def dbcreator():
        return postgresql_my.cursor().connection

    # Connect to local sqlite db
    engine = create_engine('postgresql+psycopg2://', creator=dbcreator)
    engine.execute('create schema featurestore')
    # Point SQLAlchemy Client to local engine
    SQLAlchemyClient.SpliceBase.metadata.bind = engine
    TestingSessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    ScopedSessionLocal = scoped_session(TestingSessionLocal)
    sess = ScopedSessionLocal()
    try:
        yield sess
    finally:
        sess.close()
