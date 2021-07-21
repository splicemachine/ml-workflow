import pytest
import testing.postgresql
from fastapi.testclient import TestClient
from sqlalchemy import Column, Integer, String, Table, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from mlflow.store.tracking.dbmodels.models import SqlExperiment, SqlRun
from shared.db.connection import SQLAlchemyClient
from shared.logger.logging_config import logger
from shared.models.feature_store_models import (Deployment,
                                                DeploymentFeatureStats,
                                                DeploymentHistory, Feature,
                                                FeatureSet, FeatureSetKey,
                                                TrainingSet,
                                                TrainingSetFeature,
                                                TrainingSetFeatureStats,
                                                TrainingView, TrainingViewKey)

from ...main import APP

# Add SqlRun and SqlExperiment to the featurestore schema
SqlRun.__table_args__ += ({'schema': 'featurestore'},)
SqlExperiment.__table_args__ += ({'schema': 'featurestore'},)
TABLES = [t.__table__ for t in (Feature, FeatureSet, FeatureSetKey, TrainingView,
                                TrainingViewKey, TrainingSet, TrainingSetFeature,
                                TrainingSetFeatureStats, Deployment, DeploymentHistory,
                                DeploymentFeatureStats, SqlRun, SqlExperiment)]

# Create postgresql database for testing
Postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)
Base = declarative_base()

def override_get_db():
    logger.info("Creating postgres engine")

    postgresql = Postgresql()
    engine = create_engine(postgresql.url())
    engine.execute('create schema featurestore')
    engine.execute('create schema test_fs')
    SQLAlchemyClient().SpliceBase.metadata.bind = engine


    # Create local postgres session maker
    logger.info("Creating session")
    TestingSessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    ScopedSessionLocal = scoped_session(TestingSessionLocal)

    logger.info("Done")
    try:
        db = ScopedSessionLocal()
        yield db
    finally:
        db.close()
        postgresql.stop()
        Postgresql.clear_cache()

@pytest.fixture(scope='function')
def get_my_session():
    sess = override_get_db()
    yield next(sess)


def cleanup(sess):
    """
    Drops all tables and recreates them in the postgres database
    """
    logger.info('Cleanup')
    Base.metadata.drop_all(sess.get_bind(), tables=TABLES)

    logger.info("Creating tables")
    Base.metadata.create_all(sess.get_bind(), checkfirst=True, tables=TABLES)

@pytest.fixture
def test_app():
    """
    Returns a Mock fastapi testing client
    """
    client = TestClient(APP)
    yield client


