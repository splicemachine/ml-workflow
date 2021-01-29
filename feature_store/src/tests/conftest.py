import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from shared.logger.logging_config import logger

from ..rest_api.crud import get_db
from ..rest_api.main import APP

# Create sqlite database for testing
Base = declarative_base()
SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

# Connect to local sqlite db
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create local sqlite db
Base.metadata.create_all(engine, checkfirst=True)

# Override the SpliceDB connection with sqlite
def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


@pytest.fixture
def test_app():
    client = TestClient(APP)
    yield client

@pytest.fixture(scope='module')
def setup_feature_store():
    pass
