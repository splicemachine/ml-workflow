from shared.logger.logging_config import logger
from shared.models.feature_store_models import (create_feature_store_tables,
                                                wait_for_runs_table)


def setup():
    """
    A function that runs before starting up the API server that creates all of the FeatureStore tables in a single thread
    to avoid write-write conflicts.
    """
    wait_for_runs_table()
    logger.info("Creating Feature Store Tables...")
    create_feature_store_tables()
    logger.info("Tables are created")

if __name__ == '__main__':
    setup()
