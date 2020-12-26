import logging
import pickle
from os import environ as env_vars


def build_logger():
    logger = logging.getLogger(__name__)
    logging.basicConfig()
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter('%(levelname)s %(asctime)s - %(message)s')
    logger.addHandler(console_handler)
    return logger

logger = build_logger()

def main():
    """
    Load the Retrainer implementation, create the running contexts,
    and run the retraining logic.
    """
    mount_path = env_vars.get('MOUNT_PATH', '/var/run/model')
    retrainer = 'retrainer.pkl'
    try:
        logger.info("Loading the retrainer")
        retrainer = pickle.load(f'{mount_path}/{retrainer}')
        logger.info("Creating the contexts for spark, splice, mlflow, and feature store")
        retrainer._create_contexts()
        logger.info("Retraining the model")
        retrainer.retrain()
        logger.info("RETRAINING_CONTAINER_COMPLETED")
    except Exception as e:
        logger.exception("RETRAINING_CONTAINER_FAILED")
        raise e from None


if __name__ == '__main__':
    main()
