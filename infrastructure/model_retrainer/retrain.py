import logging
import pickle
from os import environ as env_vars

LOGGING_FORMAT = "%(levelname)s %(asctime)s.%(msecs)03d - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=LOGGING_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("loader")


def main():
    """
    Load the Retrainer implementation, create the running contexts,
    and run the retraining logic.
    """
    mount_path = env_vars.get('MOUNT_PATH', '/var/run/model')
    retrainer = 'retrainer.pkl'
    try:
        logger.info("Loading the retrainer")
        retrainer = pickle.load(open(f'{mount_path}/{retrainer}', 'rb'))
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
