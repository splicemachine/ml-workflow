import pickle
from os import environ as env_vars
from shared.logger.logging_config import logger

MOUNT_PATH = env_vars.get('MOUNT_PATH', '/var/run/model')
RETRAINER = 'retrainer.pkl'


def main():
    try:
        logger.info("Loading the retrainer")
        retrainer = pickle.load(f'{MOUNT_PATH}/{RETRAINER}')
        logger.info("Creating the contexts for spark, splcie, mlflow, and feature store")
        retrainer._create_contexts()
        logger.info("Retraining the model")
        retrainer.retrain()
        logger.info("Done!")
    except:
        logger.exception("An error occured...")
        raise Exception from None

if __name__ == '__main__':
    main()
