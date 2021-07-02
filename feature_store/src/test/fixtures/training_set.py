from json import dumps

from sqlalchemy.sql.elements import TextClause
from sqlalchemy.types import DateTime, Float

from mlflow.store.tracking.dbmodels.models import SqlExperiment, SqlRun
from shared.models.feature_store_models import (Deployment, TrainingSet,
                                                TrainingSetFeature,
                                                TrainingView, TrainingViewKey)

from .conftest import *
from .feature_set import (cleanup, create_deployed_fset,
                          create_fset_with_features)


@pytest.fixture(scope='function')
def create_schema(get_my_session):
    """
    Gets a scoped session, creates a test schema and yields
    """
    logger.info("getting postgres database")
    sess = get_my_session

    cleanup(sess)

    logger.info("Done. Creating new schema")
    sess.execute('create schema if not exists test_fs')
    sess.commit()

    logger.info('Done')
    yield sess

@pytest.fixture(scope='function')
def create_training_set(create_fset_with_features):
    """
    Gets a database session as a fixture, drops and recreates all FS tables, and adds a single FeatureSet entry
    to the database, and a Feature Set Key (as to emulate the existence of an undeployed feature set)
    """
    logger.info("getting postgres database")
    sess = create_fset_with_features

    # cleanup(sess)

    # logger.info('creating table for training view')
    # raw_table = Table(
    #     'JOIN_TABLE',Base.metadata,
    #     Column('pk', Integer, primary_key = True),
    #     Column('moment_key', Integer),
    #     Column('ts', DateTime, server_default=(TextClause("CURRENT_TIMESTAMP"))),
    #     schema='splice',
    #     extend_existing=True
    # )
    # Base.metadata.create_all(sess.get_bind(), tables=[raw_table])
    # for i in range(1,6):
    #     ins = raw_table.insert().values(pk=i, moment_key=i)
    #     sess.engine.execute(ins)

    logger.info('Creating Training View')
    sql = '''select
    ts,pk,moment_key
    from splice.join_table
    '''
    sess.add(TrainingView(
        view_id=1,
        name='test_vw',
        description='a test view',
        sql_text=sql,
        ts_column = 'ts_col'
    ))
    sess.flush()
    logger.info("adding tvw keys")
    sess.add(TrainingViewKey(
        view_id=1,
        key_column_name='MOMENT_KEY',
        key_type='J'
    ))
    sess.add(TrainingViewKey(
        view_id=1,
        key_column_name='PK',
        key_type='P'
    ))
    logger.info('Adding training set')
    sess.add(TrainingSet(
        training_set_id=1,
        name='ts1',
        view_id=1
    ))
    sess.flush()
    logger.info('Adding training set feature')
    sess.add(TrainingSetFeature(
        training_set_id=1,
        feature_id=1,
    ))
    sess.commit()
    try:
        yield sess
    finally:
        sess.commit()


@pytest.fixture(scope='function')
def create_deployment(create_training_set):
    """
    Creates a deployment entry from the existing training view
    """
    exp = SqlExperiment(experiment_id=0,name='default')
    run = SqlRun(run_uuid='45d02ded28f7', experiment_id=0)
    sess = create_training_set
    sess.add(exp)
    sess.add(run)
    sess.flush()
    d = Deployment(
        model_schema_name='splice',
        model_table_name='deployment_table',
        training_set_id = 1,
        run_id = '45d02ded28f7'
    )
    sess.add(d)
    sess.commit()
    try:
        yield sess
    finally:
        sess.commit()

    # sql = """
    # insert into featurestore.deployment
    #     (MODEL_SCHEMA_NAME,MODEL_TABLE_NAME,TRAINING_SET_ID,TRAINING_SET_START_TS,
    #     TRAINING_SET_END_TS,TRAINING_SET_CREATE_TS,RUN_ID)
    # values ('splice','deployment_table',1,timestamp('1990-01-01 00:00:00'), current_timestamp,
    # current_timestamp, '45d02ded28f7');
    # """
