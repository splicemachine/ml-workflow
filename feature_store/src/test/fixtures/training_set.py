from sqlalchemy.sql.elements import TextClause

from .conftest import *
from sqlalchemy.types import Float, DateTime
from json import dumps
from shared.models.feature_store_models import TrainingView, TrainingViewKey, TrainingSet, TrainingSetFeature
from .feature_set import create_deployed_fset, cleanup

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
def create_training_set(create_deployed_fset):
    """
    Gets a database session as a fixture, drops and recreates all FS tables, and adds a single FeatureSet entry
    to the database, and a Feature Set Key (as to emulate the existence of an undeployed feature set)
    """
    logger.info("getting postgres database")
    sess = create_deployed_fset

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
        sql_text=sql
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
        # Base.metadata.drop_all(sess.get_bind(), tables=[raw_table])
        sess.execute('truncate table featurestore.training_set_feature')
        sess.execute('truncate table featurestore.training_set')
        sess.execute('truncate table featurestore.training_view_key')
        sess.execute('truncate table featurestore.training_view')
        sess.commit()

