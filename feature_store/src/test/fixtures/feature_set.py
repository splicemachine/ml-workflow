from .conftest import *
from sqlalchemy.types import Float
from json import dumps

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
def create_undeployed_fset(get_my_session):
    """
    Gets a database session as a fixture, drops and recreates all FS tables, and adds a single FeatureSet entry
    to the database, and a Feature Set Key (as to emulate the existence of an undeployed feature set)
    """
    logger.info("getting postgres database")
    sess = get_my_session

    cleanup(sess)

    logger.info("Done. Adding feature set entry")
    fset = FeatureSet(schema_name='TEST_FS', table_name='FSET_1', description='Test Fset 1', deployed=False)
    sess.add(fset)
    sess.flush()

    logger.info('Done. Adding Feature Set Key')
    fset_key = FeatureSetKey(feature_set_id=fset.feature_set_id,key_column_name='pk_col', key_column_data_type='INTEGER')
    sess.add(fset_key)
    sess.flush()

    yield sess

@pytest.fixture(scope='function')
def create_deployed_fset(get_my_session):
    """
    Gets a database session as a fixture, drops and recreates all FS tables, and adds a single DEPLOYED FeatureSet entry
    to the database, and a Feature Set Key (as to emulate the existence of a deployed feature set). Also creates the
    Table under the schema/table of the fset
    """
    logger.info("getting postgres database")
    sess = get_my_session

    cleanup(sess)

    # Add fset
    logger.info("Done. Adding feature set entry")
    fset = FeatureSet(schema_name='TEST_FS', table_name='FSET_1', description='Test Fset 1', deployed=True)
    sess.add(fset)
    sess.flush()

    # Add fset key
    logger.info('Done. Adding Feature Set Key')
    fset_key = FeatureSetKey(feature_set_id=fset.feature_set_id,key_column_name='pk_col', key_column_data_type='INTEGER')
    sess.add(fset_key)
    sess.flush()

     # Add fset key
    logger.info('Done. Adding Feature')
    fset_key = Feature(feature_set_id=fset.feature_set_id,feature_id=1, name='Test_Feature', feature_data_type='INT',
                       feature_type='C')
    sess.add(fset_key)
    sess.flush()

    # Create table
    logger.info("Creating table")
    sess.execute('create schema if not exists test_fs')

    fset_1 = Table(
        'FSET_1',Base.metadata,
        Column('pk_col', Integer, primary_key = True),
        Column('name', String),
        schema='test_fs',
        extend_existing=True
    )
    Base.metadata.create_all(sess.get_bind(), tables=[fset_1])
    logger.info("Done")
    try:
        yield sess
    finally:
        # Cleanup temp table
        Base.metadata.drop_all(sess.get_bind(), tables=[fset_1])

@pytest.fixture(scope='function')
def create_fset_with_features(get_my_session):
    """
    Gets a database session as a fixture, drops and recreates all FS tables, and adds a 2 DEPLOYED FeatureSet entries
    to the database, and a Feature Set Key (as to emulate the existence of a deployed feature set). Also creates the
    Table under the schema/table of the fset, and adds features/columns to each fset
    """
    logger.info("getting postgres database")
    sess = get_my_session

    cleanup(sess)

    # Add fset
    logger.info("Done. Adding feature set entries")
    fset = FeatureSet(schema_name='TEST_FS', table_name='FSET_1', description='Test Fset 1', deployed=True)
    fset2 = FeatureSet(schema_name='TEST_FS', table_name='FSET_2', description='Test Fset 2', deployed=True)
    sess.add(fset)
    sess.add(fset2)
    sess.flush()

    # Add fset key
    logger.info('Done. Adding Feature Set Keys')
    fset_key = FeatureSetKey(feature_set_id=fset.feature_set_id,key_column_name='pk_col', key_column_data_type='INTEGER')
    fset_key2 = FeatureSetKey(feature_set_id=fset2.feature_set_id,key_column_name='pk_col2', key_column_data_type='FLOAT')
    sess.add(fset_key)
    sess.add(fset_key2)
    sess.flush()

    logger.info('Done. Adding features')
    # Add features
    feat = Feature(feature_set_id=fset.feature_set_id, name='name',description='the name of the user',
                   feature_data_type='VARCHAR(200)', feature_type='N', cardinality=1000, tags=dumps({"type":"demographic"}))
    feat2 = Feature(feature_set_id=fset2.feature_set_id, name='income',description='the income of the user',
                   feature_data_type='FLOAT', feature_type='C', tags=dumps({"type":"monetary"}))
    sess.add(feat)
    sess.add(feat2)
    sess.flush()

    # Create table
    logger.info("Done. Creating tables")
    sess.execute('create schema if not exists test_fs')

    fset_1 = Table(
        'FSET_1',Base.metadata,
        Column('pk_col', Integer, primary_key = True),
        Column('name', String),
        schema='test_fs',
        extend_existing=True
    )
    fset_2 = Table(
        'FSET_2',Base.metadata,
        Column('pk_col2', Integer, primary_key = True),
        Column('income', Float),
        schema='test_fs',
        extend_existing=True
    )
    Base.metadata.create_all(sess.get_bind(), tables=[fset_1,fset_2])
    logger.info("Done")
    try:
        yield sess
    finally:
        # Cleanup temp table
        Base.metadata.drop_all(sess.get_bind(), tables=[fset_1,fset_2])


