from .conftest import *

@pytest.fixture(scope='function')
def test_session_create(get_my_session):
    """
    Gets a database session as a fixture, drops and recreates all FS tables, and adds a single FeatureSet entry
    to the database
    """
    logger.info("getting postgres database")
    sess = get_my_session

    cleanup(sess)

    logger.info("Done. Adding feature set entry")
    fset = FeatureSet(schema_name='TEST_FS', table_name='FSET_1', description='Test Fset 1', deployed=False)
    sess.add(fset)
    sess.flush() # Get the feature_set_id (auto generated)
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

