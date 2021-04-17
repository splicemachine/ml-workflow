"""
This module contains SQLAlchemy Models
used for the Queue
"""
from time import sleep
from os import environ as env

from shared.logger.logging_config import logger
from shared.services.database import SQLAlchemyClient, DatabaseSQL, DatabaseFunctions
from sqlalchemy import event, ForeignKeyConstraint, DDL, UniqueConstraint
from sqlalchemy import (Boolean, CheckConstraint, Column, ForeignKey, Integer,
                        String, Text, DateTime, Numeric)
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.functions import now as db_current_timestamp
from mlflow.store.tracking.dbmodels.models import SqlRun
from sqlalchemy import inspect as peer_into_splice_db

__author__: str = "Splice Machine, Inc."
__copyright__: str = "Copyright 2019, Splice Machine Inc. All Rights Reserved"
__credits__: list = ["Ben Epstein", "Sergio Ferragut"]

__license__: str = "Commercial"
__version__: str = "2.0"
__maintainer__: str = "Ben Epstein"
__email__: str = "bepstein@splicemachine.com"
__status__: str = "Quality Assurance (QA)"


class FeatureSet(SQLAlchemyClient.SpliceBase):
    """
    Feature Set is a collection of related features that are stored in a single table which is managed by the feature store
    This is a top level metadata artifact of the feature store that a user will interact with
    """
    __tablename__: str = "feature_set"
    feature_set_id: Column = Column(Integer, primary_key=True, autoincrement=True)
    schema_name: Column = Column(String(128), nullable=False)
    table_name: Column = Column(String(128), nullable=False)
    description: Column = Column(String(500), nullable=True)
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))
    deployed: Column = Column(Boolean, default=False)
    deploy_ts: Column = Column(DateTime, nullable=True)
    __table_args__: tuple = (
        UniqueConstraint(
            schema_name, table_name
        ),
        {'schema': 'featurestore'}
    )


class PendingFeatureSetDeployment(SQLAlchemyClient.SpliceBase):
    """
    A queue of feature sets that have been requested to be deployed, but have not been approved.
    """
    __tablename__: str = "pending_feature_set_deployment"
    feature_set_id: Column = Column(Integer, ForeignKey(FeatureSet.feature_set_id), primary_key=True)
    request_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    request_username: Column = Column(String(128), nullable=False)
    status: Column = Column(String(128), nullable=True, default='PENDING')
    approver_username: Column = Column(String(128), nullable=False)
    status_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    # Table Options Configuration
    __table_args__: tuple = (
        CheckConstraint(
            status.in_(('PENDING', 'ACCEPTED', 'REJECTED'))
        ),
        {'schema': 'featurestore'}
    )
    deployed: Column = Column(Boolean)

class FeatureSetKey(SQLAlchemyClient.SpliceBase):
    """
    Feature Set Key contains the keys of a given Feature Set table. This is bottom level metadata that a 
    user will NOT interact with. This is used by the Feature Store to maintain governance
    """
    __tablename__: str = "feature_set_key"
    __table_args__ = {'schema': 'featurestore'}
    feature_set_id: Column = Column(Integer, ForeignKey(FeatureSet.feature_set_id), primary_key=True)
    key_column_name: Column = Column(String(128), primary_key=True)
    key_column_data_type: Column = Column(String(128))
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))


class Feature(SQLAlchemyClient.SpliceBase):
    """
    A Feature is an individual data definition, either raw or transformed, that exists in a Feature Set and will
    likely be used alongside other Features as a feature vector for a model. This is a top level metadata that
    a user will interact with
    """
    __tablename__: str = "feature"
    feature_id: Column = Column(Integer, primary_key=True, autoincrement=True)
    feature_set_id: Column = Column(Integer, ForeignKey(FeatureSet.feature_set_id))
    name: Column = Column(String(128), index=True, unique=True)
    description: Column = Column(String(500), nullable=True)
    feature_data_type: Column = Column(String(255))
    feature_type: Column = Column(String(1))  # 'O'rdinal, 'C'ontinuous, 'N'ominal
    cardinality: Column = Column(Integer)  # Number of distint values, -1 if undefined
    tags: Column = Column(String(5000), nullable=True)
    attributes: Column = Column(String(5000), nullable=True)
    compliance_level: Column = Column(Integer)
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))

    # Table Options Configuration
    __table_args__: tuple = (
        CheckConstraint(
            feature_type.in_(('C', 'O', 'N'))  # no funny business allowed!
        ),
        {'schema': 'featurestore'}
    )


class TrainingView(SQLAlchemyClient.SpliceBase):
    """
    Training View is a definition of features and (optionally) a label for use in modeling. This contains SQL
    written by the user that defines the desired features and (optionally) a label. When the user wants to generate
    a Training Dataset, the Training View will be used, and the Feature Store will wrap SQL code around the
    View SQL to create a historical batch of the desired features and (optionally) label.
    This is a top level metadata that a user will interact with
    """
    __tablename__: str = "training_view"
    __table_args__ = {'schema': 'featurestore'}
    view_id: Column = Column(Integer, primary_key=True, autoincrement=True)
    name: Column = Column(String(128), nullable=False, index=True, unique=True)
    description: Column = Column(String(500), nullable=True)
    sql_text:Column = Column(Text)
    label_column: Column = Column(String(128))
    ts_column: Column = Column(String(128))
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))


class TrainingViewKey(SQLAlchemyClient.SpliceBase):
    """
    Training View Key holds the metadata about a training view, specifically the keys in the view and
    their types (either primary key or Join key). A join key is a key that's used to join the desired features
    with their feature sets (the primary key(s) of the Feature Sets. This is bottom level metadata that a user will NOT interact with.
    """
    __tablename__: str = "training_view_key"
    view_id: Column = Column(Integer, ForeignKey(TrainingView.view_id), primary_key=True)
    key_column_name: Column = Column(String(128), primary_key=True)
    key_type: Column = Column(String(1), primary_key=True)  # 'P'rimary key, 'J'oin key
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))

    # Table Options Configuration
    __table_args__: tuple = (
        CheckConstraint(
            key_type.in_(('P', 'J'))  # no funny business allowed!
        ),
        {'schema': 'featurestore'}
    )


class TrainingSet(SQLAlchemyClient.SpliceBase):
    """
    A Training Set is a Training View with a subset of desired Features
    in order to generate a dataframe for model development (or other analysis). This is not an independent table.
    This table uses the TrainingSetFeature table to define the desired features, and uses
    TrainingSetInstance to link the Training Set to a time window, in order to recreate the exact training set
    that the model used. It will also be linked to a Deployment via ID and Version (version comes from TrainingSetInstance)
    This is a top level metadata that a user will interact with
    """
    __tablename__: str = "training_set"
    __table_args__ = {'schema': 'featurestore'}
    training_set_id: Column = Column(Integer, primary_key=True, autoincrement=True)
    name: Column = Column(String(255), unique=True)
    view_id: Column = Column(Integer, ForeignKey(TrainingView.view_id))
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))


class TrainingSetInstance(SQLAlchemyClient.SpliceBase):
    """
    A Training Set Instance is an instance of a Training Set over a particular static time window. Training sets
    are dynamic in nature and change as time progresses. An instance locks in the time windows for full reproducibility.
    In a Deployment, the Training Set Instance's ID (same as the Training Set ID) and version will be linked as to know
    exactly what data was used to train the deployed model.
    """
    __tablename__: str = "training_set_instance"
    __table_args__ = {'schema': 'featurestore'}
    training_set_id: Column = Column(Integer, ForeignKey(TrainingSet.training_set_id), primary_key=True)
    training_set_version: Column = Column(Integer, primary_key=True)
    training_set_start_ts: Column = Column(DateTime)
    training_set_end_ts: Column = Column(DateTime)
    training_set_create_ts: Column = Column(DateTime)
    view_id: Column = Column(Integer, ForeignKey(TrainingView.view_id))
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))


class TrainingSetFeature(SQLAlchemyClient.SpliceBase):
    """
    A Training Set Feature is a reference to every feature in a given Training Set. These Features are 
    already existent in the Feature Store, and a unique row will indicate a particular Training Set and Feature
    within that Training Set. This is bottom level metadata that a user will NOT interact with.
    """
    __tablename__: str = "training_set_feature"
    __table_args__ = {'schema': 'featurestore'}
    training_set_id: Column = Column(Integer, ForeignKey(TrainingSet.training_set_id), primary_key=True)
    feature_id: Column = Column(Integer, ForeignKey(Feature.feature_id), primary_key=True )
    is_label: Column = Column(Boolean, default=False)
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))


class TrainingSetFeatureStats(SQLAlchemyClient.SpliceBase):
    """
    This table holds statistics about a Training Set when a model using that particular Training Set 
    (where a Training Set is defined as a Training View and Features over a particular time window) is deployed.
    This is static information as a training set does NOT change because of it's particular time window.
    This information will be available for a user post deployment for model/feature tracking and governance
    """
    __tablename__: str = "training_set_feature_stats"
    __table_args__ = {'schema': 'featurestore'}
    training_set_id: Column = Column(Integer, ForeignKey(TrainingSetInstance.training_set_id), primary_key=True)
    training_set_version: Column = Column(Integer, ForeignKey(TrainingSetInstance.training_set_id), primary_key=True)
    feature_id: Column = Column(Integer, ForeignKey(Feature.feature_id), primary_key=True)
    feature_cardinality: Column = Column(Integer)
    feature_histogram: Column = Column(Text)
    feature_mean: Column = Column(Numeric)
    feature_median: Column = Column(Numeric)
    feature_count: Column = Column(Integer)
    feature_stddev: Column = Column(Numeric)
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))


class TrainingSetLabelStats(SQLAlchemyClient.SpliceBase):
    """
    This table holds statistics about a Training Set Instance Label when a model using that particular Training Set
    (where a Training Set Instance is defined as a Training View and Features over a particular time window) is created.
    This is static information as a training set does NOT change because of it's particular time window.
    This information will be available for a user post deployment for model/feature tracking and governance. The reason
    we have a separate label table from features is for the specific case where a Training View is used, and the label
    is defined as a column from the View SQL (and is NOT a feature). In the case where there is no label, or the label
    is simply a feature, this table won't be used.
    """
    __tablename__: str = "training_set_label_stats"
    __table_args__ = {'schema': 'featurestore'}
    training_set_id: Column = Column(Integer, ForeignKey(TrainingSetInstance.training_set_id), primary_key=True)
    training_set_version: Column = Column(Integer, ForeignKey(TrainingSetInstance.training_set_id), primary_key=True)
    label_column: Column = Column(Integer, primary_key=True)
    label_cardinality: Column = Column(Integer)
    label_histogram: Column = Column(Text)
    label_mean: Column = Column(Numeric)
    label_median: Column = Column(Numeric)
    label_count: Column = Column(Integer)
    label_stddev: Column = Column(Numeric)
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))


class Deployment(SQLAlchemyClient.SpliceBase):
    """
    A Deployment is the capturing of a ML Model that was trained with a Training Set and is then deployed. This
    table captures that model, the training set used and the time window of that particular training set.
    We need the time window in order to recreate the exact dataset that the model used to train.
    """
    __tablename__: str = "deployment"
    __table_args__ = {'schema': 'featurestore'}
    model_schema_name: Column = Column(String(128), primary_key=True)
    model_table_name: Column = Column(String(128), primary_key=True)
    training_set_id: Column = Column(Integer, ForeignKey(TrainingSetInstance.training_set_id), primary_key=True)
    training_set_version: Column = Column(Integer, ForeignKey(TrainingSetInstance.training_set_id), primary_key=True)
    run_id: Column = Column(String(32), ForeignKey(SqlRun.run_uuid))
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))


class DeploymentHistory(SQLAlchemyClient.SpliceBase):
    """
    This table keeps track of deployments by managing new (replacement) deployments to particular tables. 
    A deployment is defined by the schema.table, so when a new model is deployed we keep history of the last 
    deployed model to the particular table. It is linked via a trigger on the Deployment table
    """
    __tablename__: str = "deployment_history"
    model_schema_name: Column = Column(String(128), primary_key=True)
    model_table_name: Column = Column(String(128), primary_key=True)
    asof_ts: Column = Column(DateTime, primary_key=True)
    training_set_id: Column = Column(Integer, primary_key=True)
    training_set_version: Column = Column(Integer, primary_key=True)
    run_id: Column = Column(String(32), ForeignKey(SqlRun.run_uuid))
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))

    __table_args__ = (
        ForeignKeyConstraint(
            (model_schema_name, model_table_name),
            [Deployment.model_schema_name, Deployment.model_table_name]
        ),
        {'schema': 'featurestore'}
    )


class DeploymentFeatureStats(SQLAlchemyClient.SpliceBase):
    """
    This table keeps track of feature statistics in a particular deployment. This is dynamic information and will
    be updated periodically based on a set interval (or potentially in realtime). The updating schedule of these
    stats are in progress.
    """
    __tablename__: str = "deployment_feature_stats"

    model_schema_name: Column = Column(String(128), primary_key=True)
    model_table_name: Column = Column(String(128), primary_key=True)
    feature_id: Column = Column(Integer, ForeignKey(Feature.feature_id), primary_key=True, )
    model_start_ts: Column = Column(DateTime)  # The start time of the window of calculation for statistics
    model_end_ts: Column = Column(DateTime)  # The end time of the window of calculation for statistics
    feature_cardinality: Column = Column(Integer)
    feature_histogram: Column = Column(Text)
    feature_mean: Column = Column(Numeric)
    feature_median: Column = Column(Numeric)
    feature_count: Column = Column(Integer)
    feature_stddev: Column = Column(Numeric)

    __table_args__ = (
        ForeignKeyConstraint(
            (model_schema_name, model_table_name),
            [Deployment.model_schema_name, Deployment.model_table_name]
        ),
        {'schema': 'featurestore'}
    )

class Source(SQLAlchemyClient.SpliceBase):
    """
    This table keeps track of sources of Feature Set tables. This table contains Sources that are defined in SQL, and
    are used to schedule Pipelines to continuously update those Feature Sets (typically using Airflow)
    """
    __tablename__: str = "SOURCE" # Reserved word listed in sqlalchemy so it needs to be uppercase
    source_id: Column = Column(Integer, primary_key=True, autoincrement=True)
    name: Column = Column(String(128), unique=True)
    sql_text: Column = Column(Text)
    # The name of the column from source that indicates the business time which is used for window aggregations
    event_ts_column: Column = Column(String(128))
    # The name of the column from source that we filter on to get the latest extract of data
    update_ts_column: Column = Column(String(128))
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))

    __table_args__ = (
        {'schema': 'featurestore'}
    )

class SourceKey(SQLAlchemyClient.SpliceBase):
    """
    This table holds the "primary keys" of the source query. Meaning the uniquely identifying column(s) of a Source query
    """
    __tablename__: str = "source_key"
    source_id: Column = Column(Integer, ForeignKey(Source.source_id), primary_key=True)
    key_column_name: Column = Column(String(128), primary_key=True)
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))

    __table_args__ = (
        {'schema': 'featurestore'}
    )

class Pipeline(SQLAlchemyClient.SpliceBase):
    """
    This table represents the instantiation of a Pipeline to feed a particular Feature Set from a Source. Pipelines
    contain a 1 to many mapping of Source ID to Feature Set ID. That is, 1 source can feed many feature sets, but 1 feature set
    is fed by 1 source.
    """
    __tablename__: str = "pipeline"
    feature_set_id: Column = Column(Integer, ForeignKey(FeatureSet.feature_set_id), primary_key=True)
    source_id: Column = Column(Integer, ForeignKey(Source.source_id))
    pipeline_start_ts: Column = Column(DateTime)
    pipeline_interval: Column = Column(String(10))
    backfill_start_ts: Column = Column(DateTime)
    backfill_interval: Column = Column(String(10))
    pipeline_url: Column = Column(String(1024))
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))
    __table_args__ = (
        {'schema': 'featurestore'}
    )

class PipelineOps(SQLAlchemyClient.SpliceBase):
    """
    This table holds the maximum timestamp extracted from a Source feeding a Feature Set. Every time a Pipeline runs
    for a Source -> Feature Set, this timestamp is used as the filter
    ie
        INSERT INTO FeatureSet SELECT FROM Source WHERE ts_col > extract_up_to_ts
    """
    __tablename__: str = "pipeline_ops"
    feature_set_id: Column = Column(Integer, ForeignKey(Pipeline.feature_set_id), primary_key=True)
    extract_up_to_ts: Column = Column(DateTime)
    __table_args__ = (
        {'schema': 'featurestore'}
    )

class PipelineAgg(SQLAlchemyClient.SpliceBase):
    """
    This table holds the aggregations that are performed on a Source column to create Features (via the
    FeatureAggregation class)
    """
    __tablename__: str = "pipeline_agg"
    feature_set_id: Column = Column(Integer, ForeignKey(Pipeline.feature_set_id), primary_key=True)
    feature_name_prefix: Column = Column(String(128), primary_key=True)
    column_name: Column = Column(String(128))
    agg_functions: Column = Column(String(50))
    agg_windows: Column = Column(String(255))
    agg_default_value: Column = Column(Numeric)
    last_update_ts: Column = Column(DateTime, server_default=(TextClause("CURRENT_TIMESTAMP")), nullable=False)
    last_update_username: Column = Column(String(128), nullable=False, server_default=TextClause("CURRENT_USER"))
    __table_args__ = (
        UniqueConstraint(feature_set_id, column_name),
        {'schema': 'featurestore'}
    )

def create_deploy_historian():
    @event.listens_for(DeploymentHistory.__table__, 'after_create')
    def create_feature_hisorian_trigger(*args, **kwargs):
        logger.warning("Creating historian trigger for feature store")
        SQLAlchemyClient.execute(
            DatabaseSQL.deployment_feature_historian  # Record the old feature in the feature store history table
        )


TABLES = [FeatureSet, PendingFeatureSetDeployment, FeatureSetKey, Feature, TrainingView, TrainingViewKey, TrainingSet,
          TrainingSetFeature, TrainingSetFeatureStats, Deployment, DeploymentHistory, DeploymentFeatureStats,
          Source, SourceKey, Pipeline, PipelineOps, PipelineAgg]

def create_feature_store_tables(_sleep_secs=1) -> None:
    """
    Function that creates all of the tables in a retry loop in case the database.py doesn't exist
    Tries to create the necessary tables, retrying every 30 seconds, max 10 times
    Will gracefully fail after that if no DB exists
    """
    if _sleep_secs > 500:
        raise Exception("Could not connect to database ")

    # noinspection PyBroadException
    try:
        # If we are testing with pytest, we cannot create this trigger
        # Because it causes a segmentation fault
        # if env.get('MODE') != 'TESTING':
        create_deploy_historian()
        logger.warning("Creating Feature Store Splice Tables inside Splice DB...")
        SQLAlchemyClient.SpliceBase.metadata.create_all(checkfirst=True, tables=[t.__table__ for t in TABLES])
        logger.info("Created Tables")
    except Exception:
        logger.exception(f"Encountered Error while initializing")  # logger might have failed
        logger.error(f"Retrying after {_sleep_secs} seconds...")
        sleep(_sleep_secs)
        create_feature_store_tables(_sleep_secs=_sleep_secs * 2)

def wait_for_runs_table() -> None:
    logger.info("Checking for mlmanager.runs table...")
    while not DatabaseFunctions.table_exists('mlmanager', 'runs', SQLAlchemyClient.engine):
        logger.info("mlmanager.runs does not exist. Checking again in 10s")
        sleep(10)
    logger.info("Found mlmanager.runs")
