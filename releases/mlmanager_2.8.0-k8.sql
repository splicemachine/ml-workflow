ALTER TABLE MLManager.Artifacts ALTER COLUMN file_extension SET DATA TYPE VARCHAR(25);
ALTER TABLE MLManager.Artifacts ADD COLUMN artifact_path Varchar(250);

-- Moving cardinality to a new table (Feature_Stats)
ALTER TABLE FeatureStore.Feature DROP COLUMN "cardinality";

-- Can't drop/rename a table that is the target of a constraint, can't remove a constraint you don't have the name of, so you have to remove the whole table - ripple effect ensues
CREATE TABLE FEATURESTORE.DEPLOYMENT_FEATURE_STATS_TMP (
    MODEL_SCHEMA_NAME VARCHAR(128) NOT NULL,
    MODEL_TABLE_NAME VARCHAR(128) NOT NULL,
    FEATURE_ID INTEGER NOT NULL,
    MODEL_START_TS TIMESTAMP,
    MODEL_END_TS TIMESTAMP,
    FEATURE_CARDINALITY INTEGER,
    FEATURE_HISTOGRAM CLOB(2147483647),
    FEATURE_MEAN DECIMAL(31,0),
    FEATURE_MEDIAN DECIMAL(31,0),
    FEATURE_COUNT INTEGER,
    FEATURE_STDDEV DECIMAL(31,0),
    PRIMARY KEY(MODEL_SCHEMA_NAME, MODEL_TABLE_NAME, FEATURE_ID),
    CONSTRAINT fk_deployment_feature_stats_feature FOREIGN KEY (FEATURE_ID) REFERENCES FEATURESTORE.FEATURE(FEATURE_ID) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FEATURESTORE.DEPLOYMENT_FEATURE_STATS_TMP (model_schema_name, model_table_name, feature_id, model_start_ts, model_end_ts, feature_cardinality, feature_histogram, feature_mean, feature_median, feature_count, feature_stddev)
SELECT model_schema_name, model_table_name, feature_id, model_start_ts, model_end_ts, feature_cardinality, feature_histogram, feature_mean, feature_median, feature_count, feature_stddev
FROM FEATURESTORE.DEPLOYMENT_FEATURE_STATS_TMP;
DROP TABLE FEATURESTORE.DEPLOYMENT_FEATURE_STATS;
RENAME TABLE FEATURESTORE.DEPLOYMENT_FEATURE_STATS_TMP to DEPLOYMENT_FEATURE_STATS;

-- We do this for the history table instead of just dropping and adding columns because it brings very weird behavior
CREATE TABLE FEATURESTORE.DEPLOYMENT_HISTORY_TMP (
    MODEL_SCHEMA_NAME VARCHAR(128) NOT NULL,
    MODEL_TABLE_NAME VARCHAR(128) NOT NULL,
    ASOF_TS TIMESTAMP NOT NULL,
    TRAINING_SET_ID INTEGER,
    TRAINING_SET_VERSION BIGINT,
    RUN_ID VARCHAR(32),
    LAST_UPDATE_TS TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    LAST_UPDATE_USERNAME VARCHAR(128) NOT NULL DEFAULT CURRENT_USER,
    PRIMARY KEY(MODEL_SCHEMA_NAME, MODEL_TABLE_NAME, ASOF_TS),
    CONSTRAINT fk_deployment_history_runs FOREIGN KEY (RUN_ID) REFERENCES MLMANAGER.RUNS(RUN_UUID) ON UPDATE NO ACTION ON DELETE NO ACTION
);
INSERT INTO FeatureStore.DEPLOYMENT_HISTORY_TMP (model_schema_name, model_table_name, asof_ts, training_set_id, run_id, last_update_username, last_update_ts)
SELECT model_schema_name, model_table_name, asof_ts, training_set_id, run_id, last_update_username, last_update_ts
FROM FeatureStore.Deployment_History;
DROP TABLE featurestore.deployment_history;
RENAME TABLE FeatureStore.DEPLOYMENT_HISTORY_TMP to DEPLOYMENT_HISTORY;

DROP TABLE FeatureStore.Training_Set_Feature_Stats;

CREATE TRIGGER FeatureStore.deployment_historian
AFTER UPDATE
ON FeatureStore.deployment
REFERENCING OLD AS od
FOR EACH ROW
INSERT INTO FeatureStore.deployment_history ( model_schema_name, model_table_name, asof_ts, training_set_id, training_set_version, run_id, last_update_ts, last_update_username)
VALUES ( od.model_schema_name, od.model_table_name, CURRENT_TIMESTAMP, od.training_set_id, od.training_set_version, od.run_id, od.last_update_ts, od.last_update_username);

DROP TABLE FeatureStore.Training_Set_Label_Stats;
DROP TABLE FeatureStore.Deployment_Feature_Stats;
