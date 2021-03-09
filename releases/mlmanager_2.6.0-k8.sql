ALTER TABLE FEATURESTORE.DEPLOYMENT ADD TRAINING_SET_CREATE_TS TIMESTAMP;
ALTER TABLE FEATURESTORE.DEPLOYMENT_HISTORY ADD TRAINING_SET_CREATE_TS TIMESTAMP;

DROP TRIGGER FeatureStore.deployment_historian;

CREATE TRIGGER FeatureStore.deployment_historian
AFTER UPDATE 
ON FeatureStore.deployment
REFERENCING OLD AS od
FOR EACH ROW 
INSERT INTO FeatureStore.deployment_history ( model_schema_name, model_table_name, asof_ts, training_set_id, training_set_start_ts, training_set_end_ts, training_set_create_ts, run_id, last_update_ts, last_update_username)
VALUES ( od.model_schema_name, od.model_table_name, CURRENT_TIMESTAMP, od.training_set_id, od.training_set_start_ts, od.training_set_end_ts, od.training_set_create_ts, od.run_id, od.last_update_ts, od.last_update_username);

DROP TABLE IF EXISTS FEATURESTORE.PENDING_FEATURE_SET_DEPLOYMENT;
CREATE TABLE FEATURESTORE.PENDING_FEATURE_SET_DEPLOYMENT (
    feature_set_id INTEGER,
    request_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    request_username VARCHAR(128) NOT NULL,
    status VARCHAR(128) CONSTRAINT status_check CHECK (status in ('PENDING', 'ACCEPTED', 'REJECTED')) DEFAULT 'PENDING',
    approver_username VARCHAR(128) NOT NULL,
    status_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    deployed BOOLEAN,
    PRIMARY KEY (feature_set_id),
    CONSTRAINT fk_feature_set_id FOREIGN KEY (feature_set_id) REFERENCES FEATURESTORE.FEATURE_SET(feature_set_id)
);

RENAME COLUMN FEATURESTORE.FEATURE.TAGS TO ATTRIBUTES;
ALTER TABLE FEATURESTORE.FEATURE ADD TAGS VARCHAR(5000);

ALTER TABLE FEATURESTORE.TRAINING_SET_FEATURE ADD IS_LABEL BOOLEAN DEFAULT FALSE;