
-- Drop and recreate so the order of the columns matches the SQLAlchemy Class (they MUST match)
drop table if exists "MLMANAGER"."ARTIFACTS2";
CREATE TABLE "MLMANAGER"."ARTIFACTS2" (
"RUN_UUID" VARCHAR(32) NOT NULL
,"NAME" VARCHAR(500) NOT NULL
,"size" INTEGER
,"binary" BLOB(2000000000) NOT NULL
,"ARTIFACT_PATH" VARCHAR(250)
,"FILE_EXTENSION" VARCHAR(25)
,"DATABASE_BINARY" BLOB(2000000000)
, CONSTRAINT ARTIFACT_PK2 PRIMARY KEY("RUN_UUID","NAME"), CONSTRAINT fk_artifacts_runs FOREIGN KEY ("RUN_UUID") REFERENCES "MLMANAGER"."RUNS"("RUN_UUID") ON UPDATE NO ACTION ON DELETE NO ACTION) ;
insert into mlmanager.artifacts2 (run_uuid, name, "size", "binary", file_extension, database_binary, artifact_path) select run_uuid, name, "size", "binary", file_extension, database_binary, artifact_path from mlmanager.artifacts;
drop table mlmanager.artifacts;
rename mlmanager.artifacts2 to artifacts;



-- Moving cardinality to a new table (Feature_Stats)
ALTER TABLE FeatureStore.Feature DROP COLUMN "cardinality";

ALTER TABLE FeatureStore.Deployment DROP COLUMN training_set_start_ts;
ALTER TABLE FeatureStore.Deployment DROP COLUMN training_set_end_ts;
ALTER TABLE FeatureStore.Deployment DROP COLUMN training_set_create_ts;
ALTER TABLE FeatureStore.Deployment ADD COLUMN training_set_version BIGINT;


-- We do this for the history table instead of just dropping and adding columns because it brings very weird behavior
CREATE TABLE "FEATURESTORE"."DEPLOYMENT_HISTORY_TMP" (
"MODEL_SCHEMA_NAME" VARCHAR(128) NOT NULL
,"MODEL_TABLE_NAME" VARCHAR(128) NOT NULL
,"ASOF_TS" TIMESTAMP NOT NULL
,"TRAINING_SET_ID" INTEGER
,"RUN_ID" VARCHAR(32)
,"LAST_UPDATE_TS" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
,"LAST_UPDATE_USERNAME" VARCHAR(128) NOT NULL DEFAULT CURRENT_USER
,"TRAINING_SET_VERSION" BIGINT
, CONSTRAINT SQLA9D801400178F17C59360004FB58B754 PRIMARY KEY("MODEL_SCHEMA_NAME","MODEL_TABLE_NAME","ASOF_TS"), CONSTRAINT SQLB22801410678F17C59360004FB58B750 FOREIGN KEY ("MODEL_SCHEMA_NAME","MODEL_TABLE_NAME") REFERENCES "FEATURESTORE"."DEPLOYMENT"("MODEL_SCHEMA_NAME","MODEL_TABLE_NAME") ON UPDATE NO ACTION ON DELETE NO ACTION, CONSTRAINT SQLBA7841420178F17C59360004FS58B750 FOREIGN KEY ("RUN_ID") REFERENCES "MLMANAGER"."RUNS"("RUN_UUID") ON UPDATE NO ACTION ON DELETE NO ACTION) ;
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
