ALTER TABLE MLManager.Artifacts ALTER COLUMN file_extension SET DATA TYPE VARCHAR(25);

ALTER TABLE FeatureStore.Deployment DROP COLUMN training_set_start_ts;
ALTER TABLE FeatureStore.Deployment DROP COLUMN training_set_end_ts;
ALTER TABLE FeatureStore.Deployment DROP COLUMN training_set_create_ts;
ALTER TABLE FeatureStore.Deployment ADD COLUMN training_set_version BIGINT;

ALTER TABLE FeatureStore.Deployment_History DROP COLUMN training_set_start_ts;
ALTER TABLE FeatureStore.Deployment_History DROP COLUMN training_set_end_ts;
ALTER TABLE FeatureStore.Deployment_History DROP COLUMN training_set_create_ts;
ALTER TABLE FeatureStore.Deployment_History ADD COLUMN training_set_version BIGINT;

ALTER TABLE FeatureStore.Training_Set_Feature_Stats DROP COLUMN training_set_start_ts;
ALTER TABLE FeatureStore.Training_Set_Feature_Stats DROP COLUMN training_set_end_ts;
ALTER TABLE FeatureStore.Training_Set_Feature_Stats DROP COLUMN training_set_create_ts;
ALTER TABLE FeatureStore.Training_Set_Feature_Stats ADD COLUMN training_set_version BIGINT;

ALTER TABLE FeatureStore.Training_Set_Label_Stats DROP COLUMN training_set_start_ts;
ALTER TABLE FeatureStore.Training_Set_Label_Stats DROP COLUMN training_set_end_ts;
ALTER TABLE FeatureStore.Training_Set_Label_Stats DROP COLUMN training_set_create_ts;
ALTER TABLE FeatureStore.Training_Set_Label_Stats ADD COLUMN training_set_version BIGINT;
