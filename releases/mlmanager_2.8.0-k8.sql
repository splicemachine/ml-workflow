ALTER TABLE MLManager.Artifacts ALTER COLUMN file_extension SET DATA TYPE VARCHAR(25);

-- Moving cardinality to a new table (Feature_Stats)
ALTER TABLE FeatureStore.Feature DROP COLUMN "cardinality";
