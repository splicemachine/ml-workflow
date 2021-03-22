INSERT INTO featurestore.training_set (name, view_id )
SELECT name||' fraud_prevention ', view_id FROM featurestore.training_view;

INSERT INTO featurestore.training_set (name, view_id )
SELECT name||' small_model ', view_id FROM featurestore.training_view;

INSERT INTO featurestore.training_set_feature (training_set_id, feature_id, is_label )
SELECT ts.training_set_id, f.feature_id, 0 FROM featurestore.training_set ts, featurestore.feature f
where ts.name LIKE '%small_model%'
and random() <= 0.34;

INSERT INTO featurestore.training_set_feature (training_set_id, feature_id, is_label )
SELECT ts.training_set_id, f.feature_id, 0 FROM featurestore.training_set ts, featurestore.feature f
where ts.name LIKE '%fraud_prevention%'
and random() > 0.24;
