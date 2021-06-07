-- Remove the foreign key constraint from FEATURESTORE.DEPLOYMENT_HISTORY to FEATURESTORE.DEPLOYMENT
-- Because the history of deployment (schema.table) shouldn't depend on the currently active deployments
ALTER TABLE FEATURESTORE.Deployment_History DROP CONSTRAINT SQLB22801410678F17C59360004FB58B750
