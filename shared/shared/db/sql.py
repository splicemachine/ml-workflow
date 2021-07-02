"""
SQL Statements for when the ORM cannot be used
"""


class SQL:
    training_set_instance = \
        """
        INSERT INTO FeatureStore.Training_Set_Instance(
            training_set_id, training_set_version, training_set_start_ts, training_set_end_ts, 
            training_set_create_ts, last_update_username) 
        values (
            {training_set_id}, {training_set_version}, '{training_set_start_ts}', '{training_set_end_ts}', 
            '{training_set_create_ts}', '{last_update_username}')
        """

    feature_update_check = \
        """
        CREATE TRIGGER <schema_name>.<feature_set_tablename>_update_check
        BEFORE UPDATE 
        ON <schema_name>.<feature_set_tablename>
        REFERENCING OLD AS OLDW NEW AS NEWW
        FOR EACH ROW
        WHEN (OLDW.LAST_UPDATE_TS > NEWW.LAST_UPDATE_TS) SIGNAL SQLSTATE '2201H' SET MESSAGE_TEXT = 'LAST_UPDATE_TS must be greater than the current row.';
        """

    deployment_feature_historian = \
        """
        CREATE TRIGGER FeatureStore.deployment_historian
        AFTER UPDATE 
        ON FeatureStore.deployment
        REFERENCING OLD AS od
        FOR EACH ROW 
        INSERT INTO FeatureStore.deployment_history ( model_schema_name, model_table_name, asof_ts, training_set_id, training_set_version, run_id, last_update_ts, last_update_username)
        VALUES ( od.model_schema_name, od.model_table_name, CURRENT_TIMESTAMP, od.training_set_id, od.training_set_version, od.run_id, od.last_update_ts, od.last_update_username)
        """

    live_status_view_selector: str = \
        """
       SELECT mm.run_uuid,
           ssa.schemaname as SCHEMA_NAME,
           sta.tablename TABLE_NAME,
           mm.action,
           CASE
               WHEN (
                    (sta.tableid IS NULL OR st.triggerid IS NULL)
                     AND mm.action = 'DEPLOYED') THEN 'Table or Trigger Missing'
               ELSE mm.action
           END AS deployment_status,
           mm.db_env,
           mm.db_user,
           mm.action_date,
           ssa.schemaid,
           mm.tableid,
           mm.triggerid,
           mm.trigger_type
    FROM mlmanager.DATABASE_DEPLOYED_METADATA mm
    LEFT OUTER JOIN sys.systables sta USING (tableid)
    LEFT OUTER JOIN sys.sysschemas ssa USING (schemaid)
    LEFT OUTER JOIN sys.systriggers st ON (mm.triggerid = st.triggerid)
        """

    get_model_table_from_run: str = """
    SELECT schema_name, table_name FROM mlmanager.live_model_status 
    WHERE run_uuid='{run_id}' 
    AND action='DEPLOYED' 
    AND deployment_status='DEPLOYED' 
    ORDER BY action_date DESC
    """

    get_deployment_status: str = 'SELECT * from MLMANAGER.LIVE_MODEL_STATUS'

    retrieve_jobs: str = \
        """
        SELECT id, handler_name FROM JOBS
        WHERE status='PENDING'
        ORDER BY "timestamp"
        """

    get_monthly_aggregated_jobs = \
        """
        SELECT MONTH(INNER_TABLE.parsed_date) AS month_1, COUNT(*) AS count_1, user_1
        FROM (
            SELECT "timestamp" AS parsed_date, "user" as user_1
            FROM JOBS
        ) AS INNER_TABLE
        WHERE YEAR(INNER_TABLE.parsed_date) = YEAR(CURRENT_TIMESTAMP)
        GROUP BY 1, 3
        """

    update_job_log = \
        """
        UPDATE JOBS SET LOGS=LOGS||:message 
        WHERE id=:task_id
        """
    add_database_deployed_metadata = \
        """
        INSERT INTO DATABASE_DEPLOYED_METADATA (
            run_uuid,action,tableid,trigger_type,triggerid,triggerid_2,db_env,db_user,action_date
        ) VALUES ('{run_uuid}','{action}','{tableid}','{trigger_type}','{triggerid}','{triggerid_2}','{db_env}',
            '{db_user}','{action_date}')
        """

    update_artifact_database_blob = \
        """
        UPDATE ARTIFACTS SET database_binary=:binary
        WHERE run_uuid='{run_uuid}' AND name='{name}'
        """

    add_feature_store_deployment = \
        """
        INSERT INTO FEATURESTORE.DEPLOYMENT(
            model_schema_name, model_table_name, training_set_id, training_set_version, run_id, last_update_username
        ) VALUES (
            '{model_schema_name}', '{model_table_name}', {training_set_id}, {training_set_version}, '{run_id}', '{last_update_username}')
        """

    update_feature_store_deployment = \
        """
        UPDATE FEATURESTORE.DEPLOYMENT
            SET 
                training_set_id={training_set_id},
                training_set_version={training_set_version},
                last_update_username='{last_update_username}',
                run_id='{run_id}'
            WHERE
                model_schema_name='{model_schema_name}' 
            AND
                model_table_name='{model_table_name}'
        """

    get_k8s_deployments_on_restart = \
        """
        SELECT "user",payload FROM MLManager.Jobs
        INNER JOIN 
           ( SELECT MLFlow_URL, max("timestamp") "timestamp" 
             FROM MLManager.Jobs 
             where HANDLER_NAME in ('DEPLOY_KUBERNETES','UNDEPLOY_KUBERNETES')  
             group by 1 ) LatestEvent using ("timestamp",MLFLOW_URL)
        where HANDLER_NAME='DEPLOY_KUBERNETES'
        """

    get_table_id = \
        """
        SELECT TABLEID FROM SYSVW.SYSTABLESVIEW WHERE TABLENAME='{table_name}' 
        AND SCHEMANAME='{schema_name}'
        """

    drop_trigger = \
        """
        DROP TRIGGER {trigger_name}
        """
