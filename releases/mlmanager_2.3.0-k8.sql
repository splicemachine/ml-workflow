ALTER TABLE MLMANAGER.ARTIFACTS ADD COLUMN DATABASE_BINARY BLOB(2G);
insert into  MLMANAGER.ARTIFACTS (run_uuid, name, database_binary, "size", "binary", FILE_EXTENSION)  --splice-properties insertMode=UPSERT 
select a.run_uuid, b.name, a.model, b."size", b."binary", b.file_extension from mlmanager.models a
join mlmanager.tags t on a.run_uuid=t.run_uuid
join mlmanager.artifacts b on b.name=t."value" and b.run_uuid=t.run_uuid
where t."key" = 'splice.model_name';
drop view mlmanager.live_model_status;
rename table mlmanager.model_metadata to mlmanager.database_deployed_metadata;
create view mlmanager.live_model_status as 
SELECT mm.run_uuid,
           mm.action,
           CASE
               WHEN ((sta.tableid IS NULL
                      OR st.triggerid IS NULL
                      OR (mm.triggerid_2 IS NOT NULL
                          AND mm.triggerid IS NULL))
                     AND mm.action = 'DEPLOYED') THEN 'Table or Trigger Missing'
               ELSE mm.action
           END AS deployment_status,
           mm.tableid,
           mm.trigger_type,
           mm.triggerid,
           mm.triggerid_2,
           mm.db_env,
           mm.db_user,
           mm.action_date
    FROM MLMANAGER.DATABASE_DEPLOYED_METADATA mm
    LEFT OUTER JOIN sys.systables sta USING (tableid)
    LEFT OUTER JOIN sys.systriggers st ON (mm.triggerid = st.triggerid)
    LEFT OUTER JOIN sys.systriggers st2 ON (mm.triggerid_2 = st2.triggerid);
