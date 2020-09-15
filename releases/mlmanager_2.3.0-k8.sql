ALTER TABLE MLMANAGER.ARTIFACTS ADD COLUMN DATABASE_BINARY BLOB(2G);
insert into  MLMANAGER.ARTIFACTS (run_uuid, name, database_binary, "size", "binary", FILE_EXTENSION)  --splice-properties insertMode=UPSERT 
select a.run_uuid, b.name, a.model, b."size", b."binary", b.file_extension from mlmanager.models a
join mlmanager.tags t on a.run_uuid=t.run_uuid
join mlmanager.artifacts b on b.name=t."value" and b.run_uuid=t.run_uuid
where t."key" = 'splice.model_name';
