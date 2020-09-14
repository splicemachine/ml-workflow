alter table mlmanager.artifacts alter column "binary" set data type BLOB(2G);
alter table mlmanager.models alter column model set data type BLOB(2G);
