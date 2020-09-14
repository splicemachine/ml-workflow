CREATE TABLE MLMANAGER.model_metadata (
 	run_uuid VARCHAR(32) NOT NULL,
 	action VARCHAR(50) NOT NULL,
 	tableid VARCHAR(250) NOT NULL,
 	trigger_type VARCHAR(250) NOT NULL,
 	triggerid VARCHAR(250) NOT NULL,
 	triggerid_2 VARCHAR(250),
 	db_env VARCHAR(100),
 	db_user VARCHAR(250) NOT NULL,
 	action_date TIMESTAMP NOT NULL,
 	PRIMARY KEY (run_uuid, tableid),
 	FOREIGN KEY(run_uuid) REFERENCES mlmanager.runs (run_uuid)
 );

CREATE VIEW mlmanager.live_model_status AS
select mm.RUN_UUID, mm.action,
CASE when ((sta.tableid is null or st.triggerid is NULL or (mm.TRIGGERID_2 is not NULL and st2.triggerid is NULL)) and mm.ACTION='DEPLOYED')
then 'Table or Trigger Missing' else mm.ACTION
end as deployment_status,
mm.TABLEID, mm.TRIGGER_TYPE, mm.TRIGGERID, mm.TRIGGERID_2, mm.DB_ENV, mm.db_user, mm.action_date
from mlmanager.model_metadata mm
left outer join sys.systables sta using (tableid)
left outer join sys.systriggers st on (mm.triggerid=st.triggerid)
left outer join sys.systriggers st2 on (mm.triggerid_2=st2.triggerid);
