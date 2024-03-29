CREATE TABLE "MLMANAGER"."JOBS_TEMP" (
"ID" INTEGER NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY
,"timestamp" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
,"HANDLER_NAME" VARCHAR(100) NOT NULL
,"STATUS" VARCHAR(100)
,"LOGS" VARCHAR(24000)
,"PAYLOAD" CLOB(2147483647) NOT NULL
,"user" VARCHAR(100) NOT NULL
,"MLFLOW_URL" VARCHAR(5000)
,"TARGET_SERVICE" VARCHAR(100) ) ;

INSERT INTO MLMANAGER.JOBS_TEMP ("ID","timestamp","HANDLER_NAME","STATUS","LOGS","PAYLOAD","user","MLFLOW_URL","TARGET_SERVICE")
SELECT "ID",TIMESTAMP("timestamp"),"HANDLER_NAME","STATUS","LOGS","PAYLOAD","user","MLFLOW_URL","TARGET_SERVICE" FROM MLMANAGER.JOBS;

DROP TABLE MLMANAGER.JOBS;
SET SCHEMA MLMANAGER;
RENAME TABLE JOBS_TEMP TO JOBS;

ALTER TABLE MLMANAGER.JOBS ADD CONSTRAINT mlmanager_job_status_check CHECK (STATUS IN ('PENDING', 'SUCCESS', 'RUNNING', 'FAILURE'));
ALTER TABLE MLMANAGER.JOBS ADD CONSTRAINT mlmanager_job_handler_kf FOREIGN KEY ("HANDLER_NAME") REFERENCES "MLMANAGER"."HANDLERS"("NAME") ON UPDATE NO ACTION ON DELETE NO ACTION;
