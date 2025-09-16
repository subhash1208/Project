CREATE TABLE spark_exporter_job_logs 
  ( 
     application_id           VARCHAR2 (255 byte), 
     attempt_id               VARCHAR2 (255 byte) DEFAULT 1, 
     target_key               INTEGER NOT NULL, 
     db_partition                VARCHAR2 (255 byte), 
     db_user_name             VARCHAR2 (255 byte), 
     owner_id                 VARCHAR2 (255 byte), 
     client_identifier        VARCHAR2 (255 byte), 
     final_select_sql         CLOB, 
     final_session_setup_call VARCHAR2 (3999 byte), 
     msg                      CLOB, 
     exec_time_ms             INTEGER DEFAULT 0 NOT NULL, 
     row_count                INTEGER DEFAULT 0 NOT NULL, 
     is_error                 NUMBER (1) DEFAULT 0 NOT NULL, 
     log_date                 DATE DEFAULT SYSDATE 
  ) 