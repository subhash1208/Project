CREATE TABLE spark_extractor_job_logs 
  ( 
     application_id           VARCHAR2 (255 byte), 
     attempt_id               VARCHAR2 (255 byte) DEFAULT 1, 
     sql_name                 VARCHAR2 (255 byte), 
     target_key               INTEGER NOT NULL, 
     db_partition             VARCHAR2 (255 byte), 
     final_select_sql         CLOB,
     batch_number             INTEGER DEFAULT 0, 
     final_session_setup_call VARCHAR2 (3999 byte),
     task_attempt_number      INTEGER DEFAULT 0,
     task_attempt_id          INTEGER DEFAULT 0,
     extract_status           VARCHAR2 (255 byte),
     extract_status_code      NUMBER (1),
     msg                      CLOB, 
     exec_time_ms             INTEGER DEFAULT 0 NOT NULL, 
     row_count                INTEGER DEFAULT 0 NOT NULL, 
     is_error                 NUMBER (1) DEFAULT 0 NOT NULL, 
     log_date                 DATE DEFAULT SYSDATE,
     extract_attempt_id       NUMBER (1) DEFAULT 1 NOT NULL
  )