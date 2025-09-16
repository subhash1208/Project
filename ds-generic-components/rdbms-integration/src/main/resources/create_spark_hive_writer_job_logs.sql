CREATE TABLE spark_hive_writer_job_logs 
  ( 
     application_id    VARCHAR2(255 byte), 
     attempt_id        VARCHAR2(255 byte) DEFAULT 1, 
     table_name        VARCHAR2(255 byte), 
     df_approx_size_mb INTEGER DEFAULT 0 NOT NULL, 
     table_partitions  INTEGER DEFAULT 0 NOT NULL, 
     df_partitions     INTEGER DEFAULT 0 NOT NULL, 
     write_time_secs   INTEGER DEFAULT 0 NOT NULL, 
     msg               CLOB, 
     is_error          NUMBER(1) DEFAULT 0 NOT NULL, 
     log_date          DATE DEFAULT SYSDATE 
  )