CREATE TABLE spark_extractor_job_status
  (
     application_id             VARCHAR2 (255 byte),
     attempt_id                 NUMBER (1) DEFAULT 1,
     application_name           VARCHAR2 (255 byte),
     product_name               VARCHAR2 (255 byte),
     dxr_sql                    CLOB,
     input_sql                  CLOB,
     job_config                 CLOB,
     job_status                 VARCHAR2 (255 byte),
     job_status_code            NUMBER (1),
     msg                        CLOB,
     parent_application_id      VARCHAR2 (255 byte),
     cluster_id                 VARCHAR2 (255 byte),
     workflow_name              VARCHAR2 (255 byte),
     state_machine_execution_id VARCHAR2 (1000 byte),
     env                        VARCHAR2 (255 byte),
     job_start_time             TIMESTAMP,
     job_end_time               TIMESTAMP,
     is_error                   NUMBER (1) DEFAULT 0 NOT NULL,
     log_date                   DATE DEFAULT SYSDATE
  )