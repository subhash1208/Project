CREATE TABLE &TABLE_NAME
  (
     APPLICATION_ID           VARCHAR2(255 byte),
     ATTEMPT_ID               VARCHAR2(255 byte) DEFAULT '1',
     APPLICATION_NAME         VARCHAR2(255 byte),
     XML_INPUT_CONFIG         CLOB,
     JOB_STATUS               VARCHAR2(255 byte),
     JOB_STATUS_CODE          NUMBER(1),
     FAILED_TABLES            CLOB,
     MSG                      CLOB,
     IS_ERROR                 NUMBER(1)          DEFAULT 0,
     LOG_DATE                 DATE               DEFAULT SYSDATE
  )