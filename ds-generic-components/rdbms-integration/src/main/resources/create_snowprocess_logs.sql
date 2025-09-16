CREATE TABLE &TABLE_NAME
  (
     APPLICATION_ID           VARCHAR2(255 byte),
     ACTION_TYPE              VARCHAR2(20 byte),
     ACTION                   CLOB,
     TIME_DURATION            NUMBER(19),
     STATUS                   VARCHAR2(20 byte),
     STACK_TRACE              CLOB,
     REC_CRT_TS               TIMESTAMP DEFAULT SYSTIMESTAMP
  );