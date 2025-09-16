CREATE TABLE &tableName
  ( 
     schema_name  VARCHAR2(255 byte),
     table_name   VARCHAR2(255 byte),
     table_count  NUMBER(19, 0),
     filters      VARCHAR2(255 byte),
     execute_date DATE DEFAULT SYSDATE
  )
