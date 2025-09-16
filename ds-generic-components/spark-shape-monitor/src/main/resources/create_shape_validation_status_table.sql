CREATE TABLE &TABLE_NAME_STG
  (
     application_id           VARCHAR2(255 byte),
     database_name            VARCHAR2(255 byte),
     table_name               VARCHAR2(255 byte),
     column_name              VARCHAR2(255 byte),
     column_type              VARCHAR2(255 byte),
     column_spec              VARCHAR2(255 byte),
     column_datatype          VARCHAR2(1000 byte),
     partition_spec           VARCHAR2(255 byte),
     partition_value          VARCHAR2(255 byte),
     subpartition_spec        VARCHAR2(255 byte),
     subpartition_value       VARCHAR2(255 byte),
     validation_rule          VARCHAR2(1000 byte),
     validation_status        VARCHAR2(255 byte),
     num_invalids             NUMBER(19, 0) NOT NULL,
     rec_crt_dt               VARCHAR2(255 byte)
  );

CREATE or REPLACE VIEW &TABLE_NAME
AS
SELECT * FROM (
    SELECT
        a.*,
        row_number() over (partition by database_name, table_name, column_name, partition_spec, partition_value, subpartition_spec, subpartition_value, SUBSTR(validation_rule,1,70) order by rec_crt_dt desc) as latest 
    FROM &TABLE_NAME_STG a) b
WHERE latest = 1