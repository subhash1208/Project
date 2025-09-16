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
     num_records              NUMBER(19, 0) NOT NULL,
     num_missing              NUMBER(19, 0) NOT NULL,
     num_invalid              NUMBER(19, 0) NOT NULL,
     mean                     NUMBER(30, 4),
     standard_deviation       NUMBER(30, 4),
     min                      NUMBER(30, 4),
     median                   NUMBER(30, 4),
     fifth_percentile         NUMBER(30, 4),
     first_decile             NUMBER(30, 4),
     first_quartile           NUMBER(30, 4),
     third_quartile           NUMBER(30, 4),
     ninth_decile             NUMBER(30, 4),
     ninty_fifth_percentile   NUMBER(30, 4),
     max                      NUMBER(30, 4),
     num_approx_unique_values NUMBER(19, 0) NOT NULL,
     distinctness             NUMBER(19, 4),
     entropy                  NUMBER(19, 4),
     unique_value_ratio       NUMBER(19, 4),
     rec_crt_dt               VARCHAR2(255 byte)
  );

CREATE or REPLACE VIEW &TABLE_NAME
AS
SELECT * FROM (
    SELECT
        a.*,
        row_number() over (partition by database_name, table_name, column_name, partition_spec, partition_value, subpartition_spec, subpartition_value order by rec_crt_dt desc) as latest 
    FROM &TABLE_NAME_STG a) b
WHERE latest = 1