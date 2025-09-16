CREATE TABLE spark_extractor_increment_bounds (
    application_id         VARCHAR2(255 BYTE),
    env_name               VARCHAR2(100 BYTE),
    sql_name               VARCHAR2(255 BYTE),
    incremental_start_date   TIMESTAMP,
    incremental_end_date     TIMESTAMP DEFAULT current_timestamp(6),
    etl_load_ts            TIMESTAMP(6) DEFAULT current_timestamp(6)
)
