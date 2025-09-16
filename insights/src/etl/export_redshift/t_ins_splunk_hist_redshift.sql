-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_ins_splunk_hist_redshift;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_ins_splunk_hist_redshift (
    clnt_obj_id STRING,
    pers_obj_id STRING,
    mtrc_ky INT,
    mtrc_hits_cnt INT,
    rec_crt_ts DATE,
    db_schema STRING,
    environment string
) USING PARQUET 
PARTITIONED BY (environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');



INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_ins_splunk_hist_redshift PARTITION(environment)
SELECT
    clnt_obj_id,
    pers_obj_id,
    mtrc_ky,
    mtrc_hits_cnt,
    rec_crt_ts,
    db_schema,
    environment
FROM(
SELECT
    ish.clnt_obj_id,
    pers_obj_id,
    mtrc_ky,
    mtrc_hits_cnt,
    rec_crt_ts,
    tcm.db_schema,
    tcm.environment
 FROM ${__BLUE_MAIN_DB__}.t_ins_splunk_hist ish
  INNER JOIN
  ${__BLUE_RAW_DB__}.dwh_t_clnt_mapping_redshift tcm
  ON
  ish.clnt_obj_id = tcm.clnt_obj_id
  )
