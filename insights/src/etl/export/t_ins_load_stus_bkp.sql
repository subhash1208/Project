-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_ins_load_stus_backup;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_ins_load_stus_backup (
    clnt_obj_id string,
    lst_extrc_wk_cd string,
    tot_mtrcs int,
    tot_clnt_int_ins int,
    tot_clnt_int_bm_ins int,
    tot_clnt_ext_bm_ins int,
    lst_ins_load_dt DATE,
    db_schema string,
    environment string
) USING PARQUET 
PARTITIONED BY (environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');


INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_ins_load_stus_backup PARTITION(environment)
SELECT 
 clnt_obj_id,
 lst_extrc_wk_cd,
 tot_mtrcs,
 tot_clnt_int_ins,
 tot_clnt_int_bm_ins,
 tot_clnt_ext_bm_ins,
 lst_ins_load_dt,
 db_schema,
 environment
 FROM(
 SELECT 
  ils.clnt_obj_id,
  lst_extrc_wk_cd,
  tot_mtrcs,
  tot_clnt_int_ins,
  tot_clnt_int_bm_ins,
  tot_clnt_ext_bm_ins,
  lst_ins_load_dt,
  ils.db_schema,
  ils.environment
  FROM ${__BLUE_MAIN_DB__}.t_ins_load_stus ils
 ) 
