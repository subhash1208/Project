-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_ins_load_stus_archive;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_ins_load_stus_archive (
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

INSERT INTO TABLE ${__BLUE_MAIN_DB__}.t_ins_load_stus_archive PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    ins.*
FROM 
    ${__BLUE_MAIN_DB__}.t_ins_load_stus ins;
--WHERE
--    environment = '${environment}'
--DISTRIBUTE BY environment;