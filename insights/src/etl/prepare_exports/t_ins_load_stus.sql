-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_ins_load_stus;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_ins_load_stus (
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

--ALTER TABLE ${__GREEN_MAIN_DB__}.t_ins_load_stus DROP IF EXISTS PARTITION (environment = '${environment}');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_ins_load_stus PARTITION(environment)
SELECT 
      x_hr.clnt_obj_id,
      last_xtrct_wk_cd as lst_extrc_wk_cd,
      COALESCE(total_metrics,0) as tot_mtrcs,
      COALESCE(tot_clnt_int_ins,0) as tot_clnt_int_ins,
      COALESCE(tot_clnt_int_bm_ins,0) as tot_clnt_int_bm_ins,
      COALESCE(tot_clnt_ext_bm_ins,0) as tot_clnt_ext_bm_ins,
      cast(COALESCE(last_ins_load_date,'1900-01-01') as DATE) as lst_ins_load_dt, 
      x_hr.db_schema,
      x_hr.environment
FROM (
      SELECT 
          clnt_obj_id,
          environment,
          db_schema,
          max(clndr_wk_cd) as last_xtrct_wk_cd
      FROM 
          ${__BLUE_MAIN_DB__}.emi_base_hr_waf 
      --WHERE environment='${environment}'
      GROUP BY clnt_obj_id,db_schema,environment
     ) x_hr 
LEFT OUTER JOIN 
    (  
      SELECT 
          clnt_obj_id,
          environment,
          db_schema,
          count(distinct mtrc_ky) as total_metrics,
          SUM (CASE WHEN ins_type = 'CLIENT_INTERNAL' THEN 1 else 0 END ) as tot_clnt_int_ins,
          SUM (CASE WHEN ins_type = 'CLIENT_INTERNAL_BM' THEN 1 else 0 END ) as tot_clnt_int_bm_ins,
          SUM (CASE WHEN ins_type = 'CLIENT_VS_BM' THEN 1 else 0 END ) as tot_clnt_ext_bm_ins,
          max(rec_crt_ts) as last_ins_load_date
      FROM 
           ${__BLUE_MAIN_DB__}.t_fact_insights 
      --WHERE environment='${environment}'
      GROUP BY clnt_obj_id,db_schema,environment
    ) t_ins 
ON x_hr.clnt_obj_id = t_ins.clnt_obj_id
AND x_hr.environment = t_ins.environment
AND x_hr.db_schema = t_ins.db_schema;
--DISTRIBUTE BY x_hr.environment,x_hr.db_schema;