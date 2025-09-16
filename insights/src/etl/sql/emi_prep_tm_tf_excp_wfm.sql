-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_wfm (
  clnt_obj_id STRING,
  supvr_pers_obj_id STRING,
  pers_obj_id STRING,
  qtr_cd STRING,
  mnth_cd STRING,
  excep_count_pers INT,
  norm_excep_count_pers DOUBLE,
  excp_type STRING,
  rpt_access STRING,
  db_schema STRING,
  yr_cd STRING,
  environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_wfm PARTITION(environment)
select
  /*+ COALESCE(800) */
  clnt_obj_id,
  supvr_pers_obj_id,
  pers_obj_id,
  qtr_cd,
  mnth_cd,
  SUM(excp_ind) as excep_count_pers,  --Exception count of an employee by Exception Type
  log(SUM(excp_ind)) as norm_excep_count_pers, -- log normal of excep_count_pers
  excp_type,
  rpt_access,
  db_schema,
  yr_cd,
  environment
  from (  
SELECT clnt_obj_id,
  supvr_pers_obj_id,
  pers_obj_id,
  qtr_cd,
  mnth_cd,
  excp_ind,
  excp_type,
  rpt_access,
  environment,
  db_schema,
  yr_cd      
FROM
(SELECT clnt_obj_id,
  supvr_pers_obj_id,
  pers_obj_id,
  qtr_cd,
  mnth_cd,
  excp_ind,
  excp_type,
  rpt_access,
  environment,
  db_schema,
  yr_cd
FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_ezlm_supvr
where excp_type in ('Clocked In Early','Clocked In Late','Clocked Out Late','Clocked Out Early','Unscheduled day or shift'))a
UNION ALL
SELECT clnt_obj_id,
  supvr_pers_obj_id,
  pers_obj_id,
  qtr_cd,
  mnth_cd,
  excp_ind,  
  excp_type,
  rpt_access,
  environment,
  db_schema,
  yr_cd
FROM
(SELECT clnt_obj_id,
  supvr_pers_obj_id,
  pers_obj_id,
  qtr_cd,
  mnth_cd,
  excp_ind,  
  CASE when lower(excp_type) in ('missing out punch','missing in punch')
  THEN 'Missing In Punch, Missing Out Punch'
  ELSE excp_type end as excp_type,
  rpt_access,
  environment,
  db_schema,
  yr_cd
FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_mssd_pnch_supvr
where excp_type in ('Missing Out Punch', 'Missing In Punch'))b
) excp
GROUP BY environment,db_schema,clnt_obj_id, supvr_pers_obj_id,pers_obj_id,excp_type,rpt_access,yr_cd,qtr_cd,mnth_cd
GROUPING SETS((environment,db_schema,clnt_obj_id, supvr_pers_obj_id,pers_obj_id,excp_type,rpt_access,yr_cd,qtr_cd),(environment,db_schema,clnt_obj_id, supvr_pers_obj_id,pers_obj_id,excp_type,rpt_access,yr_cd,qtr_cd,mnth_cd));