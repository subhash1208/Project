-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_base_tm_tf_edits(
  clndr_wk_cd STRING,
  clnt_obj_id STRING,
  cmpny_cd STRING,
  db_schema STRING,
  full_tm_part_tm_cd STRING,
  hr_orgn_id STRING,
  job_cd STRING,
  labor_acct_id STRING,
  mngr_pers_obj_id STRING,
  pay_grp_cd STRING,
  pay_rt_type_cd STRING,
  paycd_clsfn_cd STRING,
  paycd_grp_cd STRING,
  paycd_id STRING,
  payrl_orgn_id STRING,
  pers_clsfn_cd STRING,
  pers_obj_id STRING,
  qtr_cd STRING,
  reg_ind DOUBLE,
  reg_temp_cd STRING,
  supvr_pers_obj_id STRING,
  time_product STRING,
  tm_sheet_item_id_ind DOUBLE,
  trans_dt TIMESTAMP,
  usr_pers_obj_id STRING,
  work_asgnmt_nbr STRING,
  work_asgnmt_stus_cd STRING,
  work_cntry_cd STRING,
  work_loc_cd STRING,
  yr_cd STRING,
  yr_wk_cd STRING,
  mnth_cd STRING,
  environment STRING)
USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_base_tm_tf_edits PARTITION(environment)
SELECT
  /*+ COALESCE(800) */
  clndr_wk_cd,
  clnt_obj_id,
  cmpny_cd,
  db_schema,
  full_tm_part_tm_cd,
  hr_orgn_id,
  job_cd,
  labor_acct_id,
  mngr_pers_obj_id,
  pay_grp_cd,
  pay_rt_type_cd,
  paycd_clsfn_cd,
  paycd_grp_cd,
  paycd_id,
  payrl_orgn_id,
  pers_clsfn_cd,
  pers_obj_id,
  qtr_cd,
  reg_ind,
  reg_temp_cd,
  supvr_pers_obj_id,
  time_product,
  --tm_sheet_item_id_cnt_ind,
  tm_sheet_item_id_ind,
  trans_dt,  
  usr_pers_obj_id,
  work_asgnmt_nbr,
  work_asgnmt_stus_cd,
  work_cntry_cd,
  work_loc_cd, 
  yr_cd,
  yr_wk_cd, 
  mnth_cd,
  environment
FROM
(SELECT
  obs.clndr_wk_cd,
  ext.clnt_obj_id,
  ext.cmpny_cd,
  ext.db_schema,
  ext.full_tm_part_tm_cd,
  ext.hr_orgn_id,
  ext.job_cd,
  ext.labor_acct_id,
  ext.mngr_pers_obj_id,
  ext.pay_grp_cd,
  ext.pay_rt_type_cd,
  pc.paycd_clsfn_cd,
  pc.paycd_grp_cd,
  ext.paycd_id,
  ext.payrl_orgn_id,
  ext.pers_clsfn_cd,
  ext.pers_obj_id,
  obs.qtr_cd,
  pc.reg_ind,
  ext.reg_temp_cd,
  ext.supvr_pers_obj_id,
  ext.time_product,
  --CASE WHEN tm_sheet_item_id IS NOT NULL THEN 1 ELSE 0 END AS tm_sheet_item_id_cnt_ind,
  CASE WHEN (((ext.pers_obj_id <> COALESCE(ext.usr_pers_obj_id,'UNKNOWN') AND ext.data_srce_id IN('E','S','P','N'))) AND COALESCE(ext.usr_pers_obj_id,'UNKNOWN') <> 'UNKNOWN')
       THEN 1 ELSE 0 END AS tm_sheet_item_id_ind,
  ext.trans_dt,  
  ext.usr_pers_obj_id,
  ext.work_asgnmt_nbr,
  ext.work_asgnmt_stus_cd,
  ext.work_loc_cntry_cd AS work_cntry_cd,
  ext.work_loc_cd, 
  obs.yr_cd,
  NULL AS yr_wk_cd, 
  ext.environment,
  obs.mnth_cd  
FROM ${__RO_BLUE_RAW_DB__}.dwh_t_fact_tm_audit_trail ext
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_paycd pc
  ON ext.environment = pc.environment
  AND ext.db_schema = pc.db_schema 
  AND ext.clnt_obj_id = pc.clnt_obj_id
  AND ext.paycd_id = pc.paycd_id
  AND pc.paycd_grp_cd <> 'Exclude from Analytics'
LEFT OUTER JOIN (
    SELECT
      environment,
      --db_schema,
      yr_cd,
      qtr_cd,
      mnth_cd,
      mnth_strt_dt,
      mnth_end_dt,
      MIN(CONCAT(SUBSTR(wk_cd, 1,4), SUBSTR(wk_cd, 6,2))) as clndr_wk_cd
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    GROUP BY
    environment,
    --db_schema,
    yr_cd,
    qtr_cd,
    mnth_cd,
    mnth_strt_dt,
    mnth_end_dt
) obs
  ON ext.environment = obs.environment
  --AND ext.db_schema = obs.db_schema
  AND ext.trans_dt BETWEEN obs.mnth_strt_dt AND obs.mnth_end_dt  
WHERE
ext.glob_excld_ind=0 AND (ext.clnt_live_ind = 'Y' OR ext.clnt_obj_id IN (select distinct clnt_obj_id from ${__BLUE_MAIN_DB__}.emi_non_live_clnts))
--ext.environment='${environment}'
AND ext.trans_dt >= obs.mnth_strt_dt
AND obs.yr_cd IS NOT NULL
AND date_format(ext.trans_dt,'yyyy') <= date_format(current_date,'yyyy'))final;