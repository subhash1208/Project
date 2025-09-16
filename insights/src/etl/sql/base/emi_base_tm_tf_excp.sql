-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_base_tm_tf_excp(
clndr_wk_cd STRING,
clnt_obj_id STRING,
cmpny_cd STRING,
db_schema STRING,
excep_dt TIMESTAMP,
excep_hrs_in_secnd_nbr DOUBLE,
excp_ind DOUBLE,
excp_type STRING,
full_tm_part_tm_cd STRING,
hr_orgn_id STRING,
job_cd STRING,
mngr_pers_obj_id STRING,
pay_grp_cd STRING,
pay_rt_type_cd STRING,
payrl_orgn_id STRING,
pers_clsfn_cd STRING,
pers_obj_id STRING,
prmry_labor_acct_id STRING,
qtr_cd STRING,
reg_temp_cd STRING,
supvr_pers_obj_id STRING,
time_product STRING,
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

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_base_tm_tf_excp PARTITION(environment)
SELECT 
    /*+ COALESCE(800) */
    clndr_wk_cd,
    clnt_obj_id,
    cmpny_cd,
    db_schema,
    excep_dt,
    excep_hrs_in_secnd_nbr,
    excp_ind,
    excp_type,
    full_tm_part_tm_cd,
    hr_orgn_id,
    job_cd,
    mngr_pers_obj_id,
    pay_grp_cd,
    pay_rt_type_cd,
    payrl_orgn_id,
    pers_clsfn_cd,
    pers_obj_id,
    prmry_labor_acct_id,
    qtr_cd,
    reg_temp_cd,
    supvr_pers_obj_id,
    time_product,
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
    ext.excep_dt,
    ext.excep_hrs_in_secnd_nbr,
    CASE WHEN (ext.excep_dt BETWEEN obs.mnth_strt_dt AND obs.mnth_end_dt) THEN 1 ELSE 0 END AS excp_ind,
    ext.excep_shrt_nm as excp_type,
    ext.full_tm_part_tm_cd,
    ext.hr_orgn_id,
    ext.job_cd,
    ext.mngr_pers_obj_id,
    ext.pay_grp_cd,
    ext.pay_rt_type_cd,
    ext.payrl_orgn_id,
    ext.pers_clsfn_cd,
    ext.pers_obj_id,
    ext.prmry_labor_acct_id,
    obs.qtr_cd,
    ext.reg_temp_cd,
    ext.supvr_pers_obj_id,
    ext.time_product,
    ext.work_asgnmt_nbr,
    ext.work_asgnmt_stus_cd,
    ext.work_loc_cntry_cd as work_cntry_cd,
    ext.work_loc_cd,
    obs.yr_cd,
    NULL AS yr_wk_cd,
    ext.environment,      
    obs.mnth_cd
FROM ${__BLUE_RAW_DB__}.dwh_t_fact_tm_excep ext
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
    FROM ${__BLUE_RAW_DB__}.dwh_t_dim_day
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
  AND ext.excep_dt BETWEEN obs.mnth_strt_dt AND obs.mnth_end_dt  
WHERE
ext.glob_excld_ind=0 AND (ext.clnt_live_ind = 'Y' OR ext.clnt_obj_id IN (select distinct clnt_obj_id from ${__BLUE_MAIN_DB__}.emi_non_live_clnts))
--ext.environment='${environment}'
AND ext.excep_dt >= obs.mnth_strt_dt
AND obs.yr_cd IS NOT NULL
AND date_format(ext.excep_dt,'yyyy') <= date_format(current_date,'yyyy'))final;