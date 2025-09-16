-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_base_pr_pef(
  check_dt TIMESTAMP,
  clnt_obj_id STRING,
  cmpny_cd STRING,
  d_gndr_cd STRING,
  d_ot_ind DOUBLE,
  d_reg_temp_dsc STRING,
  db_schema STRING,
  earn_amt DOUBLE,
  earn_cd STRING,
  ee_range_band_ky DOUBLE,
  full_tm_part_tm_cd STRING,
  hr_cmpn_freq_cd STRING,
  hr_orgn_id STRING,
  hrly_rt DOUBLE,
  hrs_nbr DOUBLE,
  inds_ky DOUBLE,
  job_cd STRING,
  mngr_pers_obj_id STRING,
  mnth_cd STRING,
  pay_grp_cd STRING,
  pay_rt_type_cd STRING,
  pers_clsfn_cd STRING,
  pers_obj_id STRING,
  qtr_cd STRING,
  reg_temp_cd STRING,
  rev_range_band_ky DOUBLE,
  strgt_tm_ind DOUBLE,
  work_asgnmt_nbr STRING,
  work_loc_cd STRING,
  work_loc_cntry_cd STRING,
  yr_cd STRING,
  yr_wk_cd STRING,
  clndr_wk_cd STRING,
  yyyymm STRING,
  environment STRING)
USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_base_pr_pef PARTITION(environment)
SELECT 
  /*+ COALESCE(800) */
  check_dt,
  clnt_obj_id,
  cmpny_cd,
  d_gndr_cd,
  d_ot_ind,
  d_reg_temp_dsc,
  db_schema,
  earn_amt,
  earn_cd,
  ee_range_band_ky,
  full_tm_part_tm_cd,
  hr_cmpn_freq_cd,
  hr_orgn_id,
  hrly_rt,
  hrs_nbr, 
  inds_ky,
  job_cd,
  mngr_pers_obj_id,
  mnth_cd,
  pay_grp_cd,
  pay_rt_type_cd,
  pers_clsfn_cd,
  pers_obj_id,
  qtr_cd,
  reg_temp_cd,
  rev_range_band_ky,
  strgt_tm_ind,
  work_asgnmt_nbr,
  work_loc_cd,
  work_loc_cntry_cd,
  yr_cd, 
  yr_wk_cd,
  clndr_wk_cd,
  yyyymm,
  environment
FROM
(SELECT
  ext.check_dt,
  ext.clnt_obj_id,
  ext.cmpny_cd,
  pers.gndr_cd AS d_gndr_cd,
  dpe.ot_ind AS d_ot_ind,
  CASE WHEN ext.reg_temp_cd = 'T' THEN 'TEMPORARY' ELSE 'REGULAR' END AS d_reg_temp_dsc,
  ext.db_schema,
  earn_amt AS earn_amt,
  ext.earn_cd,
  ext.ee_range_band_ky,
  full_tm_part_tm_cd,
  hr_cmpn_freq_cd,
  hr_orgn_id,
  hrly_rt AS hrly_rt, -- APPROXIMATE. FAULTY.
  hrs_nbr AS hrs_nbr, 
  ext.inds_ky,
  job_cd,
  mngr_pers_obj_id,
  COALESCE(fobs.mnth_cd, obs.mnth_cd) as mnth_cd,
  ext.pay_grp_cd,
  pay_rt_type_cd,
  pers_clsfn_cd,
  ext.pers_obj_id,
  COALESCE(fobs.qtr_cd, obs.qtr_cd) as qtr_cd,
  reg_temp_cd,
  ext.rev_range_band_ky,
  strgt_tm_ind,
  work_asgnmt_nbr,
  work_loc_cd,
  work_loc_cntry_cd,
  ext.yyyymm,
  COALESCE(fobs.yr_cd, obs.yr_cd) as yr_cd, 
  COALESCE(fobs.wk_cd, obs.wk_cd) as yr_wk_cd,  
  COALESCE(fobs.clndr_wk_cd, obs.clndr_wk_cd) as clndr_wk_cd,
  ext.environment
FROM ${__RO_BLUE_RAW_DB__}.dwh_t_fact_payrl_earn_dtl ext
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pers pers
ON pers.environment = ext.environment
ANd pers.db_schema = ext.db_schema
AND pers.clnt_obj_id = ext.clnt_obj_id
AND pers.pers_obj_id = ext.pers_obj_id
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_payrl_earn dpe
ON dpe.environment = ext.environment
AND dpe.db_schema = ext.db_schema
AND dpe.earn_cd   = ext.earn_cd
AND dpe.pay_grp_cd = ext.pay_grp_cd
AND dpe.clnt_obj_id = ext.clnt_obj_id
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_clnt clnt -- this join is for defaulting client to his country
ON ext.environment = clnt.environment
AND ext.db_schema = clnt.db_schema
AND ext.clnt_obj_id = clnt.clnt_obj_id
AND lower(trim(ext.work_loc_cntry_cd)) = lower(trim(clnt.work_cntry_cd))
LEFT OUTER JOIN (
  SELECT
    environment,
    --db_schema, 
    yr_cd,
    qtr_cd,
    mnth_cd,
    wk_cd,
    wk_strt_dt,
    wk_end_dt,
    CONCAT(SUBSTR(wk_cd, 1,4), SUBSTR(wk_cd, 6,2)) as clndr_wk_cd
  FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
  GROUP BY
   environment,
   --db_schema,
   yr_cd,
   qtr_cd,
   mnth_cd,
   wk_strt_dt,
   wk_end_dt,
   CONCAT(SUBSTR(wk_cd, 1,4), SUBSTR(wk_cd, 6,2)),
   wk_cd) obs
ON ext.environment = obs.environment
--AND ext.db_schema = obs.db_schema
AND ext.check_dt BETWEEN obs.wk_strt_dt AND obs.wk_end_dt
AND ext.is_fscl_client = 'N' -- Join to t_dim_day ONLY for clients not using custom fiscal calendar
LEFT OUTER JOIN (
  SELECT
    environment,
    db_schema,
    clnt_obj_id,
    yr_cd,
    qtr_cd,
    mnth_cd,
    fscl_wk_cd as wk_cd,
    wk_strt_dt,
    wk_end_dt,    
    CONCAT(SUBSTR(nor_wk_cd, 1,4), SUBSTR(nor_wk_cd, 6,2)) as clndr_wk_cd
  FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_fiscal  
  GROUP BY
   environment,
   db_schema,
   clnt_obj_id,
   nor_wk_cd,
   yr_cd,
   qtr_cd,
   mnth_cd,
   wk_strt_dt,
   wk_end_dt,
   fscl_wk_cd) fobs
ON ext.environment = fobs.environment
AND ext.db_schema = fobs.db_schema
AND ext.check_dt BETWEEN fobs.wk_strt_dt AND fobs.wk_end_dt
AND ext.clnt_obj_id = fobs.clnt_obj_id  -- Join to t_dim_fscl_clndr for clients using custom fiscal calendar
--WHERE ext.environment = '${environment}'
WHERE ext.glob_excld_ind=0 AND (ext.clnt_live_ind = 'Y' OR ext.clnt_obj_id IN (select distinct clnt_obj_id from ${__BLUE_MAIN_DB__}.emi_non_live_clnts))
AND dpe.earn_grp_cd <> 'Exclude from Analytics'
AND COALESCE(fobs.yr_cd, obs.yr_cd) IS NOT NULL
AND date_format(ext.check_dt,'yyyy') <= date_format(current_date,'yyyy')
AND ext.db_schema not like  '%ADPDC_WFNPISCH00001%' )final;