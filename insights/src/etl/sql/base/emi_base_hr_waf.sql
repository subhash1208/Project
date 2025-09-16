-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_base_hr_waf(
  act_wk_end_dt TIMESTAMP,
  act_wk_strt_dt TIMESTAMP,
  birth_yr DOUBLE,
  clnt_obj_id STRING,
  d_cmpny_cd STRING,
  d_eeo_ethncty_clsfn_cd STRING,
  d_eeo_ethncty_clsfn_dsc STRING,
  d_full_tm_part_tm_cd STRING,
  d_gndr_cd STRING,
  d_hr_orgn_id STRING,
  d_job_cd STRING,
  d_martl_stus_cd STRING,
  d_pay_grp_cd STRING,
  d_pay_rt_type_cd STRING,
  d_reg_temp_cd STRING,
  d_reg_temp_dsc STRING,
  d_work_cntry_cd STRING,
  d_work_loc_cd STRING,
  db_schema STRING,
  ee_range_band_ky DOUBLE,
  eff_person_days DOUBLE,
  f_compa_rt DOUBLE,
  f_hr_annl_cmpn_amt DOUBLE,
  f_hrly_cmpn_amt DOUBLE,
  f_work_asgnmt_actv_ind DOUBLE,
  f_work_asgnmt_stus_cd STRING,
  fwa_rec_eff_end_dt TIMESTAMP,
  fwa_rec_eff_strt_dt TIMESTAMP,
  inds_ky DOUBLE,
  ltst_hire_dt TIMESTAMP,
  mngr_pers_obj_id STRING,
  supvr_pers_obj_id STRING,
  mnth_cd STRING,
  mnth_end_dt TIMESTAMP,
  pers_clsfn_cd STRING,
  pers_obj_id STRING,
  qtr_cd STRING,
  qtr_end_dt TIMESTAMP,
  rec_eff_end_dt TIMESTAMP,
  rec_eff_strt_dt TIMESTAMP,
  rev_range_band_ky DOUBLE,
  tnur_dt TIMESTAMP,
  trmnt_dt TIMESTAMP,
  work_asgnmt_nbr STRING,
  yr_cd STRING,
  yr_end_dt TIMESTAMP,
  yr_wk_cd STRING,
  prmry_work_asgnmt_ind DOUBLE,
  clndr_wk_cd STRING,
  environment STRING
  )
USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_base_hr_waf PARTITION( environment)
SELECT
   /*+ COALESCE(800) */
   act_wk_end_dt,
   act_wk_strt_dt,
   birth_yr,
   clnt_obj_id,
   d_cmpny_cd,
   d_eeo_ethncty_clsfn_cd,
   d_eeo_ethncty_clsfn_dsc,
   d_full_tm_part_tm_cd,
   d_gndr_cd,
   d_hr_orgn_id,
   d_job_cd,
   d_martl_stus_cd,
   d_pay_grp_cd,
   d_pay_rt_type_cd,
   d_reg_temp_cd,
   d_reg_temp_dsc,
   d_work_cntry_cd,
   d_work_loc_cd,
   db_schema,
   ee_range_band_ky,
   eff_person_days,
   f_compa_rt,
   f_hr_annl_cmpn_amt,
   f_hrly_cmpn_amt,
   f_work_asgnmt_actv_ind,
   f_work_asgnmt_stus_cd,
   fwa_rec_eff_end_dt,
   fwa_rec_eff_strt_dt,
   inds_ky,
   ltst_hire_dt,
   mngr_pers_obj_id,
   supvr_pers_obj_id,
   mnth_cd,
   mnth_end_dt,
   pers_clsfn_cd,
   pers_obj_id,
   qtr_cd,
   qtr_end_dt,
   rec_eff_end_dt,
   rec_eff_strt_dt,    
   rev_range_band_ky,
   tnur_dt,
   trmnt_dt,
   work_asgnmt_nbr,    
   yr_cd,
   yr_end_dt,
   yr_wk_cd,
   prmry_work_asgnmt_ind,
   clndr_wk_cd,
   environment
FROM
(SELECT
   COALESCE(fobs.wk_end_dt,obs.wk_end_dt) AS act_wk_end_dt,
   COALESCE(fobs.wk_strt_dt,obs.wk_strt_dt) AS act_wk_strt_dt,
   pers.birth_yr,
   ext.clnt_obj_id,
   ext.cmpny_cd AS d_cmpny_cd,
   pers.eeo_ethncty_clsfn_cd  AS d_eeo_ethncty_clsfn_cd,
   pers.eeo_ethncty_clsfn_dsc AS d_eeo_ethncty_clsfn_dsc,
   ext.full_tm_part_tm_cd AS d_full_tm_part_tm_cd,
   pers.gndr_cd AS d_gndr_cd,
   ext.hr_orgn_id AS d_hr_orgn_id,
   ext.job_cd AS d_job_cd,
   pers.martl_stus_cd AS d_martl_stus_cd,
   ext.pay_grp_cd AS d_pay_grp_cd,
   ext.pay_rt_type_cd AS d_pay_rt_type_cd,
   ext.reg_temp_cd AS d_reg_temp_cd,
   CASE
      WHEN ext.reg_temp_cd = 'T'
      THEN 'TEMPORARY'
      ELSE 'REGULAR'
    END AS d_reg_temp_dsc,
    ext.work_loc_cntry_cd AS d_work_cntry_cd,
    ext.work_loc_cd AS d_work_loc_cd,
    ext.db_schema,
    ext.ee_range_band_ky,
    (datediff(least(COALESCE(fobs.wk_end_dt,obs.wk_end_dt), ext.rec_eff_end_dt),
         greatest(COALESCE(fobs.wk_strt_dt,obs.wk_strt_dt), ext.rec_eff_strt_dt)) + 1) AS eff_person_days,
    ext.compa_rt AS f_compa_rt,
    ext.annl_cmpn_amt AS f_hr_annl_cmpn_amt,
    ext.hrly_cmpn_amt AS f_hrly_cmpn_amt,
    ext.work_asgnmt_actv_ind AS f_work_asgnmt_actv_ind,
    ext.work_asgnmt_stus_cd  AS f_work_asgnmt_stus_cd,
    CASE
      WHEN ext.work_asgnmt_stus_cd IN ('P','A','L','S')
      THEN ext.rec_eff_end_dt
      ELSE ext.rec_eff_strt_dt
    END AS fwa_rec_eff_end_dt,
    ext.rec_eff_strt_dt AS fwa_rec_eff_strt_dt,
    ext.inds_ky,
    pers.ltst_hire_dt,
    ext.mngr_pers_obj_id AS mngr_pers_obj_id,
    ext.supvr_pers_obj_id AS supvr_pers_obj_id,
    COALESCE(fobs.mnth_cd, obs.mnth_cd) as mnth_cd,
    COALESCE(fobs.mnth_end_dt,obs.mnth_end_dt) AS mnth_end_dt,
    ext.pers_clsfn_cd,
    ext.pers_obj_id,
    COALESCE(fobs.qtr_cd, obs.qtr_cd) as qtr_cd,
    COALESCE(fobs.qtr_end_dt,obs.qtr_end_dt) AS qtr_end_dt,
    least(COALESCE(fobs.wk_end_dt,obs.wk_end_dt), ext.rec_eff_end_dt) AS rec_eff_end_dt,
    greatest(COALESCE(fobs.wk_strt_dt,obs.wk_strt_dt), ext.rec_eff_strt_dt) AS rec_eff_strt_dt,    
    ext.rev_range_band_ky,
    ext.tnur_dt,
    pers.trmnt_dt,
    ext.work_asgnmt_nbr,    
    COALESCE(fobs.yr_cd, obs.yr_cd) as yr_cd,
    COALESCE(fobs.yr_end_dt,obs.yr_end_dt) AS yr_end_dt,
    COALESCE(fobs.wk_cd, obs.wk_cd) as yr_wk_cd,
    COALESCE(fobs.clndr_wk_cd, obs.clndr_wk_cd) as clndr_wk_cd,
    ext.prmry_work_asgnmt_ind,
    ext.environment
  FROM ${__BLUE_RAW_DB__}.dwh_t_fact_work_asgnmt ext
  INNER JOIN ${__BLUE_RAW_DB__}.dwh_t_dim_pers pers
  ON pers.environment = ext.environment
  AND pers.db_schema = ext.db_schema
  AND pers.clnt_obj_id  = ext.clnt_obj_id
  AND pers.pers_obj_id = ext.pers_obj_id  
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
       yr_end_dt,
       qtr_end_dt,
       mnth_end_dt,
       CONCAT(SUBSTR(wk_cd, 1,4), SUBSTR(wk_cd, 6,2)) as clndr_wk_cd
    FROM ${__BLUE_RAW_DB__}.dwh_t_dim_day
    GROUP BY 
      environment,
      --db_schema,
      yr_cd,
      qtr_cd,
      mnth_cd,
      wk_strt_dt,
      wk_end_dt,
      mnth_end_dt,
      qtr_end_dt,
      yr_end_dt,
      wk_cd) obs
  ON ext.environment = obs.environment
  --AND ext.db_schema = obs.db_schema
  AND ext.rec_eff_strt_dt <= obs.wk_end_dt
  AND
    CASE
      WHEN ext.work_asgnmt_stus_cd IN ('P','A','L','S')
      THEN ext.rec_eff_end_dt
      ELSE ext.rec_eff_strt_dt
    END >= obs.wk_strt_dt
  AND ext.is_fscl_client = 'N'  -- Join to t_dim_day ONLY for clients not using custom fiscal calendar -- this implies clients are using normal calendar
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
       yr_end_dt,
       qtr_end_dt,
       mnth_end_dt,
       CONCAT(SUBSTR(nor_wk_cd, 1,4), SUBSTR(nor_wk_cd, 6,2)) as clndr_wk_cd
    FROM ${__BLUE_RAW_DB__}.dwh_t_dim_fiscal
    GROUP BY
      environment,
      db_schema,
      clnt_obj_id,
      yr_cd,
      qtr_cd,
      mnth_cd,
      wk_strt_dt,
      wk_end_dt,
      mnth_end_dt,
      qtr_end_dt,
      yr_end_dt,
      fscl_wk_cd,
      nor_wk_cd) fobs
  ON ext.rec_eff_strt_dt <= fobs.wk_end_dt
  AND
    CASE
      WHEN ext.work_asgnmt_stus_cd IN ('P','A','L','S')
      THEN ext.rec_eff_end_dt
      ELSE ext.rec_eff_strt_dt
    END >= fobs.wk_strt_dt
  AND ext.environment = fobs.environment
  AND ext.db_schema = fobs.db_schema  
  AND ext.clnt_obj_id = fobs.clnt_obj_id  -- Join to t_dim_fscl_clndr for clients using custom fiscal calendar
  WHERE ext.glob_excld_ind=0 AND (ext.clnt_live_ind = 'Y' OR ext.clnt_obj_id IN (select distinct clnt_obj_id from ${__BLUE_MAIN_DB__}.emi_non_live_clnts))
  --WHERE ext.environment = '${environment}'
  --AND ext.prmry_work_asgnmt_ind = 1 --commenting out this and getting this column in select
  AND ext.work_asgnmt_stus_cd    IN ('A','L','P','S','T','R','D') --A active, L Leave, S suspension, P Leave With Pay, L Leave of Absence, T terminated, R Retired, D Deceased
  AND COALESCE(fobs.yr_cd, obs.yr_cd) IS NOT NULL)final;