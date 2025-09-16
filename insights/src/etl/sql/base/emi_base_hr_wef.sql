-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_base_hr_wef;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_base_hr_wef(
  avdble_trmnt_ind    DOUBLE ,
  clnt_obj_id     STRING ,
  d_cmpny_cd    STRING ,
  d_full_tm_part_tm_cd    STRING ,
  d_gndr_cd     STRING ,
  d_hr_orgn_id    STRING ,
  d_job_cd    STRING ,
  d_pay_grp_cd    STRING ,
  d_pay_rt_type_cd    STRING ,
  d_payrl_orgn_id     STRING ,
  d_reg_temp_cd     STRING ,
  d_work_cntry_cd     STRING ,
  d_work_loc_cd     STRING ,
  db_schema     STRING ,
  dem_ind     DOUBLE ,
  event_cd    STRING ,
  event_eff_dt    TIMESTAMP,
  event_rsn_cd    STRING ,
  hir_ind     DOUBLE ,
  int_mob_hir_ind     DOUBLE ,
  invlntry_trmnt_ind    DOUBLE ,
  lst_promo_dt    TIMESTAMP,
  mngr_pers_obj_id    STRING ,
  mnth_cd     STRING ,
  months_between_promotion    DOUBLE ,
  new_hir_ind     DOUBLE ,
  pay_grp_excl_ind    DOUBLE ,
  pers_obj_id     STRING ,
  pers_stus_cd    STRING ,
  promo_ind     DOUBLE ,
  qtr_cd    STRING ,
  reh_ind     DOUBLE ,
  retirement_ind    DOUBLE ,
  trmnt_ind     DOUBLE ,
  vlntry_trmnt_ind    DOUBLE ,
  work_asgnmt_nbr     STRING ,
  work_asgnmt_stus_cd     STRING ,
  xfr_ind     DOUBLE ,
  yr_cd     STRING ,
  yr_wk_cd    STRING,
  clndr_wk_cd STRING,
  environment STRING
  )
USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT INTO TABLE ${__BLUE_MAIN_DB__}.emi_base_hr_wef PARTITION(environment)
SELECT  
    /*+ COALESCE(800) */
    avdble_trmnt_ind,
    clnt_obj_id,
    d_cmpny_cd,
    d_full_tm_part_tm_cd,
    d_gndr_cd,
    d_hr_orgn_id,
    d_job_cd,
    d_pay_grp_cd,
    d_pay_rt_type_cd,
    d_payrl_orgn_id,
    d_reg_temp_cd,
    d_work_cntry_cd,
    d_work_loc_cd,
    db_schema,
    dem_ind,
    event_cd,
    event_eff_dt,
    event_rsn_cd,
    hir_ind,
    int_mob_hir_ind,
    invlntry_trmnt_ind,
    lst_promo_dt,
    mngr_pers_obj_id,
    mnth_cd,
    months_between_promotion,
    new_hir_ind,
    pay_grp_excl_ind,
    pers_obj_id,
    pers_stus_cd,
    promo_ind,
    qtr_cd,
    reh_ind,
    retirement_ind,
    trmnt_ind,
    vlntry_trmnt_ind,
    work_asgnmt_nbr,
    work_asgnmt_stus_cd,
    xfr_ind,
    yr_cd,
    yr_wk_cd,
    clndr_wk_cd,
    environment
FROM
(SELECT
    avdble_trmnt_ind,
    ext.clnt_obj_id,
    ext.cmpny_cd AS d_cmpny_cd,
    ext.full_tm_part_tm_cd AS d_full_tm_part_tm_cd,
    pers.gndr_cd AS d_gndr_cd,
    ext.hr_orgn_id AS d_hr_orgn_id,
    ext.job_cd AS d_job_cd,
    ext.pay_grp_cd AS d_pay_grp_cd,
    ext.pay_rt_type_cd AS d_pay_rt_type_cd,
    ext.payrl_orgn_id AS d_payrl_orgn_id,
    ext.reg_temp_cd AS d_reg_temp_cd,
    ext.work_loc_cntry_cd AS d_work_cntry_cd,
    ext.work_loc_cd AS d_work_loc_cd,
    ext.db_schema,
    CASE WHEN dwe.event_cd = 'DEM' THEN 1 ELSE 0 END AS dem_ind,
    dwe.event_cd,
    ext.event_eff_dt,
    dwe.event_rsn_cd,
    CASE WHEN hire_ind = 1 OR dwe.event_cd = 'REH' THEN 1 ELSE 0 END AS hir_ind,
    CASE WHEN hire_ind = 1 THEN 1 ELSE 0 END AS int_mob_hir_ind,
    invlntry_trmnt_ind,
    CASE WHEN dwe.event_cd = 'PRO' THEN lst_promo_dt ELSE NULL END AS lst_promo_dt,
    ext.mngr_pers_obj_id,
    COALESCE(fobs.mnth_cd, obs.mnth_cd) as mnth_cd,
    CASE WHEN dwe.event_cd = 'PRO' 
        THEN months_between(ext.event_eff_dt, NVL(ext.lst_promo_dt, ext.event_eff_dt)) 
        ELSE NULL END AS months_between_promotion,
    CASE WHEN rb.sort_ord_nbr = 1 THEN 1 ELSE 0 END AS new_hir_ind,
    cast(pay_grp_excl_ind as DOUBLE) pay_grp_excl_ind,
    ext.pers_obj_id,
    ext.pers_stus_cd,
    promo_ind,
    COALESCE(fobs.qtr_cd, obs.qtr_cd) as qtr_cd,
    CASE WHEN dwe.event_cd = 'REH' THEN 1 ELSE 0 END AS reh_ind,
    CASE WHEN dwe.event_cd = 'RET' THEN 1 ELSE 0 END AS retirement_ind,
    trmnt_ind,
    vlntry_trmnt_ind,
    ext.work_asgnmt_nbr,
    ext.work_asgnmt_stus_cd,
    xfer_ind AS xfr_ind,
    COALESCE(fobs.yr_cd, obs.yr_cd) as yr_cd,
    COALESCE(fobs.wk_cd, obs.wk_cd) as yr_wk_cd,   
    COALESCE(fobs.clndr_wk_cd, obs.clndr_wk_cd) as clndr_wk_cd,
    ext.environment       
FROM ${__RO_BLUE_RAW_DB__}.dwh_t_fact_work_event ext
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_event dwe
    ON ext.environment = dwe.environment
   AND ext.db_schema = dwe.db_schema
   AND ext.clnt_obj_id = dwe.clnt_obj_id
   AND ext.event_cd = dwe.event_cd
   AND ext.event_rsn_cd = dwe.event_rsn_cd
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pers pers
    ON pers.environment = ext.environment
    AND pers.db_schema = ext.db_schema
    AND pers.clnt_obj_id = ext.clnt_obj_id
   AND pers.pers_obj_id = ext.pers_obj_id
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_range_band rb
    ON ext.environment = rb.environment 
    --AND ext.db_schema = rb.db_schema
    AND months_between(ext.event_eff_dt,NVL(cast(ext.tnur_dt as date),cast(to_date(from_unixtime(unix_timestamp('99991231','yyyyMMdd'))) as date))) BETWEEN rb.low_range_val AND rb.hgh_range_val
LEFT OUTER JOIN (
    SELECT
      environment,
      --db_schema, 
      day_dt,
      yr_cd,
      qtr_cd,
      mnth_cd,
      wk_cd,
      CONCAT(SUBSTR(wk_cd, 1,4), SUBSTR(wk_cd, 6,2)) as clndr_wk_cd
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
) obs
    ON ext.environment = obs.environment
    --AND ext.db_schema = obs.db_schema
    AND ext.event_eff_dt = obs.day_dt
    AND ext.is_fscl_client = 'N' 
LEFT OUTER JOIN (
    SELECT
      environment,
      db_schema,
      clnt_obj_id,
      day_dt,
      yr_cd,
      qtr_cd,
      mnth_cd,
      fscl_wk_cd as wk_cd,
      CONCAT(SUBSTR(nor_wk_cd, 1,4), SUBSTR(nor_wk_cd, 6,2)) as clndr_wk_cd
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_fiscal
) fobs
    ON ext.environment = fobs.environment
    AND ext.db_schema = fobs.db_schema
    AND ext.event_eff_dt = fobs.day_dt
   AND ext.clnt_obj_id = fobs.clnt_obj_id
LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pay_grp tpg
    ON ext.environment = tpg.environment 
    AND ext.db_schema = tpg.db_schema
    AND ext.clnt_obj_id = tpg.clnt_obj_id 
   AND ext.pay_grp_cd = tpg.pay_grp_cd
--WHERE ext.environment = '${environment}'
WHERE ext.glob_excld_ind=0 AND (ext.clnt_live_ind = 'Y' OR ext.clnt_obj_id IN (select distinct clnt_obj_id from ${__BLUE_MAIN_DB__}.emi_non_live_clnts)) 
   AND (dwe.event_cd IN ('DEM','XFR','PRO','HIR','TER','REH','RET','LOF') OR dwe.HIRE_IND = 1)
   AND ext.prmry_work_asgnmt_ind = 1
   AND rb.range_band_catg_nm = 'Tenure Band'
   AND COALESCE(fobs.yr_cd, obs.yr_cd) IS NOT NULL)final;