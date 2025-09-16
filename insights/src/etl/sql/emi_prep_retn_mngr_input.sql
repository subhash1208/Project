-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_prep_retn_mngr;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_prep_retn_mngr(
    clnt_obj_id STRING,
    pers_obj_id STRING,
    mngr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    d_job_cd STRING,
    d_hr_orgn_id STRING,
    d_work_cntry_cd STRING,
    d_work_state_cd STRING,
    d_work_loc_cd STRING,
    rec_eff_strt_dt TIMESTAMP,
    rec_eff_end_dt TIMESTAMP,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
)
USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');
 
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_prep_retn_mngr PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    clnt_obj_id,
    pers_obj_id,
    mngr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    d_job_cd,
    d_hr_orgn_id,
    d_work_cntry_cd,
    d_work_state_cd,
    d_work_loc_cd,
    rec_eff_strt_dt,
    rec_eff_end_dt,
    db_schema,
    yr_cd,
    environment
FROM
(SELECT
     /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
    waf.clnt_obj_id,
    waf.pers_obj_id,
    rl.mngr_pers_obj_id,
    waf.qtr_cd,
    waf.mnth_cd,
    d_job_cd,
    d_hr_orgn_id,
    d_work_cntry_cd,
    d_work_state_cd,
    d_work_loc_cd,
    waf.rec_eff_strt_dt,
    waf.rec_eff_end_dt,
    waf.environment,
    waf.db_schema,
    waf.yr_cd
    FROM
    (SELECT
      clnt_obj_id,
      pers_obj_id,
      mngr_pers_obj_id,
      qtr_cd,
      mnth_cd,
      d_job_cd,
      d_hr_orgn_id,
      d_work_cntry_cd,
      d_work_state_cd,
      d_work_loc_cd,
      rec_eff_strt_dt,
      rec_eff_end_dt,
      environment,
      db_schema,
      yr_cd
    FROM ${__BLUE_MAIN_DB__}.emi_prep_retn_input
    DISTRIBUTE BY  db_schema, d_job_cd, d_hr_orgn_id,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd) waf
INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
    ON waf.environment = rl.environment
    AND waf.db_schema = rl.db_schema
    AND waf.clnt_obj_id = rl.clnt_obj_id
    AND waf.mngr_pers_obj_id = rl.pers_obj_id 
    AND rl.mngr_pers_obj_id IS NOT NULL
WHERE waf.rec_eff_strt_dt >= rl.rec_eff_strt_dt AND waf.rec_eff_end_dt <= rl.rec_eff_end_dt

UNION ALL

SELECT
     /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
    waf.clnt_obj_id,
    waf.pers_obj_id,
    rl.mngr_pers_obj_id,
    waf.qtr_cd,
    waf.mnth_cd,
    d_job_cd,
    d_hr_orgn_id,
    d_work_cntry_cd,
    d_work_state_cd,
    d_work_loc_cd,
    waf.rec_eff_strt_dt,
    waf.rec_eff_end_dt,
    waf.environment,
    waf.db_schema,
    waf.yr_cd
FROM
(SELECT
    clnt_obj_id,
    pers_obj_id,
    mngr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    d_job_cd,
    d_hr_orgn_id,
    d_work_cntry_cd,
    d_work_state_cd,
    d_work_loc_cd,
    rec_eff_strt_dt,
    rec_eff_end_dt,
    environment,
    db_schema,
    yr_cd
FROM ${__BLUE_MAIN_DB__}.emi_prep_retn_input
DISTRIBUTE BY  db_schema, d_job_cd, d_hr_orgn_id,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd) waf
INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
    ON waf.environment = rl.environment
    AND waf.db_schema = rl.db_schema
    AND waf.clnt_obj_id = rl.clnt_obj_id
    AND waf.pers_obj_id = rl.pers_obj_id
    AND mtrx_hrchy_ind = 1
    AND rl.mngr_pers_obj_id IS NOT NULL

 )waf;

 
-- Temporary table which will hold which persons are already captured as a part of manager hierarchy
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_retn_hrchy_mngrs;
 
CREATE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_retn_hrchy_mngrs
USING PARQUET
AS
SELECT
    /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
    DISTINCT waf.clnt_obj_id,
    waf.pers_obj_id,
    rl.mngr_pers_obj_id,
    waf.environment,
    waf.db_schema,
    waf.yr_cd,
    waf.qtr_cd,
    waf.mnth_cd
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_retn_input waf
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
        ON waf.environment = rl.environment
        AND waf.db_schema = rl.db_schema
        AND waf.clnt_obj_id = rl.clnt_obj_id
        AND waf.mngr_pers_obj_id = rl.pers_obj_id
WHERE
    --waf.environment='${environment}' 
    waf.rec_eff_strt_dt >= rl.rec_eff_strt_dt AND waf.rec_eff_end_dt <= rl.rec_eff_end_dt 
    AND waf.mngr_pers_obj_id IS NOT NULL;
 

-- Insert Cherrypick records into the main table first
INSERT INTO TABLE ${__BLUE_MAIN_DB__}.tmp_emi_prep_retn_mngr PARTITION(environment)
SELECT
    /*+ BROADCAST(dwh_t_dim_mngr_cherrypick,tmp_emi_retn_hrchy_mngrs),COALESCE(800)  */
    waf.clnt_obj_id,
    waf.pers_obj_id,
    rl.mngr_pers_obj_id,
    waf.qtr_cd,
    waf.mnth_cd,
    d_job_cd,
    d_hr_orgn_id,
    d_work_cntry_cd,
    d_work_state_cd,
    d_work_loc_cd,
    rec_eff_strt_dt,
    rec_eff_end_dt,
    waf.db_schema,
    waf.yr_cd,
    waf.environment
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_retn_input waf
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_mngr_cherrypick rl
    ON waf.environment = rl.environment
    AND waf.db_schema = rl.db_schema
    AND waf.clnt_obj_id = rl.clnt_obj_id
    AND waf.pers_obj_id = rl.pers_obj_id
    AND waf.work_asgnmt_nbr = rl.work_asgnmt_nbr
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_emi_retn_hrchy_mngrs prev
    ON waf.environment = prev.environment
    AND waf.db_schema = prev.db_schema
    AND waf.clnt_obj_id = prev.clnt_obj_id
    AND waf.pers_obj_id = prev.pers_obj_id
    AND rl.mngr_pers_obj_id = prev.mngr_pers_obj_id
    AND waf.yr_cd = prev.yr_cd
    AND waf.qtr_cd = prev.qtr_cd
    AND waf.mnth_cd = prev.mnth_cd
WHERE 
-- to exclude cases where the manager is included as a part of the reporting hierarchy
prev.mngr_pers_obj_id IS NULL;