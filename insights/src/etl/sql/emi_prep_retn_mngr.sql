-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_retn_mngr(
    clnt_obj_id STRING,
    mngr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    d_job_cd STRING,
    d_hr_orgn_id STRING,
    d_work_cntry_cd STRING,
    d_work_state_cd STRING,
    d_work_loc_cd STRING,
    is_active_in_strt_cnt INT,
    is_retained_in_end_cnt INT,
    yr_cd STRING,
    environment STRING
)
USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY'); 

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_retn_mngr PARTITION(environment)
SELECT
  /*+ COALESCE(800) */
  clnt_obj_id,  
  mngr_pers_obj_id,
  qtr_cd,
  mnth_cd,
  d_job_cd,
  d_hr_orgn_id,
  d_work_cntry_cd,
  d_work_state_cd,
  d_work_loc_cd,
  SUM(is_active_in_strt) as is_active_in_strt_cnt,
  SUM(is_retained_in_end) as is_retained_in_end_cnt,
  --db_schema,
  yr_cd,
  environment
FROM
(SELECT
   environment,
    --db_schema,
    clnt_obj_id,
    pers_obj_id,
    mngr_pers_obj_id,
    yr_cd,
    qtr_cd,
    mnth_cd,
    d_job_cd,
    d_hr_orgn_id,
    d_work_cntry_cd,
    d_work_state_cd,
    d_work_loc_cd,
    CASE WHEN (qtr_cd IS NULL AND mnth_cd is NULL) THEN
        (CASE WHEN
             MIN(GREATEST(CAST(rec_eff_strt_dt AS DATE), TRUNC(rec_eff_strt_dt,'YY'))) = MIN(TRUNC(rec_eff_strt_dt,'YY'))
             THEN 1 ELSE 0 END)
        WHEN (qtr_cd IS NOT NULL AND mnth_cd IS NULL) THEN
        (CASE WHEN
              MIN(GREATEST(CAST(rec_eff_strt_dt AS DATE), ADD_MONTHS(TRUNC(rec_eff_strt_dt,'MM'), - (MONTH(rec_eff_strt_dt) - 1)%3))) = MIN(ADD_MONTHS(TRUNC(rec_eff_strt_dt,'MM'), - (MONTH(rec_eff_strt_dt) - 1)%3))
              THEN 1 ELSE 0 END)
        WHEN (qtr_cd IS NOT NULL AND mnth_cd IS NOT NULL) THEN
        (CASE WHEN
              MIN(GREATEST(CAST(rec_eff_strt_dt AS DATE), TRUNC(rec_eff_strt_dt,'MM'))) = MIN(TRUNC(rec_eff_strt_dt,'MM'))
              THEN 1 ELSE 0 END)
        ELSE 0 END AS is_active_in_strt,
    CASE WHEN (qtr_cd IS NULL AND mnth_cd is NULL) THEN
         (CASE WHEN 
               MIN(GREATEST(CAST(rec_eff_strt_dt AS DATE), TRUNC(rec_eff_strt_dt,'YY'))) = MIN(TRUNC(rec_eff_strt_dt,'YY')) AND
               MAX(LEAST(CAST(rec_eff_end_dt AS DATE), DATE_SUB(TRUNC(ADD_MONTHS(rec_eff_end_dt,12),'YY'), 1))) = MAX(DATE_SUB(TRUNC(ADD_MONTHS(rec_eff_end_dt,12),'YY'), 1))
               THEN 1 ELSE 0 END)
         WHEN (qtr_cd IS NOT NULL AND mnth_cd IS NULL) THEN
         (CASE WHEN
               MIN(GREATEST(CAST(rec_eff_strt_dt AS DATE), ADD_MONTHS(TRUNC(rec_eff_strt_dt,'MM'), - (MONTH(rec_eff_strt_dt) - 1)%3))) = MIN(ADD_MONTHS(TRUNC(rec_eff_strt_dt,'MM'), - (MONTH(rec_eff_strt_dt) - 1)%3)) AND
               MAX(LEAST(CAST(rec_eff_end_dt AS DATE), DATE_SUB(TRUNC(ADD_MONTHS(rec_eff_end_dt, 3 - (MONTH(rec_eff_end_dt) - 1)%3 ),'MM'),1) )) = MAX(DATE_SUB(TRUNC(ADD_MONTHS(rec_eff_end_dt, 3 - (MONTH(rec_eff_end_dt) - 1)%3 ) ,'MM'),1))
               THEN 1 ELSE 0 END)
         WHEN (qtr_cd IS NOT NULL AND mnth_cd IS NOT NULL) THEN
         (CASE WHEN
               MIN(GREATEST(CAST(rec_eff_strt_dt AS DATE), TRUNC(rec_eff_strt_dt,'MM'))) = MIN(TRUNC(rec_eff_strt_dt,'MM')) AND
               MAX(LEAST(CAST(rec_eff_end_dt AS DATE), DATE_SUB(TRUNC(ADD_MONTHS(rec_eff_end_dt,1),'MM'), 1))) = MAX(DATE_SUB(TRUNC(ADD_MONTHS(rec_eff_end_dt,1),'MM'), 1))
               THEN 1 ELSE 0 END)
         ELSE 0 END AS is_retained_in_end
   FROM ${__BLUE_MAIN_DB__}.tmp_emi_prep_retn_mngr
   --WHERE environment = '${environment}'
        GROUP BY environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_job_cd,d_hr_orgn_id,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd
        GROUPING SETS ( (environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id, yr_cd),(environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,d_job_cd),(environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,d_hr_orgn_id) ,
        (environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,d_work_cntry_cd,d_work_state_cd),(environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd),
        (environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd),(environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,d_job_cd),(environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,d_hr_orgn_id) ,
        (environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,d_work_cntry_cd,d_work_state_cd),(environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd),
        (environment,clnt_obj_id, mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd),(environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_job_cd),(environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_hr_orgn_id) ,
        (environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_work_cntry_cd,d_work_state_cd),(environment,clnt_obj_id,mngr_pers_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd))
)final
GROUP BY environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_job_cd,d_hr_orgn_id,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd;