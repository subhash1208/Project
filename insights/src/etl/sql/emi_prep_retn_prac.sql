-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_retn_input(
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
    work_asgnmt_nbr STRING,
    rec_eff_strt_dt TIMESTAMP,
    rec_eff_end_dt TIMESTAMP,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
)USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_retn_prac(
    clnt_obj_id STRING,
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
)USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');


--DROP TABLE IF EXISTS ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_retn;

--CREATE TABLE ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_retn
--USING PARQUET
--AS
--SELECT  environment,min(yr_cd) as last_load_clndr_yr_cd
--FROM 
--    (SELECT  environment,clnt_obj_id, max(yr_cd) as yr_cd 
--    FROM ${__GREEN_MAIN_DB__}.emi_prep_retn_prac
--    --WHERE environment = '${environment}'
--    GROUP BY environment,clnt_obj_id)IGN
--GROUP BY environment;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_retn_input PARTITION (environment)
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
  work_asgnmt_nbr,
  rec_eff_strt_dt,
  rec_eff_end_dt,
  db_schema,
  yr_cd,
  environment
FROM 
(SELECT
    /*+ BROADCAST(dwh_t_dim_job,dwh_t_dim_work_loc) */
    waf.environment,
    waf.db_schema,
    waf.clnt_obj_id,
    pers_obj_id,
    waf.mngr_pers_obj_id,
    yr_cd,
    qtr_cd,
    mnth_cd,
    d_job_cd,
    d_hr_orgn_id,
    waf.d_work_cntry_cd,
    worklocs.state_prov_cd AS d_work_state_cd,
    d_work_loc_cd,
    work_asgnmt_nbr,
    rec_eff_strt_dt,
    rec_eff_end_dt
FROM ${__BLUE_MAIN_DB__}.emi_base_hr_waf waf
    --LEFT OUTER JOIN (
    --    SELECT  environment,min(yr_cd) as last_load_clndr_yr_cd
    --    FROM 
    --    (SELECT  environment,clnt_obj_id, max(yr_cd) as yr_cd 
    --    FROM ${__GREEN_MAIN_DB__}.emi_prep_retn_prac
    --GROUP BY environment,clnt_obj_id)IGN
    --GROUP BY environment
    --)t 
    ----LEFT OUTER JOIN ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_retn t
    --ON t.environment = waf.environment
    LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_job tdj
        ON  waf.clnt_obj_id = tdj.clnt_obj_id
            AND waf.db_schema = tdj.db_schema
            AND waf.d_job_cd = tdj.job_cd
            AND waf.environment = tdj.environment
    LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_loc worklocs
            ON waf.clnt_obj_id  = worklocs.clnt_obj_id
            AND waf.d_work_loc_cd = worklocs.work_loc_cd
            AND waf.db_schema = worklocs.db_schema
            AND waf.environment = worklocs.environment
    WHERE
        waf.yr_cd >= (YEAR(CURRENT_DATE()) - 3)
        AND waf.prmry_work_asgnmt_ind=1
        --AND (excld.clnt_obj_id  IS NULL OR excl.clnt_obj_id IS NOT NULL) -- exclusions
        AND   f_work_asgnmt_actv_ind=1 
        --AND waf.environment = '${environment}'
)final;
 

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_retn_prac PARTITION (environment)
SELECT 
    /*+ COALESCE(800) */   
    clnt_obj_id,    
    qtr_cd,
    mnth_cd,
    d_job_cd,
    d_hr_orgn_id,
    d_work_cntry_cd,
    d_work_state_cd,
    d_work_loc_cd,
    SUM(is_active_in_yr_strt) as is_active_in_strt_cnt,
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
    yr_cd,
    qtr_cd,
    mnth_cd,
    d_job_cd,
    d_hr_orgn_id,
    d_work_state_cd,
    d_work_cntry_cd,
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
        ELSE 0 END AS is_active_in_yr_strt,
    CASE WHEN (qtr_cd IS NULL AND mnth_cd IS NULL) THEN
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
        ELSE 0  END AS is_retained_in_end
   FROM ${__BLUE_MAIN_DB__}.emi_prep_retn_input 
   --WHERE environment='${environment}'
   GROUP BY environment,clnt_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_job_cd,d_hr_orgn_id,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd   
        GROUPING SETS ( (environment,clnt_obj_id,pers_obj_id, yr_cd),(environment,clnt_obj_id,pers_obj_id,yr_cd,d_job_cd),(environment,clnt_obj_id,pers_obj_id,yr_cd,d_hr_orgn_id) ,
        (environment,clnt_obj_id,pers_obj_id,yr_cd,d_work_cntry_cd,d_work_state_cd),(environment,clnt_obj_id,pers_obj_id,yr_cd,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd),
        (environment,clnt_obj_id, pers_obj_id,yr_cd,qtr_cd),(environment,clnt_obj_id,pers_obj_id,yr_cd,qtr_cd,d_job_cd),(environment,clnt_obj_id,pers_obj_id,yr_cd,qtr_cd,d_hr_orgn_id) ,
        (environment,clnt_obj_id,pers_obj_id,yr_cd,qtr_cd,d_work_cntry_cd,d_work_state_cd),(environment,clnt_obj_id,pers_obj_id,yr_cd,qtr_cd,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd),
        (environment,clnt_obj_id, pers_obj_id,yr_cd,qtr_cd,mnth_cd),(environment,clnt_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_job_cd),(environment,clnt_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_hr_orgn_id) ,
        (environment,clnt_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_work_cntry_cd,d_work_state_cd),(environment,clnt_obj_id,pers_obj_id,yr_cd,qtr_cd,mnth_cd,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd)))final
    GROUP BY environment,clnt_obj_id,yr_cd,qtr_cd,mnth_cd,d_job_cd,d_hr_orgn_id,d_work_cntry_cd,d_work_state_cd,d_work_loc_cd;
