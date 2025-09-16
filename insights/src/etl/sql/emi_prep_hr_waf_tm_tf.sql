-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_tm_tf(
    act_wk_end_dt TIMESTAMP,
    act_wk_strt_dt TIMESTAMP,
    clnt_obj_id STRING,
    d_job_cd STRING,
    eff_person_days DOUBLE,   
    f_work_asgnmt_actv_ind DOUBLE,
    f_work_asgnmt_stus_cd STRING,
    fwa_rec_eff_end_dt TIMESTAMP,
    fwa_rec_eff_strt_dt TIMESTAMP,
    supvr_pers_obj_id STRING,
    mnth_cd STRING,
    pers_obj_id STRING,
    qtr_cd STRING,
    rec_eff_end_dt TIMESTAMP,
    rec_eff_strt_dt TIMESTAMP,
    tnur_dt TIMESTAMP,
    tnur DOUBLE,
    work_asgnmt_nbr STRING,
    yr_wk_cd STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

--DROP TABLE IF EXISTS ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr;
--
--CREATE TABLE ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr
--USING PARQUET
--AS
--SELECT environment, min(yr_cd) as last_load_clndr_yr_cd
--FROM 
--    (SELECT environment, clnt_obj_id, max(yr_cd) as yr_cd 
--    FROM ${__GREEN_MAIN_DB__}.emi_prep_hr_waf_tm_tf
--    --WHERE environment = '${environment}'
--    GROUP BY environment, clnt_obj_id) IGN
--GROUP BY environment;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_tm_tf PARTITION(environment)
SELECT 
    /*+ COALESCE(800) */
    act_wk_end_dt,
    act_wk_strt_dt,
    clnt_obj_id,
    d_job_cd,
    eff_person_days,
    f_work_asgnmt_actv_ind,
    f_work_asgnmt_stus_cd,
    fwa_rec_eff_end_dt,
    fwa_rec_eff_strt_dt,
    supvr_pers_obj_id,
    mnth_cd,
    pers_obj_id,
    qtr_cd,
    rec_eff_end_dt,
    rec_eff_strt_dt,
    tnur_dt,
    tnur,
    work_asgnmt_nbr,
    yr_wk_cd,
    db_schema,
    yr_cd,
    environment
FROM 
    (SELECT
        act_wk_end_dt,
        act_wk_strt_dt,
        waf.work_asgnmt_nbr,
        waf.clnt_obj_id,
        d_job_cd,
        eff_person_days,
        f_work_asgnmt_actv_ind,
        f_work_asgnmt_stus_cd,
        fwa_rec_eff_end_dt,
        fwa_rec_eff_strt_dt,
        waf.supvr_pers_obj_id,
        waf.mnth_cd,
        waf.pers_obj_id,
        waf.qtr_cd,
        waf.rec_eff_end_dt,
        waf.rec_eff_strt_dt,
        tnur_dt,
        CASE WHEN to_date(act_wk_end_dt) <> to_date(waf.yr_end_dt)
                THEN NULL
             ELSE
                MONTHS_BETWEEN(waf.yr_end_dt,tnur_dt)/12
         END AS tnur,
        waf.environment,
        waf.db_schema,
        waf.yr_cd,
        waf.yr_wk_cd
    FROM
        ${__BLUE_MAIN_DB__}.emi_base_hr_waf waf
        --LEFT OUTER JOIN ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr t
        --ON 
        --    t.environment = waf.environment
        WHERE 
            waf.yr_cd >= (YEAR(CURRENT_DATE()) - 3)
            --AND waf.environment = '${environment}'
            --AND waf.f_work_asgnmt_actv_ind = 1
        ) ignored;

--DROP TABLE ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr;