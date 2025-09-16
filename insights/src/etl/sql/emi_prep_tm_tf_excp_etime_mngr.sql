-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode=2000;

-- Aggregated table to be used for insight generation instead of cube.This is applicable only for etime.
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_etime_mngr (
    clnt_obj_id STRING,
    mngr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    clndr_wk_cd STRING,
    job_cd STRING,
    hr_orgn_id STRING,
    --gndr_cd STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    --work_city_cd STRING,
    work_cntry_cd STRING,
    excp_type STRING,
    excep_hrs_in_secnd_nbr DOUBLE,
    excp_ind INT,     
    num_employees INT,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Aggregated table to be used for insight generation instead of cube.This is applicable only for etime.
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_etime_mngr_agg (
    clnt_obj_id STRING,
    mngr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    clndr_wk_cd STRING,
    job_cd STRING,
    hr_orgn_id STRING,
    --gndr_cd STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    --work_city_cd STRING,
    work_cntry_cd STRING,
    excp_type STRING,
    exception_hours DOUBLE,
    exception_count INT,
    num_employees INT,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_excp_etime;
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_excp_etime (
    clnt_obj_id STRING,
    clndr_wk_cd STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    job_cd STRING,
    hr_orgn_id STRING,
    --gndr_cd STRING,
    --paycd_id STRING,
    labor_acct_id STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    --work_city_cd STRING,
    work_cntry_cd STRING,
    excp_type STRING,
    excep_hrs_in_secnd_nbr DOUBLE,
    excp_ind INT,
    num_employees INT,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');
 
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_excp_etime PARTITION (environment)
SELECT
    /*+ COALESCE(800) */
    clnt_obj_id,
    clndr_wk_cd,
    qtr_cd,
    mnth_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    --paycd_id,
    labor_acct_id,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    excp_type,
    SUM(excep_hrs_in_secnd_nbr) as excep_hrs_in_secnd_nbr,
    SUM(excp_ind) as excp_ind,
    count(distinct pers_obj_id) as num_employees,
    db_schema,
    yr_cd,
    environment
FROM
(
    SELECT
        clnt_obj_id,
        pers_obj_id,
        clndr_wk_cd,
        qtr_cd,
        mnth_cd,
        COALESCE(job_cd,'UNKNOWN') as job_cd,
        COALESCE(hr_orgn_id,'UNKNOWN') as hr_orgn_id,
        --gndr_cd,
        --paycd_id,
        COALESCE(labor_acct_id,'UNKNOWN') as labor_acct_id,
        COALESCE(work_state_cd,'UNKNOWN') as work_state_cd,
        COALESCE(work_loc_cd,'UNKNOWN') as work_loc_cd,
       --work_city_cd,
        COALESCE(work_cntry_cd,'UNKNOWN') as work_cntry_cd,
        excp_type,
        excep_hrs_in_secnd_nbr,
        excp_ind,       
        db_schema,
        environment,
        yr_cd
    FROM
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp
    WHERE
    --environment = '${environment}'
    time_product = 'etime'
    AND yr_cd >= cast(year(add_months(current_date, -24)) as string)
)rtw
    GROUP BY environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd,job_cd,hr_orgn_id,work_cntry_cd,work_state_cd,work_loc_cd,labor_acct_id,excp_type 
        GROUPING SETS ( (environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd),(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,job_cd),(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,excp_type,labor_acct_id, yr_cd,qtr_cd),(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,job_cd),(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,mnth_cd),(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,mnth_cd,job_cd),(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,mnth_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,excp_type,labor_acct_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,excp_type,yr_cd),(environment,db_schema,clnt_obj_id,excp_type,yr_cd,job_cd),(environment,db_schema,clnt_obj_id,excp_type,yr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,excp_type,yr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,excp_type,yr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,excp_type,yr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,excp_type, yr_cd,qtr_cd),(environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,job_cd),(environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,mnth_cd),(environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,mnth_cd,job_cd),(environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,mnth_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,excp_type,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd,work_loc_cd));
 
-- Multiple inserts for various combinations of mngr_accs_labor_acct_set_id, mngr_view_paycd_accs_prfl_id
-- This had to be done for performance reasons
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_etime_mngr PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    clnt_obj_id,
    mngr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --rtw.gndr_cd,
    work_state_cd,
    work_loc_cd,
    --rtw.work_city_cd,
    work_cntry_cd,
    excp_type,
    excep_hrs_in_secnd_nbr,
    excp_ind,   
    num_employees,
    db_schema,
    yr_cd,
    environment
FROM
(SELECT
    /*+ BROADCAST(emi_prep_paycd_lbracct) */
    rtw.clnt_obj_id,
    perms.mngr_pers_obj_id,
    rtw.qtr_cd,
    rtw.mnth_cd,
    rtw.clndr_wk_cd,
    rtw.job_cd,
    rtw.hr_orgn_id,
    --rtw.gndr_cd,
    rtw.work_state_cd,
    rtw.work_loc_cd,
    --rtw.work_city_cd,
   rtw.work_cntry_cd,
    rtw.excp_type,
    rtw.excep_hrs_in_secnd_nbr,
    rtw.excp_ind,   
    rtw.num_employees,
    rtw.db_schema,
    rtw.yr_cd,
    rtw.environment
FROM
    ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_excp_etime rtw
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_paycd_lbracct perms
    ON rtw.environment = perms.environment
    AND rtw.db_schema = perms.db_schema
    AND rtw.clnt_obj_id = perms.clnt_obj_id
    AND rtw.labor_acct_id = perms.labor_acct_id
    AND perms.mngr_accs_labor_acct_set_id != '-1'
WHERE
--rtw.environment = '${environment}'
rtw.labor_acct_id IS NOT NULL
AND flag='LAB_SEC'

UNION ALL

SELECT
    /*+ BROADCAST(emi_prep_paycd_lbracct) */
    rtw.clnt_obj_id,
    perms.mngr_pers_obj_id,
    rtw.qtr_cd,
    rtw.mnth_cd,
    rtw.clndr_wk_cd,
    rtw.job_cd,
    rtw.hr_orgn_id,
    --rtw.gndr_cd,
    rtw.work_state_cd,
    rtw.work_loc_cd,
    --rtw.work_city_cd,
    rtw.work_cntry_cd,
    rtw.excp_type,
    rtw.excep_hrs_in_secnd_nbr,
    rtw.excp_ind,
    rtw.num_employees,
    rtw.db_schema,
    rtw.yr_cd,
    rtw.environment
FROM
    ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_excp_etime rtw
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_paycd_lbracct perms
        ON rtw.environment = perms.environment
        AND rtw.db_schema = perms.db_schema
        AND rtw.clnt_obj_id = perms.clnt_obj_id
        AND perms.mngr_accs_labor_acct_set_id = '-1'
WHERE
--rtw.environment = '${environment}'
rtw.labor_acct_id IS NULL AND flag='LAB_SEC')
final;
 
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_etime_mngr_agg PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    clnt_obj_id,
    mngr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    work_cntry_cd,
    excp_type,
    SUM(excep_hrs_in_secnd_nbr)/3600 as exception_hours,
    SUM(excp_ind) as exception_count,
    SUM(num_employees) as num_employees,
    yr_cd,
    environment
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_etime_mngr r
--WHERE
--    environment = '${environment}'
GROUP BY
    clnt_obj_id,
    mngr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    work_cntry_cd,
    excp_type,
    environment,
    yr_cd;