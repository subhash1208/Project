-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode=2000;

-- Time security is applied using the dedicated security tables
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_etime_mngr (
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
    work_cntry_cd STRING,
    actl_hrs_in_secnd_nbr DOUBLE,
    ot_hrs_in_secnd DOUBLE,
    absence_hrs_in_secnd DOUBLE,
    num_employees INT,
    absence_hrs_event_count INT,
    ot_hrs_event_count INT,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_etime_mngr_agg (
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
    overtime_ratio DOUBLE,
    absence_ratio DOUBLE,
    num_employees INT,
    absence_hrs_event_count INT,
    ot_hrs_event_count INT,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');
 
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_etime;
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_etime (
    clnt_obj_id STRING,
    clndr_wk_cd STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    job_cd STRING,
    hr_orgn_id STRING,
    --gndr_cd STRING,
    paycd_id STRING,
    labor_acct_id STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    --work_city_cd STRING,
    work_cntry_cd STRING,
    --clnt_cnfrm_ind INT,
    --absence_ind INT,
    --ot_ind INT,
    --leav_ind INT,
    --reg_ind INT,
    --prem_tm_ind INT,
    --paid_tm_off_ind INT,
    --plan_absence_ind INT,
    actl_hrs_in_secnd_nbr DOUBLE,
    ot_hrs_in_secnd DOUBLE,
    absence_hrs_in_secnd DOUBLE,
    --actl_cost DOUBLE,
    --schdl_hrs_in_secnd_nbr DOUBLE,
    num_employees INT,
    absence_hrs_event_count INT,
    ot_hrs_event_count INT,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');
  
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_etime PARTITION(environment)
SELECT
    rtw.clnt_obj_id,
    rtw.clndr_wk_cd,
    --rtw.pers_obj_id,
    --perms.mngr_pers_obj_id,
    rtw.qtr_cd,
    rtw.mnth_cd,
    rtw.job_cd,
    rtw.hr_orgn_id,
    rtw.paycd_id,
    rtw.labor_acct_id,
    --rtw.gndr_cd,
    rtw.work_state_cd,
    rtw.work_loc_cd,
    --rtw.work_city_cd,
    rtw.work_cntry_cd,
    SUM(CASE WHEN actl_hrs_in_secnd_nbr IS NOT NULL THEN actl_hrs_in_secnd_nbr END) as actl_hrs_in_secnd_nbr,
    SUM(CASE WHEN (clnt_cnfrm_ind = 1 AND ot_ind = 1 and actl_hrs_in_secnd_nbr IS NOT NULL) THEN actl_hrs_in_secnd_nbr END) as ot_hrs_in_secnd,
    SUM(CASE WHEN (clnt_cnfrm_ind = 1 AND absence_ind = 1 and actl_hrs_in_secnd_nbr IS NOT NULL) THEN actl_hrs_in_secnd_nbr END) as absence_hrs_in_secnd,
    count(distinct pers_obj_id) as num_employees,
    COUNT(DISTINCT(CASE WHEN (clnt_cnfrm_ind = 1 AND ot_ind = 1 and actl_hrs_in_secnd_nbr IS NOT NULL) THEN pers_obj_id END)) as absence_hrs_event_count,
    COUNT(DISTINCT(CASE WHEN (clnt_cnfrm_ind = 1 AND ot_ind = 1 and actl_hrs_in_secnd_nbr IS NOT NULL) THEN pers_obj_id END)) as ot_hrs_event_count,
    rtw.db_schema,
    rtw.yr_cd,
    rtw.environment
FROM
(
    SELECT
        rtw.clnt_obj_id,
        rtw.pers_obj_id,
        rtw.clndr_wk_cd,
        --rtw.pers_obj_id,
        --perms.mngr_pers_obj_id,
        rtw.qtr_cd,
        rtw.mnth_cd,
        COALESCE(rtw.job_cd,'UNKNOWN') as job_cd,
        COALESCE(rtw.hr_orgn_id,'UNKNOWN') as hr_orgn_id,
        COALESCE(rtw.paycd_id,'UNKNOWN') as paycd_id,
        COALESCE(rtw.labor_acct_id,'UNKNOWN') as labor_acct_id,
        --rtw.gndr_cd,
        COALESCE(rtw.work_state_cd,'UNKNOWN') as work_state_cd,
        COALESCE(rtw.work_loc_cd,'UNKNOWN') as work_loc_cd,
        --rtw.work_city_cd,
        COALESCE(rtw.work_cntry_cd,'UNKNOWN') as work_cntry_cd,
        actl_hrs_in_secnd_nbr,
        clnt_cnfrm_ind,
        ot_ind,
        absence_ind,
        rtw.db_schema,
        rtw.environment,
        rtw.yr_cd
    FROM   
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf rtw
WHERE
--rtw.environment = '${environment}'
rtw.time_product = 'etime'
AND rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string))rtw
GROUP BY environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd,job_cd,hr_orgn_id,work_cntry_cd,work_state_cd,work_loc_cd,labor_acct_id,paycd_id 
        GROUPING SETS ( (environment,db_schema,clnt_obj_id, labor_acct_id,yr_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,job_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id, labor_acct_id, yr_cd,qtr_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,job_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id, labor_acct_id,yr_cd,qtr_cd,mnth_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,mnth_cd,job_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,mnth_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id, paycd_id,yr_cd),(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,job_cd),(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,paycd_id,yr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id, paycd_id, yr_cd,qtr_cd),(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,job_cd),(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id, paycd_id,yr_cd,qtr_cd,mnth_cd),(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,mnth_cd,job_cd),(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,mnth_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,paycd_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,job_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id, yr_cd,qtr_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,job_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,mnth_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,mnth_cd,job_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,mnth_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,labor_acct_id,paycd_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,yr_cd),(environment,db_schema,clnt_obj_id,yr_cd,job_cd),(environment,db_schema,clnt_obj_id,yr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,yr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,yr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,yr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id, yr_cd,qtr_cd),(environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,job_cd),(environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,mnth_cd),(environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,mnth_cd,job_cd),(environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,mnth_cd,hr_orgn_id) ,(environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd),
        (environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd),(environment,db_schema,clnt_obj_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd,work_loc_cd));
 
-- Multiple inserts for various combinations of mngr_accs_labor_acct_set_id, mngr_view_paycd_accs_prfl_id
-- This had to be done for performance reasons
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_etime_mngr PARTITION(environment)
SELECT
    clnt_obj_id,
    mngr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    work_state_cd,
    work_loc_cd,
    work_cntry_cd,
    actl_hrs_in_secnd_nbr,
    ot_hrs_in_secnd,
    absence_hrs_in_secnd,
    num_employees,
    absence_hrs_event_count,
    ot_hrs_event_count,
    db_schema,
    yr_cd,
    environment
FROM
(SELECT
    /*+ BROADCAST(emi_prep_paycd_lbracct) */
    rtw.clnt_obj_id,
    perms.mngr_pers_obj_id as mngr_pers_obj_id,
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
    rtw.actl_hrs_in_secnd_nbr,
    rtw.ot_hrs_in_secnd,
    rtw.absence_hrs_in_secnd,
    rtw.num_employees,
    rtw.absence_hrs_event_count,
    rtw.ot_hrs_event_count,
    rtw.db_schema,
    rtw.yr_cd,
    rtw.environment
FROM
    ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_etime rtw
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_paycd_lbracct perms
        ON rtw.environment = perms.environment
        AND rtw.db_schema = perms.db_schema
        AND rtw.clnt_obj_id = perms.clnt_obj_id
        AND rtw.labor_acct_id = perms.labor_acct_id
        AND rtw.paycd_id = perms.paycd_id
        AND perms.mngr_view_paycd_accs_prfl_id != '-1'
        AND perms.mngr_accs_labor_acct_set_id != '-1'
WHERE
--rtw.environment = '${environment}'
rtw.labor_acct_id IS NOT NULL
AND rtw.paycd_id IS NOT NULL
AND flag='PAY_LAB_SEC'

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
    rtw.actl_hrs_in_secnd_nbr,
    rtw.ot_hrs_in_secnd,
    rtw.absence_hrs_in_secnd,
    rtw.num_employees,
    rtw.absence_hrs_event_count,
    rtw.ot_hrs_event_count,
    rtw.db_schema,
    rtw.yr_cd,
    rtw.environment
FROM
    ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_etime rtw
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_paycd_lbracct perms
        ON rtw.environment = perms.environment
        AND rtw.db_schema = perms.db_schema
        AND rtw.clnt_obj_id = perms.clnt_obj_id
        AND perms.mngr_view_paycd_accs_prfl_id = '-1'
        AND perms.mngr_accs_labor_acct_set_id = '-1'
WHERE 
--rtw.environment = '${environment}'
rtw.labor_acct_id IS NULL
AND rtw.paycd_id IS NULL
AND flag='PAY_LAB_SEC'
 
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
    rtw.actl_hrs_in_secnd_nbr,
    rtw.ot_hrs_in_secnd,
    rtw.absence_hrs_in_secnd,
    rtw.num_employees,
    rtw.absence_hrs_event_count,
    rtw.ot_hrs_event_count,
    rtw.db_schema,
    rtw.yr_cd,
    rtw.environment
FROM
    ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_etime rtw
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_paycd_lbracct perms
        ON rtw.environment = perms.environment
        AND rtw.db_schema = perms.db_schema
        AND rtw.clnt_obj_id = perms.clnt_obj_id
        AND rtw.labor_acct_id = perms.labor_acct_id
        AND perms.mngr_view_paycd_accs_prfl_id = '-1'
        AND perms.mngr_accs_labor_acct_set_id != '-1'
WHERE
--rtw.environment = '${environment}'
rtw.labor_acct_id IS NOT NULL
AND rtw.paycd_id IS NULL
AND flag='PAY_LAB_SEC'

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
    rtw.actl_hrs_in_secnd_nbr,
    rtw.ot_hrs_in_secnd,
    rtw.absence_hrs_in_secnd,
    rtw.num_employees,
    rtw.absence_hrs_event_count,
    rtw.ot_hrs_event_count,
    rtw.db_schema,
    rtw.yr_cd,
    rtw.environment
FROM
    ${__BLUE_MAIN_DB__}.tmp_emi_prep_tm_tf_etime rtw
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_paycd_lbracct perms
        ON rtw.environment = perms.environment
        AND rtw.db_schema = perms.db_schema
        AND rtw.clnt_obj_id = perms.clnt_obj_id
        AND rtw.paycd_id = perms.paycd_id
        AND perms.mngr_view_paycd_accs_prfl_id != '-1'
        AND perms.mngr_accs_labor_acct_set_id = '-1'
WHERE
--rtw.environment = '${environment}'
rtw.paycd_id IS NOT NULL
AND rtw.labor_acct_id IS NULL
AND flag='PAY_LAB_SEC')final;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_etime_mngr_agg PARTITION(environment)
SELECT
    clnt_obj_id,
    --pers_obj_id,
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
    CASE WHEN total_hrs = 0 THEN 0 ELSE round(100 * overtime_hrs/total_hrs, 2) END as overtime_ratio,
    CASE WHEN total_hrs = 0 THEN 0 ELSE round(100 * absence_hrs/total_hrs, 2) END as absence_ratio,
    num_employees,
    absence_hrs_event_count,
    ot_hrs_event_count,
    yr_cd,
    environment
FROM
(SELECT
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
    SUM(actl_hrs_in_secnd_nbr)/3600 as total_hrs,
    SUM(ot_hrs_in_secnd)/3600 as overtime_hrs,
    SUM(absence_hrs_in_secnd)/3600 as absence_hrs,
    SUM(num_employees) as num_employees,
    SUM(absence_hrs_event_count) as absence_hrs_event_count,
    SUM(ot_hrs_event_count) as ot_hrs_event_count,
    environment,
    yr_cd
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_etime_mngr r
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
    environment,
    yr_cd)final;