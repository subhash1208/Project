-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Time security is applied using the dedicated security tables
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_mngr (
    clnt_obj_id STRING,
    --pers_obj_id STRING,
    mngr_pers_obj_id STRING,
    supvr_pers_obj_id STRING,
    pers_obj_id STRING,
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
    time_product STRING,
    rpt_access STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Time security is applied using the dedicated security tables
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_mngr (
    clnt_obj_id STRING,
    --pers_obj_id STRING,
    mngr_pers_obj_id STRING,
    supvr_pers_obj_id STRING,
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
    time_product STRING,
    rpt_access STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Time security is applied using the dedicated security tables
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_mngr (
    clnt_obj_id STRING,
    --pers_obj_id STRING,
    mngr_pers_obj_id STRING,
    supvr_pers_obj_id STRING,
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
    tm_sheet_item_id_ind INT,
    --tm_sheet_item_id_cnt_ind INT,
    num_employees INT,
    time_product STRING,
    rpt_access STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_mngr PARTITION(environment)
SELECT
    /*+ COALESCE(800) */ 
    clnt_obj_id,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
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
    --num_employees,
    time_product,
    rpt_access,
    db_schema,
    yr_cd,
    environment
FROM
(SELECT 
    clnt_obj_id,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
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
    --num_employees,
    time_product,
    rpt_access,
    db_schema,
    environment,
    yr_cd
FROM
    (SELECT 
    clnt_obj_id,
    mngr_pers_obj_id,
    NULL AS supvr_pers_obj_id,
    pers_obj_id,
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
    'ezlm' as time_product,
    null as rpt_access,
    r2.db_schema,
    environment,
    yr_cd
FROM 
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_ezlm_mngr r2
WHERE 
    --environment = '${environment}'
    yr_cd >= cast(year(add_months(current_date, -24)) as string)
)a
UNION ALL
SELECT 
    clnt_obj_id,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
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
    --num_employees,
    time_product,
    rpt_access,
    db_schema,
    environment,
    yr_cd
FROM
(SELECT 
    clnt_obj_id,
    NULL AS mngr_pers_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
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
    'ezlm' as time_product,
    r2.rpt_access,
    r2.db_schema,
    environment,
    yr_cd
FROM 
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_ezlm_supvr r2
WHERE 
    --environment = '${environment}'
    yr_cd >= cast(year(add_months(current_date, -24)) as string)
)b)final;


INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_mngr PARTITION(environment)
SELECT 
    /*+ COALESCE(800) */
    clnt_obj_id,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,   
    work_state_cd,
    work_loc_cd,    
    work_cntry_cd,
    excp_type,
    excep_hrs_in_secnd_nbr,
    excp_ind,
    num_employees,
    time_product,
    rpt_access,
    db_schema,
    yr_cd,
    environment
FROM
(SELECT 
    clnt_obj_id,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,   
    work_state_cd,
    work_loc_cd,    
    work_cntry_cd,
    excp_type,
    excep_hrs_in_secnd_nbr,
    excp_ind,
    num_employees,
    time_product,
    rpt_access,
    db_schema,
    environment,
    yr_cd
FROM
(SELECT 
    clnt_obj_id,
    mngr_pers_obj_id,
    NULL AS supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    excp_type,
    SUM(excep_hrs_in_secnd_nbr) as excep_hrs_in_secnd_nbr,
    SUM(excp_ind) as excp_ind,
    count(distinct pers_obj_id) as num_employees,
    'ezlm' as time_product,
    null as rpt_access,
    r2.db_schema,
    environment,
    yr_cd
FROM 
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_ezlm_mngr r2
WHERE 
    --environment = '${environment}'
    yr_cd >= cast(year(add_months(current_date, -24)) as string)
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
    --work_city_cd,
    work_cntry_cd,
    excp_type,
    environment,
    r2.db_schema,
    yr_cd)a
UNION ALL
SELECT 
    clnt_obj_id,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,    
    work_state_cd,
    work_loc_cd,    
    work_cntry_cd,
    excp_type,
    excep_hrs_in_secnd_nbr,
    excp_ind,
    num_employees,
    time_product,
    rpt_access,
    db_schema,
    environment,
    yr_cd
FROM
(SELECT 
    clnt_obj_id,
    NULL AS mngr_pers_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    excp_type,
    SUM(excep_hrs_in_secnd_nbr) as excep_hrs_in_secnd_nbr,
    SUM(excp_ind) as excp_ind,
    count(distinct pers_obj_id) as num_employees,
    'ezlm' as time_product,
    r2.rpt_access,
    r2.db_schema,
    environment,
    yr_cd
FROM 
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_ezlm_supvr r2
WHERE 
    --environment = '${environment}'
    yr_cd >= cast(year(add_months(current_date, -24)) as string)
GROUP BY 
    clnt_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    excp_type,
    r2.rpt_access,
    environment,
    r2.db_schema,
    yr_cd)b)final;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_mngr PARTITION(environment)
SELECT 
    /*+ COALESCE(800) */
    clnt_obj_id,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    tm_sheet_item_id_ind,
    --SUM(tm_sheet_item_id_cnt_ind) as tm_sheet_item_id_cnt_ind,
    num_employees,
    time_product,
    rpt_access,
    db_schema,
    yr_cd,
    environment
FROM
(SELECT 
    clnt_obj_id,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    tm_sheet_item_id_ind,
    --SUM(tm_sheet_item_id_cnt_ind) as tm_sheet_item_id_cnt_ind,
    num_employees,
    time_product,
    rpt_access,
    db_schema,
    environment,
    yr_cd
FROM
(SELECT 
    clnt_obj_id,
    mngr_pers_obj_id,
    NULL AS supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    SUM(tm_sheet_item_id_ind) as tm_sheet_item_id_ind,
    --SUM(tm_sheet_item_id_cnt_ind) as tm_sheet_item_id_cnt_ind,
    count(distinct pers_obj_id) as num_employees,
    'ezlm' as time_product,
    null as rpt_access,
    r2.db_schema,
    environment,
    yr_cd
FROM 
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_ezlm_mngr r2
WHERE 
    --environment = '${environment}'
    yr_cd >= cast(year(add_months(current_date, -24)) as string)
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
    --work_city_cd,
    work_cntry_cd,
    environment,
    r2.db_schema,
    yr_cd)a
UNION ALL
SELECT 
    clnt_obj_id,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    tm_sheet_item_id_ind,
    --SUM(tm_sheet_item_id_cnt_ind) as tm_sheet_item_id_cnt_ind,
    num_employees,
    time_product,
    rpt_access,
    db_schema,
    environment,
    yr_cd
FROM
(SELECT 
    clnt_obj_id,
    NULL AS mngr_pers_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    SUM(tm_sheet_item_id_ind) as tm_sheet_item_id_ind,
    --SUM(tm_sheet_item_id_cnt_ind) as tm_sheet_item_id_cnt_ind,
    count(distinct pers_obj_id) as num_employees,
    'ezlm' as time_product,
    r2.rpt_access,
    r2.db_schema,
    environment,
    yr_cd
FROM 
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_ezlm_supvr r2
WHERE 
    --environment = '${environment}'
    yr_cd >= cast(year(add_months(current_date, -24)) as string)
GROUP BY 
    clnt_obj_id,
    supvr_pers_obj_id,
    qtr_cd,
    mnth_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    --gndr_cd,
    work_state_cd,
    work_loc_cd,
    --work_city_cd,
    work_cntry_cd,
    r2.rpt_access,
    environment,
    r2.db_schema,
    yr_cd)b)final;