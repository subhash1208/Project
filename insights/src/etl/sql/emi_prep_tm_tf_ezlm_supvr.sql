-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Time security for EZLM products. Applied using the SUPVR_RPT_TO_HRCHY
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_ezlm_supvr (
    clnt_obj_id STRING, 
    pers_obj_id STRING,
    supvr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    clndr_wk_cd STRING,
    full_tm_part_tm_cd STRING,
    job_cd STRING,
    pay_grp_cd STRING,
    cmpny_cd STRING,
    work_asgnmt_stus_cd STRING,
    gndr_cd STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    work_city_cd STRING,
    work_cntry_cd STRING,
    paycd_id STRING,
    paycd_grp_cd STRING,
    hr_orgn_id STRING,
    payrl_orgn_id STRING,
    pay_rt_type_cd STRING,
    actl_hrs_in_secnd_nbr DOUBLE,
    ot_hrs_in_secnd DOUBLE,
    absence_hrs_in_secnd DOUBLE,
    rpt_access STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Time security for EZLM products. Applied using the RPT_TO_HRCHY
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_ezlm_supvr (
    clnt_obj_id STRING, 
    pers_obj_id STRING,
    supvr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    clndr_wk_cd STRING,
    full_tm_part_tm_cd STRING,
    job_cd STRING,
    pay_grp_cd STRING,
    cmpny_cd STRING,
    work_asgnmt_stus_cd STRING,
    gndr_cd STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    work_city_cd STRING,
    work_cntry_cd STRING,
    hr_orgn_id STRING,
    payrl_orgn_id STRING,
    pay_rt_type_cd STRING,
    excep_hrs_in_secnd_nbr DOUBLE,
    excp_type STRING,
    excp_ind INT,
    rpt_access STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- This table is used only in case of WFM People based insights, which contains Resolved and Unresolved Missed Punches
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_mssd_pnch_supvr (
    clnt_obj_id STRING, 
    pers_obj_id STRING,
    supvr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    clndr_wk_cd STRING,
    full_tm_part_tm_cd STRING,
    job_cd STRING,
    pay_grp_cd STRING,
    cmpny_cd STRING,
    work_asgnmt_stus_cd STRING,
    gndr_cd STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    work_city_cd STRING,
    work_cntry_cd STRING,
    hr_orgn_id STRING,
    payrl_orgn_id STRING,
    pay_rt_type_cd STRING,
    excep_hrs_in_secnd_nbr DOUBLE,
    excp_type STRING,
    excp_ind INT,
    rslv_ind INT,
    rpt_access STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Time security for EZLM products. Applied using the RPT_TO_HRCHY
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_ezlm_supvr (
    clnt_obj_id STRING, 
    pers_obj_id STRING,
    supvr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    clndr_wk_cd STRING,
    job_cd STRING,
    gndr_cd STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    work_city_cd STRING,
    work_cntry_cd STRING,
    hr_orgn_id STRING,
    tm_sheet_item_id_ind INT,
    rpt_access STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_ezlm_supvr PARTITION(environment)
SELECT
  /*+ COALESCE(800) */
  clnt_obj_id,
  pers_obj_id,
  supvr_pers_obj_id,
  qtr_cd,
  mnth_cd,
  clndr_wk_cd,
  full_tm_part_tm_cd,
  job_cd,
  pay_grp_cd,
  cmpny_cd,
  work_asgnmt_stus_cd,
  gndr_cd,
  work_state_cd,
  work_loc_cd,
  work_city_cd,
  work_cntry_cd,
  paycd_id,
  paycd_grp_cd,
  hr_orgn_id,
  payrl_orgn_id,
  pay_rt_type_cd,
  actl_hrs_in_secnd_nbr ,
  ot_hrs_in_secnd,
  absence_hrs_in_secnd,
  rpt_access,
  db_schema,
  yr_cd,
  environment
FROM
(SELECT 
       /*+ BROADCAST(emi_prep_supvr_hrchy) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.login_supvr_pers_obj_id as supvr_pers_obj_id,
       rtw.qtr_cd,
       rtw.mnth_cd,
       rtw.clndr_wk_cd,
       rtw.full_tm_part_tm_cd,
       rtw.job_cd,
       rtw.pay_grp_cd,
       rtw.cmpny_cd,
       rtw.work_asgnmt_stus_cd,
       rtw.gndr_cd,
       rtw.work_state_cd,
       rtw.work_loc_cd,
       rtw.work_city_cd,
       rtw.work_cntry_cd,
       rtw.paycd_id,
       rtw.paycd_grp_cd,
       rtw.hr_orgn_id,
       rtw.payrl_orgn_id,
       rtw.pay_rt_type_cd,
       CASE WHEN rtw.actl_hrs_in_secnd_nbr IS NOT NULL THEN rtw.actl_hrs_in_secnd_nbr END as actl_hrs_in_secnd_nbr,
       CASE WHEN (rtw.clnt_cnfrm_ind = 1 AND rtw.ot_ind = 1 and rtw.actl_hrs_in_secnd_nbr IS NOT NULL) THEN rtw.actl_hrs_in_secnd_nbr END as ot_hrs_in_secnd,
       CASE WHEN (rtw.clnt_cnfrm_ind = 1 AND rtw.absence_ind = 1 and rtw.actl_hrs_in_secnd_nbr IS NOT NULL) THEN rtw.actl_hrs_in_secnd_nbr END as absence_hrs_in_secnd,
       rtw.time_product,
       rl.rpt_access,
       rtw.environment,
       rtw.db_schema,
       rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_supvr_hrchy rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND (case when rl.work_asgnmt_nbr is null then rtw.supvr_pers_obj_id = rl.supvr_pers_obj_id 
        else rtw.work_asgnmt_nbr=rl.work_asgnmt_nbr end)
    AND rtw.pers_obj_id = rl.pers_obj_id
    AND rl.Login_supvr_pers_obj_id IS NOT NULL
)rtw
WHERE 
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.supvr_pers_obj_id IS NOT NULL 
    AND rtw.supvr_pers_obj_id != 'UNKNOWN';


INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_ezlm_supvr PARTITION(environment)
SELECT
  /*+ COALESCE(800) */
  clnt_obj_id,
  pers_obj_id,
  supvr_pers_obj_id,
  qtr_cd,
  mnth_cd,
  clndr_wk_cd,
  full_tm_part_tm_cd,
  job_cd,
  pay_grp_cd,
  cmpny_cd,
  work_asgnmt_stus_cd,
  gndr_cd,
  work_state_cd,
  work_loc_cd,
  work_city_cd,
  work_cntry_cd,
  hr_orgn_id,
  payrl_orgn_id,
  pay_rt_type_cd,
  excep_hrs_in_secnd_nbr,
  excp_type,
  excp_ind,
  rpt_access,
  db_schema,
  yr_cd,
  environment
FROM
(SELECT 
    /*+ BROADCAST(emi_prep_supvr_hrchy) */
    rtw.clnt_obj_id,
    rtw.pers_obj_id,
    rl.Login_supvr_pers_obj_id as supvr_pers_obj_id,
    rtw.qtr_cd,
    rtw.mnth_cd,
    rtw.clndr_wk_cd,
    rtw.full_tm_part_tm_cd,
    rtw.job_cd,
    rtw.pay_grp_cd,
    rtw.cmpny_cd,
    rtw.work_asgnmt_stus_cd,
    rtw.gndr_cd,
    rtw.work_state_cd,
    rtw.work_loc_cd,
    rtw.work_city_cd,
    rtw.work_cntry_cd,
    rtw.hr_orgn_id,
    rtw.payrl_orgn_id,
    rtw.pay_rt_type_cd,
    rtw.excep_hrs_in_secnd_nbr,
    rtw.excp_type,
    rtw.excp_ind,
    rtw.time_product,
    rl.rpt_access,
    rtw.environment,
    rtw.db_schema,
    rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_supvr_hrchy rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND (case when rl.work_asgnmt_nbr is null then rtw.supvr_pers_obj_id = rl.supvr_pers_obj_id 
        else rtw.work_asgnmt_nbr=rl.work_asgnmt_nbr end)
    AND rtw.pers_obj_id = rl.pers_obj_id
    AND rl.login_supvr_pers_obj_id IS NOT NULL
    )rtw
WHERE 
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.supvr_pers_obj_id IS NOT NULL 
    AND rtw.supvr_pers_obj_id != 'UNKNOWN';



-- These tables are used in case of WFM People based insights
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_mssd_pnch_supvr PARTITION(environment)
SELECT
  /*+ COALESCE(800) */
  clnt_obj_id,
  pers_obj_id,
  supvr_pers_obj_id,
  qtr_cd,
  mnth_cd,
  clndr_wk_cd,
  full_tm_part_tm_cd,
  job_cd,
  pay_grp_cd,
  cmpny_cd,
  work_asgnmt_stus_cd,
  gndr_cd,
  work_state_cd,
  work_loc_cd,
  work_city_cd,
  work_cntry_cd,
  hr_orgn_id,
  payrl_orgn_id,
  pay_rt_type_cd,
  excep_hrs_in_secnd_nbr,
  excp_type,
  excp_ind,
  rslv_ind,
  rpt_access,
  db_schema,
  yr_cd,
  environment
FROM
(SELECT 
    /*+ BROADCAST(emi_prep_supvr_hrchy) */
    rtw.clnt_obj_id,
    rtw.pers_obj_id,
    rl.login_supvr_pers_obj_id as supvr_pers_obj_id,
    rtw.qtr_cd,
    rtw.mnth_cd,
    rtw.clndr_wk_cd,
    rtw.full_tm_part_tm_cd,
    rtw.job_cd,
    rtw.pay_grp_cd,
    rtw.cmpny_cd,
    rtw.work_asgnmt_stus_cd,
    rtw.gndr_cd,
    rtw.work_state_cd,
    rtw.work_loc_cd,
    rtw.work_city_cd,
    rtw.work_cntry_cd,
    rtw.hr_orgn_id,
    rtw.payrl_orgn_id,
    rtw.pay_rt_type_cd,
    rtw.excep_hrs_in_secnd_nbr,
    rtw.excp_type,
    rtw.excp_ind,
    rtw.time_product,
    rtw.rslv_ind,
    rl.rpt_access,
    rtw.environment,
    rtw.db_schema,
    rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_mssd_pnchs rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_supvr_hrchy rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND (case when rl.work_asgnmt_nbr is null then rtw.supvr_pers_obj_id = rl.supvr_pers_obj_id 
        else rtw.work_asgnmt_nbr=rl.work_asgnmt_nbr end)
    AND rtw.pers_obj_id = rl.pers_obj_id
    AND rl.login_supvr_pers_obj_id IS NOT NULL
    )rtw
WHERE 
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.supvr_pers_obj_id IS NOT NULL 
    AND rtw.supvr_pers_obj_id != 'UNKNOWN';



INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_ezlm_supvr PARTITION(environment)
SELECT
  /*+ COALESCE(800) */
  clnt_obj_id,
  pers_obj_id,
  supvr_pers_obj_id,
  qtr_cd,
  mnth_cd,
  clndr_wk_cd,       
  job_cd,       
  gndr_cd,
  work_state_cd,
  work_loc_cd,
  work_city_cd,
  work_cntry_cd,       
  hr_orgn_id,       
  tm_sheet_item_id_ind,
  rpt_access,
  db_schema,
  yr_cd,
  environment
FROM
(SELECT 
    /*+ BROADCAST(emi_prep_supvr_hrchy) */
    rtw.clnt_obj_id,
    rtw.pers_obj_id,
    rl.login_supvr_pers_obj_id as supvr_pers_obj_id,
    rtw.qtr_cd,
    rtw.mnth_cd,
    rtw.clndr_wk_cd,       
    rtw.job_cd,       
    rtw.gndr_cd,
    rtw.work_state_cd,
    rtw.work_loc_cd,
    rtw.work_city_cd,
    rtw.work_cntry_cd,       
    rtw.hr_orgn_id,       
    rtw.tm_sheet_item_id_ind,
    rtw.time_product,
    rl.rpt_access,
    rtw.environment,
    rtw.db_schema,
    rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_supvr_hrchy rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND (case when rl.work_asgnmt_nbr is null then rtw.supvr_pers_obj_id = rl.supvr_pers_obj_id 
        else rtw.work_asgnmt_nbr=rl.work_asgnmt_nbr end)
    AND rtw.pers_obj_id = rl.pers_obj_id
    AND rl.login_supvr_pers_obj_id IS NOT NULL
    )rtw
WHERE 
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.supvr_pers_obj_id IS NOT NULL 
    AND rtw.supvr_pers_obj_id != 'UNKNOWN';