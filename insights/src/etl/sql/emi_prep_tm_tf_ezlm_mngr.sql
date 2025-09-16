-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Time security for EZLM products. Applied using the RPT_TO_HRCHY
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_ezlm_mngr (
    clnt_obj_id STRING, 
    pers_obj_id STRING,
    mngr_pers_obj_id STRING,
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
    --clnt_cnfrm_ind INT,
    --absence_ind INT,
    --ot_ind INT,
    --leav_ind INT,
    --reg_ind INT,
    --prem_tm_ind INT,
    --paid_tm_off_ind INT,
    --plan_absence_ind INT,
    --paycd_clsfn_cd STRING,
    hr_orgn_id STRING,
    payrl_orgn_id STRING,
    pay_rt_type_cd STRING,
    actl_hrs_in_secnd_nbr DOUBLE,
    ot_hrs_in_secnd DOUBLE,
    absence_hrs_in_secnd DOUBLE,
    db_schema STRING,
     yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Time security for EZLM products. Applied using the RPT_TO_HRCHY
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_ezlm_mngr (
    clnt_obj_id STRING, 
    pers_obj_id STRING,
    mngr_pers_obj_id STRING,
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
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

-- Time security for EZLM products. Applied using the RPT_TO_HRCHY
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_ezlm_mngr (
    clnt_obj_id STRING, 
    pers_obj_id STRING,
    mngr_pers_obj_id STRING,
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
    --tm_sheet_item_id_cnt_ind INT,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_ezlm_mngr PARTITION(environment)
SELECT 
    /*+ COALESCE(800) */
    clnt_obj_id,
    pers_obj_id,
    mngr_pers_obj_id,
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
    --clnt_cnfrm_ind,
    --absence_ind,
    --ot_ind,
    --leav_ind,
    --reg_ind,
    --prem_tm_ind,
    --paid_tm_off_ind,
    --plan_absence_ind,
    --paycd_clsfn_cd,
    hr_orgn_id,
    payrl_orgn_id,
    pay_rt_type_cd,
    actl_hrs_in_secnd_nbr,
    ot_hrs_in_secnd,
    absence_hrs_in_secnd,
    db_schema,
    yr_cd,
    environment
FROM
(SELECT 
       /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.mngr_pers_obj_id,
       rtw.qtr_cd,
       rtw.mnth_cd,
       rtw.clndr_wk_cd,
       rtw.full_tm_part_tm_cd,
       rtw.job_cd,
       rtw.pay_grp_cd,
       rtw.time_product,
       rtw.cmpny_cd,
       rtw.work_asgnmt_stus_cd,
       rtw.gndr_cd,
       rtw.work_state_cd,
       rtw.work_loc_cd,
       rtw.work_city_cd,
       rtw.work_cntry_cd,
       rtw.paycd_id,
       rtw.paycd_grp_cd,
       --rtw.clnt_cnfrm_ind,
       --rtw.absence_ind,
       --rtw.ot_ind,
       --rtw.leav_ind,
       --rtw.reg_ind,
       --rtw.prem_tm_ind,
       --rtw.paid_tm_off_ind,
       --rtw.plan_absence_ind,
       --rtw.paycd_clsfn_cd,
       rtw.hr_orgn_id,
       rtw.payrl_orgn_id,
       rtw.pay_rt_type_cd,
       CASE WHEN rtw.actl_hrs_in_secnd_nbr IS NOT NULL THEN rtw.actl_hrs_in_secnd_nbr END as actl_hrs_in_secnd_nbr,
       CASE WHEN (rtw.clnt_cnfrm_ind = 1 AND rtw.ot_ind = 1 and rtw.actl_hrs_in_secnd_nbr IS NOT NULL) THEN rtw.actl_hrs_in_secnd_nbr END as ot_hrs_in_secnd,
       CASE WHEN (rtw.clnt_cnfrm_ind = 1 AND rtw.absence_ind = 1 and rtw.actl_hrs_in_secnd_nbr IS NOT NULL) THEN rtw.actl_hrs_in_secnd_nbr END as absence_hrs_in_secnd,
       rtw.environment,
       rtw.db_schema,
       rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND rtw.mngr_pers_obj_id = rl.pers_obj_id
    AND rl.mngr_pers_obj_id IS NOT NULL
    AND rtw.trans_dt BETWEEN rl.rec_eff_strt_dt AND rl.rec_eff_end_dt
UNION ALL
SELECT 
       /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.mngr_pers_obj_id,
       rtw.qtr_cd,
       rtw.mnth_cd,
       rtw.clndr_wk_cd,
       rtw.full_tm_part_tm_cd,
       rtw.job_cd,
       rtw.pay_grp_cd,
       rtw.time_product,
       rtw.cmpny_cd,
       rtw.work_asgnmt_stus_cd,
       rtw.gndr_cd,
       rtw.work_state_cd,
       rtw.work_loc_cd,
       rtw.work_city_cd,
       rtw.work_cntry_cd,
       rtw.paycd_id,
       rtw.paycd_grp_cd,
       --rtw.clnt_cnfrm_ind,
       --rtw.absence_ind,
       --rtw.ot_ind,
       --rtw.leav_ind,
       --rtw.reg_ind,
       --rtw.prem_tm_ind,
       --rtw.paid_tm_off_ind,
       --rtw.plan_absence_ind,
       --rtw.paycd_clsfn_cd,
       rtw.hr_orgn_id,
       rtw.payrl_orgn_id,
       rtw.pay_rt_type_cd,
       CASE WHEN rtw.actl_hrs_in_secnd_nbr IS NOT NULL THEN rtw.actl_hrs_in_secnd_nbr END as actl_hrs_in_secnd_nbr,
       CASE WHEN (rtw.clnt_cnfrm_ind = 1 AND rtw.ot_ind = 1 and rtw.actl_hrs_in_secnd_nbr IS NOT NULL) THEN rtw.actl_hrs_in_secnd_nbr END as ot_hrs_in_secnd,
       CASE WHEN (rtw.clnt_cnfrm_ind = 1 AND rtw.absence_ind = 1 and rtw.actl_hrs_in_secnd_nbr IS NOT NULL) THEN rtw.actl_hrs_in_secnd_nbr END as absence_hrs_in_secnd,
       rtw.environment,
       rtw.db_schema,
       rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND rtw.pers_obj_id = rl.pers_obj_id
    AND mtrx_hrchy_ind = 1
    AND rl.mngr_pers_obj_id IS NOT NULL
)rtw
WHERE 
    --rtw.environment = '${environment}'
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.mngr_pers_obj_id IS NOT NULL 
    AND rtw.mngr_pers_obj_id != 'UNKNOWN';

-- Temporary table which will hold which persons are already captured as a part of manager hierarchy
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm 
USING PARQUET 
AS
SELECT
    DISTINCT waf.clnt_obj_id,
    waf.pers_obj_id,
    rl.mngr_pers_obj_id,
    waf.environment,
    waf.db_schema,
    waf.yr_cd,
    waf.qtr_cd,
    waf.mnth_cd
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf waf
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
        ON waf.environment = rl.environment
        AND waf.db_schema = rl.db_schema
        AND waf.clnt_obj_id = rl.clnt_obj_id
        AND waf.mngr_pers_obj_id = rl.pers_obj_id
        AND waf.trans_dt BETWEEN rl.rec_eff_strt_dt and rl.rec_eff_end_dt
WHERE 
    waf.mngr_pers_obj_id IS NOT NULL;

-- Cherrypick records
INSERT INTO TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_ezlm_mngr PARTITION(environment)
SELECT 
       /*+ BROADCAST(dwh_t_dim_mngr_cherrypick,tmp_emi_hrchy_mngrs_tm), COALESCE(800) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.mngr_pers_obj_id as mngr_pers_obj_id,
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
       --rtw.clnt_cnfrm_ind,
       --rtw.absence_ind,
       --rtw.ot_ind,
       --rtw.leav_ind,
       --rtw.reg_ind,
       --rtw.prem_tm_ind,
       --rtw.paid_tm_off_ind,
       --rtw.plan_absence_ind,
       --rtw.paycd_clsfn_cd,
       rtw.hr_orgn_id,
       rtw.payrl_orgn_id,
       rtw.pay_rt_type_cd,
       CASE WHEN rtw.actl_hrs_in_secnd_nbr IS NOT NULL THEN rtw.actl_hrs_in_secnd_nbr END as actl_hrs_in_secnd_nbr,
       CASE WHEN (rtw.clnt_cnfrm_ind = 1 AND rtw.ot_ind = 1 and rtw.actl_hrs_in_secnd_nbr IS NOT NULL) THEN rtw.actl_hrs_in_secnd_nbr END as ot_hrs_in_secnd,
       CASE WHEN (rtw.clnt_cnfrm_ind = 1 AND rtw.absence_ind = 1 and rtw.actl_hrs_in_secnd_nbr IS NOT NULL) THEN rtw.actl_hrs_in_secnd_nbr END as absence_hrs_in_secnd,
       rtw.db_schema,
       rtw.yr_cd,
       rtw.environment
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf rtw 
  INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_mngr_cherrypick rl
        ON rtw.environment = rl.environment
        AND rtw.db_schema = rl.db_schema
        AND rtw.clnt_obj_id = rl.clnt_obj_id
        AND rtw.pers_obj_id = rl.pers_obj_id 
        AND rtw.work_asgnmt_nbr = rl.work_asgnmt_nbr        
  LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm prev
        ON rtw.environment = prev.environment
        AND rtw.db_schema = prev.db_schema
        AND rtw.clnt_obj_id = prev.clnt_obj_id
        AND rtw.pers_obj_id = prev.pers_obj_id
        AND rl.mngr_pers_obj_id = prev.mngr_pers_obj_id
        AND rtw.yr_cd = prev.yr_cd
        AND rtw.qtr_cd = prev.qtr_cd
        AND rtw.mnth_cd = prev.mnth_cd
WHERE 
    --rtw.environment = '${environment}'
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.mngr_pers_obj_id IS NOT NULL 
    AND rtw.mngr_pers_obj_id != 'UNKNOWN'
    -- to exclude cases where the manager is included as a part of the reporting hierarchy
    AND prev.mngr_pers_obj_id IS NULL;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_ezlm_mngr PARTITION(environment)
SELECT 
    /*+ COALESCE(800) */
    clnt_obj_id,
    pers_obj_id,
    mngr_pers_obj_id,
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
    --rtw.paycd_id,
    --rtw.paycd_grp_cd,
    --rtw.paycd_clsfn_cd,
    hr_orgn_id,
    payrl_orgn_id,
    pay_rt_type_cd,
    excep_hrs_in_secnd_nbr,
    excp_type,
    excp_ind,
    db_schema,
    yr_cd,
    environment
FROM
(SELECT 
       /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.mngr_pers_obj_id,
       rtw.qtr_cd,
       rtw.mnth_cd,
       rtw.clndr_wk_cd,
       rtw.full_tm_part_tm_cd,
       rtw.job_cd,
       rtw.pay_grp_cd,
       rtw.time_product,
       rtw.cmpny_cd,
       rtw.work_asgnmt_stus_cd,
       rtw.gndr_cd,
       rtw.work_state_cd,
       rtw.work_loc_cd,
       rtw.work_city_cd,
       rtw.work_cntry_cd,
       --rtw.paycd_id,
       --rtw.paycd_grp_cd,
       --rtw.paycd_clsfn_cd,
       rtw.hr_orgn_id,
       rtw.payrl_orgn_id,
       rtw.pay_rt_type_cd,
       rtw.excep_hrs_in_secnd_nbr,
       rtw.excp_type,
       rtw.excp_ind,
       rtw.environment,
       rtw.db_schema,
       rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND rtw.mngr_pers_obj_id = rl.pers_obj_id
    AND rtw.excep_dt BETWEEN rl.rec_eff_strt_dt AND rl.rec_eff_end_dt
    AND rl.mngr_pers_obj_id IS NOT NULL
UNION ALL
SELECT 
       /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.mngr_pers_obj_id,
       rtw.qtr_cd,
       rtw.mnth_cd,
       rtw.clndr_wk_cd,
       rtw.full_tm_part_tm_cd,
       rtw.job_cd,
       rtw.pay_grp_cd,
       rtw.time_product,
       rtw.cmpny_cd,
       rtw.work_asgnmt_stus_cd,
       rtw.gndr_cd,
       rtw.work_state_cd,
       rtw.work_loc_cd,
       rtw.work_city_cd,
       rtw.work_cntry_cd,
       --rtw.paycd_id,
       --rtw.paycd_grp_cd,
       --rtw.paycd_clsfn_cd,
       rtw.hr_orgn_id,
       rtw.payrl_orgn_id,
       rtw.pay_rt_type_cd,
       rtw.excep_hrs_in_secnd_nbr,
       rtw.excp_type,
       rtw.excp_ind,
       rtw.environment,
       rtw.db_schema,
       rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND rtw.pers_obj_id = rl.pers_obj_id
    AND mtrx_hrchy_ind = 1
    AND rl.mngr_pers_obj_id IS NOT NULL
)rtw
WHERE 
    --rtw.environment = '${environment}'
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.mngr_pers_obj_id IS NOT NULL 
    AND rtw.mngr_pers_obj_id != 'UNKNOWN';

-- Temporary table which will hold which persons are already captured as a part of manager hierarchy
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm 
USING PARQUET 
AS 
SELECT
    DISTINCT waf.clnt_obj_id,
    waf.pers_obj_id,
    rl.mngr_pers_obj_id,
    waf.environment,
    waf.db_schema,
    waf.yr_cd,
    waf.qtr_cd,
    waf.mnth_cd
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp waf
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
        ON waf.environment = rl.environment
        AND waf.db_schema = rl.db_schema
        AND waf.clnt_obj_id = rl.clnt_obj_id
        AND waf.mngr_pers_obj_id = rl.pers_obj_id
        AND waf.excep_dt BETWEEN rl.rec_eff_strt_dt AND rl.rec_eff_end_dt
WHERE 
    waf.mngr_pers_obj_id IS NOT NULL;

-- Cherrypick records
INSERT INTO TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_ezlm_mngr PARTITION(environment)
SELECT 
       /*+ BROADCAST(dwh_t_dim_mngr_cherrypick,tmp_emi_hrchy_mngrs_tm), COALESCE(800) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.mngr_pers_obj_id as mngr_pers_obj_id,
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
       --rtw.paycd_id,
       --rtw.paycd_grp_cd,
       --rtw.paycd_clsfn_cd,
       rtw.hr_orgn_id,
       rtw.payrl_orgn_id,
       rtw.pay_rt_type_cd,
       rtw.excep_hrs_in_secnd_nbr,
       rtw.excp_type,
       rtw.excp_ind,
       rtw.db_schema,
       rtw.yr_cd,
       rtw.environment
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp rtw 
  INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_mngr_cherrypick rl
        ON rtw.environment = rl.environment
        AND rtw.db_schema = rl.db_schema
        AND rtw.clnt_obj_id = rl.clnt_obj_id
        AND rtw.pers_obj_id = rl.pers_obj_id 
        AND rtw.work_asgnmt_nbr = rl.work_asgnmt_nbr
  LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm prev
        ON rtw.environment = prev.environment
        AND rtw.db_schema = prev.db_schema
        AND rtw.clnt_obj_id = prev.clnt_obj_id
        AND rtw.pers_obj_id = prev.pers_obj_id
        AND rl.mngr_pers_obj_id = prev.mngr_pers_obj_id
        AND rtw.yr_cd = prev.yr_cd
        AND rtw.qtr_cd = prev.qtr_cd
        AND rtw.mnth_cd = prev.mnth_cd
WHERE 
   --rtw.environment = '${environment}'
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.mngr_pers_obj_id IS NOT NULL 
    AND rtw.mngr_pers_obj_id != 'UNKNOWN'
    -- to exclude cases where the manager is included as a part of the reporting hierarchy
    AND prev.mngr_pers_obj_id IS NULL;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_ezlm_mngr PARTITION(environment)
SELECT 
    /*+ COALESCE(800) */
    clnt_obj_id,
    pers_obj_id,
    mngr_pers_obj_id,
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
    --tm_sheet_item_id_cnt_ind,
    db_schema,
    yr_cd,
    environment
FROM
(SELECT 
       /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.mngr_pers_obj_id,
       rtw.qtr_cd,
       rtw.mnth_cd,
       rtw.clndr_wk_cd,
       rtw.time_product,       
       rtw.job_cd,       
       rtw.gndr_cd,
       rtw.work_state_cd,
       rtw.work_loc_cd,
       rtw.work_city_cd,
       rtw.work_cntry_cd,       
       rtw.hr_orgn_id,       
       rtw.tm_sheet_item_id_ind,
       --rtw.tm_sheet_item_id_cnt_ind,
       rtw.environment,
       rtw.db_schema,
       rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND rtw.mngr_pers_obj_id = rl.pers_obj_id
    AND rtw.trans_dt BETWEEN rl.rec_eff_strt_dt and rl.rec_eff_end_dt
    AND rl.mngr_pers_obj_id IS NOT NULL
UNION ALL
SELECT 
       /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.mngr_pers_obj_id,
       rtw.qtr_cd,
       rtw.mnth_cd,
       rtw.clndr_wk_cd,
       rtw.time_product,       
       rtw.job_cd,       
       rtw.gndr_cd,
       rtw.work_state_cd,
       rtw.work_loc_cd,
       rtw.work_city_cd,
       rtw.work_cntry_cd,       
       rtw.hr_orgn_id,       
       rtw.tm_sheet_item_id_ind,
       --rtw.tm_sheet_item_id_cnt_ind,
       rtw.environment,
       rtw.db_schema,
       rtw.yr_cd
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits rtw 
  INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
    ON rtw.environment = rl.environment
    AND rtw.db_schema = rl.db_schema
    AND rtw.clnt_obj_id = rl.clnt_obj_id
    AND rtw.pers_obj_id = rl.pers_obj_id
    AND mtrx_hrchy_ind = 1
    AND rl.mngr_pers_obj_id IS NOT NULL
)rtw
WHERE 
    --rtw.environment = '${environment}'
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.mngr_pers_obj_id IS NOT NULL 
    AND rtw.mngr_pers_obj_id != 'UNKNOWN';

-- Temporary table which will hold which persons are already captured as a part of manager hierarchy
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm 
USING PARQUET 
AS 
SELECT
    DISTINCT waf.clnt_obj_id,
    waf.pers_obj_id,
    rl.mngr_pers_obj_id,
    waf.environment,
    waf.db_schema,
    waf.yr_cd,
    waf.qtr_cd,
    waf.mnth_cd
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits waf
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
        ON waf.environment = rl.environment
        AND waf.db_schema = rl.db_schema
        AND waf.clnt_obj_id = rl.clnt_obj_id
        AND waf.mngr_pers_obj_id = rl.pers_obj_id
        AND waf.trans_dt BETWEEN rl.rec_eff_strt_dt AND rl.rec_eff_end_dt
WHERE 
    waf.mngr_pers_obj_id IS NOT NULL;

-- Cherrypick records
INSERT INTO TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_ezlm_mngr PARTITION(environment)
SELECT 
       /*+ BROADCAST(dwh_t_dim_mngr_cherrypick,tmp_emi_hrchy_mngrs_tm), COALESCE(800) */
       rtw.clnt_obj_id,
       rtw.pers_obj_id,
       rl.mngr_pers_obj_id as mngr_pers_obj_id,
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
       --rtw.tm_sheet_item_id_cnt_ind,
       rtw.db_schema,
       rtw.yr_cd,
       rtw.environment
  FROM ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits rtw 
  INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_mngr_cherrypick rl
        ON rtw.environment = rl.environment
        AND rtw.db_schema = rl.db_schema
        AND rtw.clnt_obj_id = rl.clnt_obj_id
        AND rtw.pers_obj_id = rl.pers_obj_id 
        AND rtw.work_asgnmt_nbr = rl.work_asgnmt_nbr     
  LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm prev
        ON rtw.environment = prev.environment
        AND rtw.db_schema = prev.db_schema
        AND rtw.clnt_obj_id = prev.clnt_obj_id
        AND rtw.pers_obj_id = prev.pers_obj_id
        AND rl.mngr_pers_obj_id = prev.mngr_pers_obj_id
        AND rtw.yr_cd = prev.yr_cd
        AND rtw.qtr_cd = prev.qtr_cd
        AND rtw.mnth_cd = prev.mnth_cd
WHERE 
    --rtw.environment = '${environment}'
    rtw.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    AND rtw.time_product = 'ezlm'
    AND rtw.mngr_pers_obj_id IS NOT NULL 
    AND rtw.mngr_pers_obj_id != 'UNKNOWN'
    -- to exclude cases where the manager is included as a part of the reporting hierarchy
    AND prev.mngr_pers_obj_id IS NULL;

--DROP TABLE ${__GREEN_MAIN_DB__}.tmp_emi_hrchy_mngrs_tm;