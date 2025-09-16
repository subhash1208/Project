-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- This table is used in case of WFM people based insights
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_mssd_pnchs (
    clnt_obj_id STRING, 
    pers_obj_id STRING,
    mngr_pers_obj_id STRING,
    supvr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    yr_wk_cd STRING,
    full_tm_part_tm_cd STRING,
    job_cd STRING,
    pay_grp_cd STRING,
    cmpny_cd STRING,
    work_asgnmt_stus_cd STRING,
    work_asgnmt_nbr STRING,
    gndr_cd STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    work_city_cd STRING,
    work_cntry_cd STRING,
    --paycd_id STRING,
    --paycd_grp_cd STRING,
    labor_acct_id STRING,
    reg_temp_cd STRING,
    --paycd_clsfn_cd STRING,
    hr_orgn_id STRING,
    payrl_orgn_id STRING,
    pay_rt_type_cd STRING,
    excep_hrs_in_secnd_nbr DOUBLE,
    excp_ind INT,
    excp_type STRING,
    time_product STRING,
    clndr_wk_cd STRING,
    excep_dt TIMESTAMP,
    rslv_ind INT,
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
--    FROM ${__GREEN_MAIN_DB__}.emi_prep_tm_tf_mssd_pnchs
--    --WHERE environment = '${environment}'
--    GROUP BY environment, clnt_obj_id) IGN
--GROUP BY environment;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_mssd_pnchs PARTITION(environment)
SELECT 
       /*+ BROADCAST(dwh_t_dim_pay_grp,dwh_t_dim_job,dwh_t_dim_pers,dwh_t_dim_work_loc), COALESCE(800) */ 
       ftw.clnt_obj_id,
       ftw.pers_obj_id,
       ftw.mngr_pers_obj_id,
       ftw.supvr_pers_obj_id,
       ftw.qtr_cd,
       ftw.mnth_cd,
       NULL AS yr_wk_cd,
       ftw.full_tm_part_tm_cd,
       tj.job_cd,
       tpg.pay_grp_cd,
       tpg.cmpny_cd,
       ftw.work_asgnmt_stus_cd,
       ftw.work_asgnmt_nbr,
       pers.gndr_cd,
       worklocs.state_prov_cd as work_state_cd,
       ftw.work_loc_cd,
       worklocs.city_id as work_city_cd,
       ftw.work_cntry_cd,
       --ftw.paycd_id,
       --ftw.paycd_grp_cd,
       ftw.prmry_labor_acct_id as labor_acct_id,
       ftw.reg_temp_cd,
       --ftw.paycd_clsfn_cd,
       ftw.hr_orgn_id,
       ftw.payrl_orgn_id,
       ftw.pay_rt_type_cd,
       ftw.excep_hrs_in_secnd_nbr,
       ftw.excp_ind,
       ftw.excp_type,
       ftw.time_product,
       ftw.clndr_wk_cd,
       ftw.excep_dt,
       ftw.rslv_ind,
       ftw.db_schema,
       ftw.yr_cd,
       ftw.environment
 FROM ${__BLUE_MAIN_DB__}.emi_base_tm_tf_excp_mssd_pnch ftw
 --LEFT OUTER JOIN ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr t
 --   ON t.environment = ftw.environment 
 LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pay_grp tpg
    ON ftw.pay_grp_cd = tpg.pay_grp_cd
   AND ftw.db_schema = tpg.db_schema
   AND ftw.clnt_obj_id = tpg.clnt_obj_id
   AND ftw.environment = tpg.environment
 LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_job tj
    ON ftw.clnt_obj_id = tj.clnt_obj_id
   AND ftw.job_cd = tj.job_cd
   AND ftw.environment = tj.environment
   AND ftw.db_schema = tj.db_schema
 LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pers pers
    ON pers.clnt_obj_id = ftw.clnt_obj_id
   AND pers.pers_obj_id = ftw.pers_obj_id
   AND pers.environment = ftw.environment
   AND pers.db_schema = ftw.db_schema
  LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_loc worklocs
    ON worklocs.clnt_obj_id = ftw.clnt_obj_id
   AND worklocs.work_loc_cd = ftw.work_loc_cd
   AND worklocs.environment = ftw.environment
   AND worklocs.db_schema = ftw.db_schema
WHERE
   --ftw.environment = '${environment}'   
   ftw.yr_cd >= (YEAR(CURRENT_DATE()) - 3);

--DROP TABLE ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr;