-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- This table is used as-is for executive time insights 
-- (i.e...) when no time security is applied.
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits (
    clnt_obj_id STRING, 
    pers_obj_id STRING,
    mngr_pers_obj_id STRING,
    supvr_pers_obj_id STRING,  
    qtr_cd STRING,
    mnth_cd STRING,
    yr_wk_cd STRING,
    job_cd STRING,    
    gndr_cd STRING,
    work_state_cd STRING,
    work_loc_cd STRING,
    work_city_cd STRING,
    work_cntry_cd STRING,    
    reg_temp_cd STRING,    
    hr_orgn_id STRING,
    paycd_id STRING,
    labor_acct_id STRING, 
    tm_sheet_item_id_ind INT,
    --tm_sheet_item_id_cnt_ind INT,
    time_product STRING,
    clndr_wk_cd STRING,
    work_asgnmt_nbr STRING,
    trans_dt TIMESTAMP,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

--DROP TABLE IF EXISTS ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_tm_edits;
--
--CREATE TABLE ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_tm_edits
--USING PARQUET
--AS
--SELECT environment, min(yr_cd) as last_load_clndr_yr_cd
--FROM 
--    (SELECT environment, clnt_obj_id, max(yr_cd) as yr_cd 
--    FROM ${__GREEN_MAIN_DB__}.emi_prep_tm_tf_edits 
--    --WHERE environment = '${environment}'
--    GROUP BY environment, clnt_obj_id) IGN
--GROUP BY environment;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits PARTITION(environment)
SELECT 
       /*+ BROADCAST(dwh_t_dim_pay_grp,dwh_t_dim_job,dwh_t_dim_pers,dwh_t_dim_work_loc), COALESCE(800) */
       ftw.clnt_obj_id,
       ftw.pers_obj_id,
       ftw.mngr_pers_obj_id,
       ftw.supvr_pers_obj_id,
       ftw.qtr_cd,
       ftw.mnth_cd,
       NULL AS yr_wk_cd,
       tj.job_cd,
       pers.gndr_cd,
       worklocs.state_prov_cd as work_state_cd,
       ftw.work_loc_cd,
       worklocs.city_id as work_city_cd,
       worklocs.iso_3_char_cntry_cd as work_cntry_cd,
       ftw.reg_temp_cd,
       ftw.hr_orgn_id, 
       ftw.paycd_id,
       ftw.labor_acct_id,
       tm_sheet_item_id_ind,
       --tm_sheet_item_id_cnt_ind,
       ftw.time_product,
       ftw.clndr_wk_cd,
       ftw.work_asgnmt_nbr,
       ftw.trans_dt,
       ftw.db_schema,
       ftw.yr_cd,
       ftw.environment
 FROM ${__BLUE_MAIN_DB__}.emi_base_tm_tf_edits ftw
 --LEFT OUTER JOIN ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_tm_edits t
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
   --ftw.environment='${environment}'
   ftw.yr_cd >= (YEAR(CURRENT_DATE()) - 3);