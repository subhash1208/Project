-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode=2000;

-- Aggregated table to be used for insight generation instead of cube.This is applicable only for etime.
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_etime_mngr_agg (
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
    edits_count INT,
    --tm_sheet_item_id_cnt_ind INT,
    num_employees INT,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');
 
--For manager edits we will not apply any labour account or paycode securities.
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_etime_mngr_agg PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
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
    SUM(tm_sheet_item_id_ind) as edits_count,
    --tm_sheet_item_id_cnt_ind,
    count(distinct pers_obj_id) as num_employees,
    yr_cd,
    environment
FROM
(
SELECT
  clnt_obj_id,
 --pers_obj_id,
  mngr_pers_obj_id,
  qtr_cd,
  mnth_cd,
  clndr_wk_cd,
  COALESCE(job_cd,'UNKNOWN') as job_cd,
  COALESCE(hr_orgn_id,'UNKNOWN') as hr_orgn_id,
  --gndr_cd,
  COALESCE(work_state_cd,'UNKNOWN') as work_state_cd,
  COALESCE(work_loc_cd,'UNKNOWN') as work_loc_cd,
  COALESCE(work_cntry_cd,'UNKNOWN') as work_cntry_cd,
  tm_sheet_item_id_ind,
  --tm_sheet_item_id_cnt_ind,
  pers_obj_id,
  environment,
  yr_cd
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits
    WHERE
    --environment = '${environment}'
    time_product = 'etime'
    AND yr_cd >= cast(year(add_months(current_date, -24)) as string) )tmp
GROUP BY environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd,job_cd,hr_orgn_id,work_cntry_cd,work_state_cd,work_loc_cd
        GROUPING SETS ( (environment,clnt_obj_id,mngr_pers_obj_id,yr_cd),(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,job_cd),(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,hr_orgn_id) ,(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,clndr_wk_cd),
        (environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,work_cntry_cd,work_state_cd),(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,clnt_obj_id, mngr_pers_obj_id, yr_cd,qtr_cd),(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,job_cd),(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,hr_orgn_id) ,(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,clndr_wk_cd),
        (environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd),(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,work_cntry_cd,work_state_cd,work_loc_cd),
        (environment,clnt_obj_id, mngr_pers_obj_id,yr_cd,qtr_cd,mnth_cd),(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,mnth_cd,job_cd),(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,mnth_cd,hr_orgn_id) ,(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,mnth_cd,clndr_wk_cd),
        (environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd),(environment,clnt_obj_id,mngr_pers_obj_id,yr_cd,qtr_cd,mnth_cd,work_cntry_cd,work_state_cd,work_loc_cd)
       );