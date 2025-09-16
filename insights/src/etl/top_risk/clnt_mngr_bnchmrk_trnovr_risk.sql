-- Databricks notebook source
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.CLNT_BNCHMRK_TRNOVR_RISK;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.CLNT_BNCHMRK_TRNOVR_RISK
(  
  CLNT_OBJ_ID          STRING,
  DB_SCHEMA            STRING,
  EMPL_CNT              INT,
  JOB_CD                STRING,
  MEAS_NM               STRING,
  MEAS_ORG_MEAN              DOUBLE,
  MEAS_NOR_MEAN             DOUBLE,
  MEAS_ORG_STDDEV       DOUBLE,
  MEAS_NOR_STDDEV       DOUBLE,
  MEAS_50th_PERCENTILE DOUBLE
  --MEAS_ORG_IQR_LOWER_OL DOUBLE,
  --MEAS_NOR_IQR_LOWER_OL DOUBLE,
  --MEAS_ORG_IQR_UPPER_OL DOUBLE,
  --MEAS_NOR_IQR_UPPER_OL DOUBLE
) 
USING PARQUET;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.CLNT_BNCHMRK_TRNOVR_RISK
SELECT
    t1.clnt_obj_id,
    t1.db_schema,
    t1.empl_cnt,
    t1.job_cd,
    t2.meas_nm,
    t2.meas_org_mean,
    t2.meas_nor_mean,
    t2.meas_org_stddev,
    t2.meas_nor_stddev,
    t2.meas_50th_percentile
    --t2.meas_org_iqr_lower_ol,
    --t2.meas_nor_iqr_lower_ol,
    --t2.meas_org_iqr_upper_ol,
    --t2.meas_nor_iqr_upper_ol
  FROM (
    SELECT
       clnt_obj_id,
       db_schema,
       empl_count AS empl_cnt,
       job_cd,
       CAST(annl_cmpn_amt_org_mean AS DOUBLE) as annl_cmpn_amt_org_mean,
       CAST(annl_cmpn_amt_nor_mean AS DOUBLE) as annl_cmpn_amt_nor_mean,
       CAST(annl_cmpn_amt_org_stddev AS DOUBLE) as annl_cmpn_amt_org_stddev,
       CAST(annl_cmpn_amt_nor_stddev AS DOUBLE) as annl_cmpn_amt_nor_stddev,
       CAST(qtrs_since_last_promotion_org_mean as DOUBLE) as qtrs_since_last_promotion_org_mean,
       CAST(qtrs_since_last_promotion_nor_mean as DOUBLE) as qtrs_since_last_promotion_nor_mean,
       CAST(qtrs_since_last_promotion_org_stddev as DOUBLE) as qtrs_since_last_promotion_org_stddev,
       CAST(qtrs_since_last_promotion_nor_stddev as DOUBLE) as qtrs_since_last_promotion_nor_stddev,
       CAST(overtime_dbltime_hours_org_mean AS DOUBLE) as overtime_dbltime_hours_org_mean,
       CAST(overtime_dbltime_hours_nor_mean AS DOUBLE) as overtime_dbltime_hours_nor_mean,
       CAST(overtime_dbltime_hours_org_stddev AS DOUBLE) as overtime_dbltime_hours_org_stddev,
       CAST(overtime_dbltime_hours_nor_stddev AS DOUBLE) as overtime_dbltime_hours_nor_stddev,
       CAST(annl_cmpn_amt_50th_percentile AS DOUBLE) as annl_cmpn_amt_50th_percentile,
       CAST(qtrs_since_last_promotion_50th_percentile AS DOUBLE) as qtrs_since_last_promotion_50th_percentile,
       CAST(overtime_dbltime_hours_50th_percentile AS DOUBLE) as overtime_dbltime_hours_50th_percentile
       --CAST(annl_cmpn_amt_org_iqr_lower_outlier AS DOUBLE) as annl_cmpn_amt_org_iqr_lower_outlier,
       --CAST(annl_cmpn_amt_nor_iqr_lower_outlier AS DOUBLE) as annl_cmpn_amt_nor_iqr_lower_outlier,
       --CAST(annl_cmpn_amt_org_iqr_upper_outlier AS DOUBLE) as annl_cmpn_amt_org_iqr_upper_outlier,
       --CAST(annl_cmpn_amt_nor_iqr_upper_outlier AS DOUBLE) as annl_cmpn_amt_nor_iqr_upper_outlier,
       --CAST(qtrs_since_last_promotion_org_iqr_lower_outlier AS DOUBLE) as qtrs_since_last_promotion_org_iqr_lower_outlier,
       --CAST(qtrs_since_last_promotion_nor_iqr_lower_outlier AS DOUBLE) as qtrs_since_last_promotion_nor_iqr_lower_outlier,
       --CAST(qtrs_since_last_promotion_org_iqr_upper_outlier AS DOUBLE) as qtrs_since_last_promotion_org_iqr_upper_outlier,
       --CAST(qtrs_since_last_promotion_nor_iqr_upper_outlier AS DOUBLE) as qtrs_since_last_promotion_nor_iqr_upper_outlier,
       --CAST(overtime_dbltime_hours_org_iqr_lower_outlier AS DOUBLE) as overtime_dbltime_hours_org_iqr_lower_outlier,
       --CAST(overtime_dbltime_hours_nor_iqr_lower_outlier AS DOUBLE) as overtime_dbltime_hours_nor_iqr_lower_outlier,
       --CAST(overtime_dbltime_hours_org_iqr_upper_outlier AS DOUBLE) as overtime_dbltime_hours_org_iqr_upper_outlier,
       --CAST(overtime_dbltime_hours_nor_iqr_upper_outlier AS DOUBLE) as overtime_dbltime_hours_nor_iqr_upper_outlier
    FROM
      ${__GREEN_MAIN_DB__}.client_trnovr_risk_cube) t1
  LATERAL VIEW stack(3,
                          'annl_cmpn_amt', annl_cmpn_amt_org_mean,annl_cmpn_amt_nor_mean,annl_cmpn_amt_org_stddev,annl_cmpn_amt_nor_stddev,annl_cmpn_amt_50th_percentile,
                          'qtrs_since_last_promotion', qtrs_since_last_promotion_org_mean,qtrs_since_last_promotion_nor_mean,qtrs_since_last_promotion_org_stddev,qtrs_since_last_promotion_nor_stddev,qtrs_since_last_promotion_50th_percentile,
                          'overtime_dbltime_hours', overtime_dbltime_hours_org_mean,overtime_dbltime_hours_nor_mean,overtime_dbltime_hours_org_stddev,overtime_dbltime_hours_nor_stddev,overtime_dbltime_hours_50th_percentile
                       ) t2 AS meas_nm, meas_org_mean,meas_nor_mean,meas_org_stddev,meas_nor_stddev,meas_50th_percentile;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.MNGR_BNCHMRK_TRNOVR_RISK;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.MNGR_BNCHMRK_TRNOVR_RISK
( 
  CLNT_OBJ_ID           STRING,
  MNGR_PERS_OBJ_ID     STRING, 
  DB_SCHEMA         STRING, 
  EMPL_CNT              INT,
  JOB_CD                STRING,
  MEAS_NM               STRING,
  MEAS_ORG_MEAN              DOUBLE,
  MEAS_NOR_MEAN              DOUBLE,
  MEAS_ORG_STDDEV       DOUBLE,
  MEAS_NOR_STDDEV       DOUBLE,
  MEAS_50TH_PERCENTILE DOUBLE
  --MEAS_NOR_IQR_LOWER_OL DOUBLE,
  --MEAS_ORG_IQR_UPPER_OL DOUBLE,
  --MEAS_NOR_IQR_UPPER_OL DOUBLE
) 
USING PARQUET;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.MNGR_BNCHMRK_TRNOVR_RISK
SELECT
    t1.clnt_obj_id,
    t1.mngr_pers_obj_id,
    t1.db_schema,
    t1.empl_cnt,
    t1.job_cd,
    t2.meas_nm,
    t2.meas_org_mean,
    t2.meas_nor_mean,
    t2.meas_org_stddev,
    t2.meas_nor_stddev,
    t2.meas_50th_percentile
    --t2.meas_org_iqr_lower_ol,
    --t2.meas_nor_iqr_lower_ol,
    --t2.meas_org_iqr_upper_ol,
    --t2.meas_nor_iqr_upper_ol
  FROM (
    SELECT
       clnt_obj_id,
       mngr_pers_obj_id,
       db_schema,
       empl_count AS empl_cnt,
       job_cd,
       CAST(annl_cmpn_amt_org_mean AS DOUBLE) as annl_cmpn_amt_org_mean,
       CAST(annl_cmpn_amt_nor_mean AS DOUBLE) as annl_cmpn_amt_nor_mean,
       CAST(annl_cmpn_amt_org_stddev AS DOUBLE) as annl_cmpn_amt_org_stddev,
       CAST(annl_cmpn_amt_nor_stddev AS DOUBLE) as annl_cmpn_amt_nor_stddev,
       CAST(qtrs_since_last_promotion_org_mean as DOUBLE) as qtrs_since_last_promotion_org_mean,
       CAST(qtrs_since_last_promotion_nor_mean as DOUBLE) as qtrs_since_last_promotion_nor_mean,
       CAST(qtrs_since_last_promotion_org_stddev as DOUBLE) as qtrs_since_last_promotion_org_stddev,
       CAST(qtrs_since_last_promotion_nor_stddev as DOUBLE) as qtrs_since_last_promotion_nor_stddev,
       CAST(overtime_dbltime_hours_org_mean AS DOUBLE) as overtime_dbltime_hours_org_mean,
       CAST(overtime_dbltime_hours_nor_mean AS DOUBLE) as overtime_dbltime_hours_nor_mean,
       CAST(overtime_dbltime_hours_org_stddev AS DOUBLE) as overtime_dbltime_hours_org_stddev,
       CAST(overtime_dbltime_hours_nor_stddev AS DOUBLE) as overtime_dbltime_hours_nor_stddev,
       CAST(annl_cmpn_amt_50th_percentile AS DOUBLE) as annl_cmpn_amt_50th_percentile,
       CAST(qtrs_since_last_promotion_50th_percentile AS DOUBLE) as qtrs_since_last_promotion_50th_percentile,
       CAST(overtime_dbltime_hours_50th_percentile AS DOUBLE) as overtime_dbltime_hours_50th_percentile
       --CAST(annl_cmpn_amt_org_iqr_lower_outlier AS DOUBLE) as annl_cmpn_amt_org_iqr_lower_outlier,
       --CAST(annl_cmpn_amt_nor_iqr_lower_outlier AS DOUBLE) as annl_cmpn_amt_nor_iqr_lower_outlier,
       --CAST(annl_cmpn_amt_org_iqr_upper_outlier AS DOUBLE) as annl_cmpn_amt_org_iqr_upper_outlier,
       --CAST(annl_cmpn_amt_nor_iqr_upper_outlier AS DOUBLE) as annl_cmpn_amt_nor_iqr_upper_outlier,
       --CAST(qtrs_since_last_promotion_org_iqr_lower_outlier AS DOUBLE) as qtrs_since_last_promotion_org_iqr_lower_outlier,
       --CAST(qtrs_since_last_promotion_nor_iqr_lower_outlier AS DOUBLE) as qtrs_since_last_promotion_nor_iqr_lower_outlier,
       --CAST(qtrs_since_last_promotion_org_iqr_upper_outlier AS DOUBLE) as qtrs_since_last_promotion_org_iqr_upper_outlier,
       --CAST(qtrs_since_last_promotion_nor_iqr_upper_outlier AS DOUBLE) as qtrs_since_last_promotion_nor_iqr_upper_outlier,
       --CAST(overtime_dbltime_hours_org_iqr_lower_outlier AS DOUBLE) as overtime_dbltime_hours_org_iqr_lower_outlier,
       --CAST(overtime_dbltime_hours_nor_iqr_lower_outlier AS DOUBLE) as overtime_dbltime_hours_nor_iqr_lower_outlier,
       --CAST(overtime_dbltime_hours_org_iqr_upper_outlier AS DOUBLE) as overtime_dbltime_hours_org_iqr_upper_outlier,
       --CAST(overtime_dbltime_hours_nor_iqr_upper_outlier AS DOUBLE) as overtime_dbltime_hours_nor_iqr_upper_outlier
    FROM
      ${__GREEN_MAIN_DB__}.mngr_trnovr_risk_cube) t1
  LATERAL VIEW stack(3,
                          'annl_cmpn_amt', annl_cmpn_amt_org_mean,annl_cmpn_amt_nor_mean,annl_cmpn_amt_org_stddev,annl_cmpn_amt_nor_stddev,annl_cmpn_amt_50th_percentile,
                          'qtrs_since_last_promotion', qtrs_since_last_promotion_org_mean,qtrs_since_last_promotion_nor_mean,qtrs_since_last_promotion_org_stddev,qtrs_since_last_promotion_nor_stddev,qtrs_since_last_promotion_50th_percentile,
                          'overtime_dbltime_hours', overtime_dbltime_hours_org_mean,overtime_dbltime_hours_nor_mean,overtime_dbltime_hours_org_stddev,overtime_dbltime_hours_nor_stddev,overtime_dbltime_hours_50th_percentile
                       ) t2 AS meas_nm, meas_org_mean,meas_nor_mean,meas_org_stddev,meas_nor_stddev,meas_50th_percentile;