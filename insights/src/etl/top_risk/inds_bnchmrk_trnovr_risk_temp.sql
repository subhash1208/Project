-- Databricks notebook source
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.INDS_BNCHMRK_TRNOVR_RISK_TEMP;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.INDS_BNCHMRK_TRNOVR_RISK_TEMP
(  
  RPT_DT  STRING,
  STD_JOB_TITL_KY STRING,
  CLNT_CNT              INT,
  EMPL_CNT              INT,
  MEAS_NM               STRING,
  MEAS_ORG_MEAN              DOUBLE,
  MEAS_NOR_MEAN              DOUBLE,
  MEAS_ORG_STDDEV       DOUBLE,
  MEAS_NOR_STDDEV       DOUBLE,
  MEAS_50TH_PERCENTILE DOUBLE,
  --MEAS_NOR_IQR_LOWER_OL DOUBLE,
  --MEAS_ORG_IQR_UPPER_OL DOUBLE,
  --MEAS_NOR_IQR_UPPER_OL DOUBLE,
  REC_CRT_TS            STRING,
  SUPERSECTOR STRING,
  SECTOR                STRING,
  QTR                    STRING
) 
USING PARQUET
PARTITIONED BY (QTR);


INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.INDS_BNCHMRK_TRNOVR_RISK_TEMP PARTITION(qtr='GA_${qtr}')
SELECT DISTINCT
  concat(CAST(YEAR(CURRENT_DATE) AS string),'-', 
  CASE WHEN MONTH(CURRENT_DATE) > 9 THEN CAST(MONTH(CURRENT_DATE) AS string) 
  ELSE concat('0', CAST(MONTH(CURRENT_DATE) AS string)) END, 
  '-01 00:00:00.000') AS rpt_dt, 
  --'-1' AS inds_ky,
  adp_lens_cd AS std_job_titl_ky, 
  clnt_cnt, 
  empl_cnt, 
  meas_nm , 
  meas_org_mean,
  meas_nor_mean,
  meas_org_stddev,
  meas_nor_stddev,
  meas_50th_percentile,
  --meas_nor_iqr_lower_ol,
  --meas_org_iqr_upper_ol,
  --meas_nor_iqr_upper_ol,
  CURRENT_TIMESTAMP AS rec_crt_ts, 
  --reg_temp_cd, full_tm_part_tm_cd,
  supersector_cd,
  sector_cd
FROM
(
  SELECT DISTINCT 
    --t1.reg_temp_cd, t1.full_tm_part_tm_cd,
    t1.adp_lens_cd, 
    t1.supersector_cd,
    t1.sector_cd,
    t1.clnt_cnt, t1.empl_cnt,
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
       clnt_count AS clnt_cnt, empl_count AS empl_cnt,
       --nvl(reg_temp_cd, '-1') AS reg_temp_cd, nvl(full_tm_part_tm_cd, '-1') AS full_tm_part_tm_cd,
       --nvl(adp_lens_cd,'-1') AS adp_lens_cd,
       adp_lens_cd,
       nvl(supersector_cd,'-1') AS supersector_cd,
       nvl(sector_cd,'-1') AS sector_cd,
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
       CAST(overtime_dbltime_hours_50th_percentile AS DOUBLE) as overtime_dbltime_hours_50th_percentile,
       CAST(annl_cmpn_amt_50th_percentile AS DOUBLE) as annl_cmpn_amt_50th_percentile,
       CAST(qtrs_since_last_promotion_50th_percentile AS DOUBLE) as qtrs_since_last_promotion_50th_percentile
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
      ${__GREEN_MAIN_DB__}.inds_turnover_risk_cube
    WHERE 
      qtr = '${qtr}' AND 
      --sector_cd IS NOT NULL AND
      --subsector_cd IS NULL AND
      clnt_count >= 5 AND empl_count >= 20) t1
  LATERAL VIEW stack(3,
                          'annl_cmpn_amt', annl_cmpn_amt_org_mean,annl_cmpn_amt_nor_mean,annl_cmpn_amt_org_stddev,annl_cmpn_amt_nor_stddev,annl_cmpn_amt_50th_percentile,
                          'qtrs_since_last_promotion', qtrs_since_last_promotion_org_mean,qtrs_since_last_promotion_nor_mean,qtrs_since_last_promotion_org_stddev,qtrs_since_last_promotion_nor_stddev,qtrs_since_last_promotion_50th_percentile,
                          'overtime_dbltime_hours', overtime_dbltime_hours_org_mean,overtime_dbltime_hours_nor_mean,overtime_dbltime_hours_org_stddev,overtime_dbltime_hours_nor_stddev,overtime_dbltime_hours_50th_percentile
                       ) t2 AS meas_nm, meas_org_mean,meas_nor_mean,meas_org_stddev,meas_nor_stddev,meas_50th_percentile
)MAIN;