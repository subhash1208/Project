-- Databricks notebook source
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.TOP_RISK_MNGR_HIERARCHY_INPUT;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.TOP_RISK_MNGR_HIERARCHY_INPUT
(
  CLNT_OBJ_ID STRING,
  PERS_OBJ_ID STRING,
  QTR_CD STRING,
  QTR_SEQ_NBR DOUBLE,
  QUARTER STRING,
  MNGR_PERS_OBJ_ID STRING,
  ANNL_CMPN_AMT DOUBLE,
  LOG_ANNL_CMPN_AMT DOUBLE,
  COMPA_RT DOUBLE,
  IS_MANAGER STRING,
  FULL_TM_PART_TM_DSC STRING,
  REG_TEMP_DSC STRING,
  JOB_CD STRING,
  FLSA_STUS_DSC STRING,
  WORK_CITY STRING,
  WORK_STATE_CD STRING,
  QTRS_SINCE_LAST_PROMOTION DOUBLE,
  LOG_QTRS_SINCE_LAST_PROMOTION DOUBLE,
  OVERTIME_DBLTIME_HOURS DOUBLE,
  LOG_OVERTIME_DBLTIME_HOURS DOUBLE,
  OVERTIME_DBLTIME_EARNINGS DOUBLE,
  SALARY_HIKE DOUBLE,
  TENURE_IN_JOB_MONTHS DOUBLE,
  TRAVEL_DISTANCE DOUBLE,
  TRAVEL_DURATION DOUBLE,
  TOTAL_REPORTS DOUBLE,
  SOURCE_SYSTEM STRING,
  FWA_REC_EFF_STRT_DT TIMESTAMP,
  FWA_REC_EFF_END_DT TIMESTAMP,
  PRMRY_STD_JOB_TITL_KY STRING,
  SECTOR_CD STRING,
  DB_SCHEMA STRING
)
USING PARQUET;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.TOP_RISK_MNGR_HIERARCHY_INPUT
SELECT
 TDE.CLNT_OBJ_ID,
 TDE.PERS_OBJ_ID,
 QTR_CD,
 QTR_SEQ_NBR,
 QUARTER,
 MNGR_PERS_OBJ_ID,
 ANNL_CMPN_AMT,
 LOG_ANNL_CMPN_AMT,
 COMPA_RT,
 IS_MANAGER,
 FULL_TM_PART_TM_DSC,
 REG_TEMP_DSC,
 TDE.JOB_CD ,
 FLSA_STUS_DSC ,
 WORK_CITY ,
 WORK_STATE_CD ,
 QTRS_SINCE_LAST_PROMOTION ,
 LOG_QTRS_SINCE_LAST_PROMOTION,
 OVERTIME_DBLTIME_HOURS ,
 LOG_OVERTIME_DBLTIME_HOURS,
 OVERTIME_DBLTIME_EARNINGS ,
 SALARY_HIKE ,
 TENURE_IN_JOB_MONTHS ,
 TRAVEL_DISTANCE ,
 TRAVEL_DURATION ,
 TOTAL_REPORTS ,
 SOURCE_SYSTEM ,
 FWA_REC_EFF_STRT_DT ,
 FWA_REC_EFF_END_DT ,
 J.prmry_std_job_titl_ky as PRMRY_STD_JOB_TITL_KY,
 cm.sector_cd,
 TDE.DB_SCHEMA
FROM
(SELECT
 TDE.CLNT_OBJ_ID,
 TDE.PERS_OBJ_ID,
 TDE.QTR_CD,
 QTR_SEQ_NBR,
 QUARTER,
 MNGR_PERS_OBJ_ID,
 CASE WHEN (TDE.annl_cmpn_amt IS NULL OR TDE.annl_cmpn_amt = 0) THEN MAIN.annl_cmpn_amt ELSE TDE.annl_cmpn_amt END AS annl_cmpn_amt,
 CASE WHEN (TDE.annl_cmpn_amt IS NULL OR TDE.annl_cmpn_amt = 0) THEN LN(MAIN.annl_cmpn_amt) ELSE LN(TDE.annl_cmpn_amt) END AS log_annl_cmpn_amt,
 CASE WHEN TDE.compa_rt = 0 THEN NULL ELSE TDE.compa_rt END AS compa_rt,
 IS_MANAGER,
 CASE WHEN (TDE.full_tm_part_tm_dsc IS NULL OR upper(TDE.full_tm_part_tm_dsc) = 'UNKNOWN') THEN MAIN.full_tm_part_tm_dsc ELSE TDE.full_tm_part_tm_dsc END AS full_tm_part_tm_dsc,
 REG_TEMP_DSC,
 COALESCE(TDE.job_cd,MAIN.job_cd) AS job_cd,
 FLSA_STUS_DSC ,
 CASE WHEN (TDE.work_city IS NULL OR upper(TDE.work_city) = 'UNKNOWN') THEN MAIN.work_city ELSE TDE.work_city END AS work_city,
 CASE WHEN (TDE.work_state_cd IS NULL OR upper(TDE.work_state_cd) = 'UNKNOWN') THEN COALESCE(MAIN.work_state_cd,'UNKNOWN') ELSE TDE.work_state_cd END AS work_state_cd,
 CASE WHEN (TDE.qtrs_since_last_promotion IS NULL OR TDE.qtrs_since_last_promotion = 0) THEN MAIN.qtrs_since_last_promotion
      ELSE TDE.qtrs_since_last_promotion
 END AS qtrs_since_last_promotion,
 CASE WHEN (TDE.qtrs_since_last_promotion IS NULL OR TDE.qtrs_since_last_promotion = 0) THEN LN(MAIN.qtrs_since_last_promotion)
      ELSE LN(TDE.qtrs_since_last_promotion)
 END AS log_qtrs_since_last_promotion,
 COALESCE(TDE.overtime_dbltime_hours, MAIN.overtime_dbltime_hours) AS overtime_dbltime_hours,
 LN(COALESCE(TDE.overtime_dbltime_hours, MAIN.overtime_dbltime_hours)) AS log_overtime_dbltime_hours,
 COALESCE(TDE.overtime_dbltime_earnings, MAIN.overtime_dbltime_earnings) AS overtime_dbltime_earnings,
 SALARY_HIKE ,
 TENURE_IN_JOB_MONTHS ,
 TRAVEL_DISTANCE ,
 TRAVEL_DURATION ,
 TOTAL_REPORTS ,
 TDE.SOURCE_SYSTEM ,
 FWA_REC_EFF_STRT_DT ,
 FWA_REC_EFF_END_DT ,
 --J.prmry_std_job_titl_ky as PRMRY_STD_JOB_TITL_KY,
 --cm.sector_cd,
 TDE.DB_SCHEMA,
 row_number() OVER (PARTITION BY TDE.clnt_obj_id, TDE.pers_obj_id ORDER BY TDE.fwa_rec_eff_strt_dt DESC) AS ranking
FROM ${__BLUE_MAIN_DB__}.TOP_DW_EXTRACT_TRH TDE
LEFT OUTER JOIN (
    SELECT 
     clnt_obj_id,
     pers_obj_id,
     source_system,
     full_tm_part_tm_dsc,
     l2_code,
     l4_code,
     l3_code,
     job_cd,
     work_city,
     work_state_cd,
     annl_cmpn_amt,
     qtrs_since_last_promotion,
     overtime_dbltime_hours,
     overtime_dbltime_earnings
     FROM ${__BLUE_MAIN_DB__}.all_top_data_extract A
     INNER JOIN (SELECT MAX(QTR_CD) as QTR_CD FROM  ${__BLUE_MAIN_DB__}.all_top_data_extract)B
     ON A.qtr_cd = B.qtr_cd
) MAIN
ON MAIN.clnt_obj_id = TDE.clnt_obj_id 
AND MAIN.pers_obj_id = TDE.pers_obj_id
AND upper(MAIN.source_system) = upper(TDE.source_system)
AND substr(MAIN.l2_code,7) = TDE.l2_code
AND MAIN.l3_code = substr(MAIN.l4_code,1,9)
WHERE TDE.IS_TERMINATED=0)TDE
LEFT OUTER JOIN (
SELECT 
  DISTINCT 
  clnt_obj_id,
  db_schema,
  job_cd,
  prmry_std_job_titl_ky
FROM 
 ${__RO_BLUE_RAW_DB__}.dwh_t_clnt_job_titl_mapping 
 where ingest_date in (select max(ingest_date) from ${__RO_BLUE_RAW_DB__}.dwh_t_clnt_job_titl_mapping)) J
ON J.CLNT_OBJ_ID = TDE.CLNT_OBJ_ID
AND J.JOB_CD  =TDE.JOB_CD
AND J.db_Schema = TDE.db_schema
--LEFT OUTER JOIN (
--    SELECT DISTINCT CLNT_OBJ_ID,get_json_object(clnt_prfl_dtl, '$.primaryIndustry.sectorCode') AS sector_cd
--    FROM ${__RO_GREEN_BASE_DB__}.dwh_t_clnt_prfl_setting
--) cm
--on TDE.clnt_obj_id = cm.clnt_obj_id
LEFT OUTER JOIN (
    SELECT ooid,sector_cd FROM
    (SELECT  ooid,sector_cd,ROW_NUMBER() OVER(PARTITION BY ooid ORDER BY control_headcount DESC) as rnk
    FROM ${__RO_BLUE_LANDING_BASE_DB__}.client_master) WHERE rnk=1
) cm
on TDE.clnt_obj_id = cm.ooid
WHERE ranking=1;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.TOP_RISK_MNGR_HIERARCHY;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.TOP_RISK_MNGR_HIERARCHY
(
  CLNT_OBJ_ID STRING,
  PERS_OBJ_ID STRING,
  QTR_CD STRING,
  QTR_SEQ_NBR DOUBLE,
  QUARTER STRING,
  MNGR_PERS_OBJ_ID STRING,
  ANNL_CMPN_AMT DOUBLE,
  LOG_ANNL_CMPN_AMT DOUBLE,
  COMPA_RT DOUBLE,
  IS_MANAGER STRING,
  FULL_TM_PART_TM_DSC STRING,
  REG_TEMP_DSC STRING,
  JOB_CD STRING,
  FLSA_STUS_DSC STRING,
  WORK_CITY STRING,
  WORK_STATE_CD STRING,
  QTRS_SINCE_LAST_PROMOTION DOUBLE,
  LOG_QTRS_SINCE_LAST_PROMOTION DOUBLE,
  OVERTIME_DBLTIME_HOURS DOUBLE,
  LOG_OVERTIME_DBLTIME_HOURS DOUBLE,
  OVERTIME_DBLTIME_EARNINGS DOUBLE,
  SALARY_HIKE DOUBLE,
  TENURE_IN_JOB_MONTHS DOUBLE,
  TRAVEL_DISTANCE DOUBLE,
  TRAVEL_DURATION DOUBLE,
  TOTAL_REPORTS DOUBLE,
  SOURCE_SYSTEM STRING,
  FWA_REC_EFF_STRT_DT TIMESTAMP,
  FWA_REC_EFF_END_DT TIMESTAMP,
  PRMRY_STD_JOB_TITL_KY STRING,
  SECTOR_CD STRING,
  DB_SCHEMA STRING
)
USING PARQUET;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.TOP_RISK_MNGR_HIERARCHY
SELECT
 CLNT_OBJ_ID,
 PERS_OBJ_ID,
 QTR_CD,
 QTR_SEQ_NBR,
 QUARTER,
 MNGR_PERS_OBJ_ID,
 ANNL_CMPN_AMT,
 LOG_ANNL_CMPN_AMT,
 COMPA_RT,
 IS_MANAGER,
 FULL_TM_PART_TM_DSC,
 REG_TEMP_DSC,
 JOB_CD ,
 FLSA_STUS_DSC ,
 WORK_CITY ,
 WORK_STATE_CD ,
 QTRS_SINCE_LAST_PROMOTION ,
 LOG_QTRS_SINCE_LAST_PROMOTION,
 OVERTIME_DBLTIME_HOURS ,
 LOG_OVERTIME_DBLTIME_HOURS,
 OVERTIME_DBLTIME_EARNINGS ,
 SALARY_HIKE ,
 TENURE_IN_JOB_MONTHS ,
 TRAVEL_DISTANCE ,
 TRAVEL_DURATION ,
 TOTAL_REPORTS ,
 SOURCE_SYSTEM ,
 FWA_REC_EFF_STRT_DT ,
 FWA_REC_EFF_END_DT ,
 PRMRY_STD_JOB_TITL_KY,
 sector_cd,
 DB_SCHEMA
FROM
(SELECT
 r.CLNT_OBJ_ID,
 r.PERS_OBJ_ID,
 QTR_CD,
 QTR_SEQ_NBR,
 QUARTER,
 rl.MNGR_PERS_OBJ_ID AS MNGR_PERS_OBJ_ID,
 ANNL_CMPN_AMT,
 LOG_ANNL_CMPN_AMT,
 COMPA_RT,
 IS_MANAGER,
 FULL_TM_PART_TM_DSC,
 REG_TEMP_DSC,
 JOB_CD ,
 FLSA_STUS_DSC ,
 WORK_CITY ,
 WORK_STATE_CD ,
 QTRS_SINCE_LAST_PROMOTION ,
 LOG_QTRS_SINCE_LAST_PROMOTION,
 OVERTIME_DBLTIME_HOURS ,
 LOG_OVERTIME_DBLTIME_HOURS,
 OVERTIME_DBLTIME_EARNINGS ,
 SALARY_HIKE ,
 TENURE_IN_JOB_MONTHS ,
 TRAVEL_DISTANCE ,
 TRAVEL_DURATION ,
 TOTAL_REPORTS ,
 SOURCE_SYSTEM ,
 --L2_CODE ,
 FWA_REC_EFF_STRT_DT ,
 FWA_REC_EFF_END_DT ,
 PRMRY_STD_JOB_TITL_KY,
 sector_cd,
 r.DB_SCHEMA
FROM ${__BLUE_MAIN_DB__}.TOP_RISK_MNGR_HIERARCHY_INPUT r
INNER JOIN (
    SELECT
    ext.clnt_obj_id,
    ext.mngr_pers_obj_id AS pers_obj_id,
    ext.login_mngr_pers_obj_id AS mngr_pers_obj_id,
    lvl_from_prnt_nbr,
    ext.db_schema,
    ext.mtrx_hrchy_ind,
    ext.rec_eff_strt_dt,
    ext.rec_eff_end_dt,
    ext.environment
FROM
    ${__RO_BLUE_RAW_DB__}.dwh_t_mngr_hrchy_sec ext
    LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pers pers
    ON ext.clnt_obj_id = pers.clnt_obj_id
    AND ext.db_Schema = pers.db_Schema
    AND ext.environment = pers.environment
    AND ext.login_mngr_pers_obj_id = pers.pers_obj_id
WHERE
    pers.dir_and_indir_rpt_accs_ind = 1 OR ext.lvl_from_prnt_nbr = 0
    --AND ext.clnt_live_ind = 'Y'
) rl
  ON 
  --r.environment = rl.environment
  r.db_schema = rl.db_schema
  AND r.clnt_obj_id = rl.clnt_obj_id
  AND r.mngr_pers_obj_id = rl.pers_obj_id 
  AND rl.mngr_pers_obj_id IS NOT NULL
WHERE r.fwa_rec_eff_strt_dt >= rl.rec_eff_strt_dt AND r.fwa_rec_eff_end_dt <= rl.rec_eff_end_dt

UNION ALL

SELECT
 r.CLNT_OBJ_ID,
 r.PERS_OBJ_ID,
 QTR_CD,
 QTR_SEQ_NBR,
 QUARTER,
 rl.MNGR_PERS_OBJ_ID AS MNGR_PERS_OBJ_ID,
 ANNL_CMPN_AMT,
 LOG_ANNL_CMPN_AMT,
 COMPA_RT,
 IS_MANAGER,
 FULL_TM_PART_TM_DSC,
 REG_TEMP_DSC,
 JOB_CD ,
 FLSA_STUS_DSC ,
 WORK_CITY ,
 WORK_STATE_CD ,
 QTRS_SINCE_LAST_PROMOTION ,
 LOG_QTRS_SINCE_LAST_PROMOTION,
 OVERTIME_DBLTIME_HOURS ,
 LOG_OVERTIME_DBLTIME_HOURS,
 OVERTIME_DBLTIME_EARNINGS ,
 SALARY_HIKE ,
 TENURE_IN_JOB_MONTHS ,
 TRAVEL_DISTANCE ,
 TRAVEL_DURATION ,
 TOTAL_REPORTS ,
 SOURCE_SYSTEM ,
 --L2_CODE ,
 FWA_REC_EFF_STRT_DT ,
 FWA_REC_EFF_END_DT ,
 PRMRY_STD_JOB_TITL_KY,
 sector_cd,
 r.DB_SCHEMA
FROM ${__BLUE_MAIN_DB__}.TOP_RISK_MNGR_HIERARCHY_INPUT r
INNER JOIN (
    SELECT
    ext.clnt_obj_id,
    ext.mngr_pers_obj_id AS pers_obj_id,
    ext.login_mngr_pers_obj_id AS mngr_pers_obj_id,
    lvl_from_prnt_nbr,
    ext.db_schema,
    ext.mtrx_hrchy_ind,
    ext.rec_eff_strt_dt,
    ext.rec_eff_end_dt,
    ext.environment
FROM
    ${__RO_BLUE_RAW_DB__}.dwh_t_mngr_hrchy_sec ext
    LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pers pers
    ON ext.clnt_obj_id = pers.clnt_obj_id
    AND ext.db_Schema = pers.db_Schema
    AND ext.environment = pers.environment
    AND ext.login_mngr_pers_obj_id = pers.pers_obj_id
WHERE
    pers.dir_and_indir_rpt_accs_ind = 1 OR ext.lvl_from_prnt_nbr = 0
    --AND ext.clnt_live_ind = 'Y'
)rl
ON
--r.environment = rl.environment
r.db_schema = rl.db_schema
AND r.clnt_obj_id = rl.clnt_obj_id
AND r.pers_obj_id = rl.pers_obj_id 
AND mtrx_hrchy_ind = 1
AND rl.mngr_pers_obj_id IS NOT NULL)final;