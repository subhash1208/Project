-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.top_risk_mngr_diff_means;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.top_risk_mngr_diff_means(
    CLNT_OBJ_ID STRING,
    db_schema STRING,
    MNGR_PERS_OBJ_ID STRING,
    JOB_CD STRING,
    FCTR_NM STRING,
    MNGR_DIFF_MEAN DOUBLE
)USING PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.top_risk_mngr_diff_means
SELECT
  CLNT_OBJ_ID,
  db_schema,
  mngr_pers_obj_id,
  job_cd,
  fctr_nm,
  AVG(fctr_val_diff) as mngr_diff_mean
FROM
(
SELECT
  CLNT_OBJ_ID,
  db_schema,
  mngr_pers_obj_id,
  job_cd,
  fctr_nm,
  CASE WHEN (fctr_nm='annl_cmpn_amt') THEN (MNGR_ORG_MEAN - fctr_val)
  WHEN (fctr_nm='qtrs_since_last_promotion' OR fctr_nm = 'overtime_dbltime_hours') THEN (fctr_val - MNGR_ORG_MEAN)
  ELSE 0 END as fctr_val_diff
FROM ${__BLUE_MAIN_DB__}.top_risk_insights
WHERE CASE WHEN(fctr_nm='annl_cmpn_amt') THEN fctr_val < MNGR_ORG_MEAN
ELSE fctr_val > MNGR_ORG_MEAN END
)
GROUP BY clnt_obj_id,mngr_pers_obj_id,db_schema,fctr_nm,job_cd;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.top_risk_clnt_diff_means;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.top_risk_clnt_diff_means(
    CLNT_OBJ_ID STRING,
    db_schema STRING,
    JOB_CD STRING,
    FCTR_NM STRING,
    CLNT_DIFF_MEAN DOUBLE
)USING PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.top_risk_clnt_diff_means
SELECT
  CLNT_OBJ_ID,
  db_schema,
  job_cd,
  fctr_nm,
  AVG(fctr_val_diff) as clnt_diff_mean
FROM
(
SELECT
  CLNT_OBJ_ID,
  db_schema,
  job_cd,
  fctr_nm,
  CASE WHEN (fctr_nm='annl_cmpn_amt') THEN (CLNT_ORG_MEAN - fctr_val)
  WHEN (fctr_nm='qtrs_since_last_promotion' OR fctr_nm = 'overtime_dbltime_hours') THEN (fctr_val - CLNT_ORG_MEAN)
  ELSE 0 END as fctr_val_diff
FROM ${__BLUE_MAIN_DB__}.top_risk_insights
WHERE CASE WHEN(fctr_nm='annl_cmpn_amt') THEN fctr_val < CLNT_ORG_MEAN
ELSE fctr_val > CLNT_ORG_MEAN END
)
GROUP BY clnt_obj_id,db_schema,fctr_nm,job_cd;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.top_risk_inds_diff_means;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.top_risk_inds_diff_means(
    sectorCode STRING,
    PRMRY_STD_JOB_TITL_KY STRING,
    FCTR_NM STRING,
    INDS_DIFF_MEAN DOUBLE
)USING PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.top_risk_inds_diff_means
SELECT
  sectorCode,
  prmry_std_job_titl_ky,
  fctr_nm,
  AVG(fctr_val_diff) as inds_diff_mean
FROM
(
SELECT
  sectorCode,
  prmry_std_job_titl_ky,
  fctr_nm,
  CASE WHEN (fctr_nm='annl_cmpn_amt') THEN (INDS_ORG_MEAN - fctr_val)
  WHEN (fctr_nm='qtrs_since_last_promotion' OR fctr_nm = 'overtime_dbltime_hours') THEN (fctr_val - INDS_ORG_MEAN)
  ELSE 0 END as fctr_val_diff
FROM ${__BLUE_MAIN_DB__}.top_risk_insights
WHERE CASE WHEN(fctr_nm='annl_cmpn_amt') THEN fctr_val < INDS_ORG_MEAN
ELSE fctr_val > INDS_ORG_MEAN END
)
GROUP BY sectorCode,prmry_std_job_titl_ky,fctr_nm;



DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_fact_empl_ins;

CREATE TABLE ${__BLUE_MAIN_DB__}.t_fact_empl_ins(
    INS_HASH_VAL STRING,
    CLNT_OBJ_ID STRING,
    PERS_OBJ_ID STRING,
    RPT_DT STRING,
    TRNOVR_PRBLTY_PCT DOUBLE,
    TRNOVR_PRBLTY_RANGE_DSC STRING,
    TRNOVR_PRBLTY_RANGE_LVL INT,
    CLNT_EMPL_CNT INT,
    MNGR_TOT_RPT_CNT INT,
    MNGR_JOB_EMPL_CNT INT,
    INDS_CLNT_CNT INT,
    INDS_EMPL_CNT INT,
    INS_TYPE STRING,
    INS_RSN STRING,
    FCTR_VAL DOUBLE,
    NOR_FCTR_VAL DOUBLE,
    sectorCode STRING,
    db_schema STRING,
    MNGR_PERS_OBJ_ID STRING,
    JOB_CD STRING,
    --REG_TEMP_DSC STRING,
    --FWA_REC_EFF_STRT_DT TIMESTAMP,
    --FWA_REC_EFF_END_DT TIMESTAMP,
    --WORK_LOC_CD STRING,
    PRMRY_STD_JOB_TITL_KY STRING,
    MNGR_OUTLIER STRING,
    CLNT_OUTLIER STRING,
    INDS_OUTLIER STRING,
    CLNT_ORG_MEAN DOUBLE,
    CLNT_NOR_MEAN DOUBLE,
    CLNT_ORG_STDDEV DOUBLE,
    CLNT_NOR_STDDEV DOUBLE,
    CLNT_MEAS_VAL DOUBLE,
    --CLNT_BNCHMRK_ORG_IQR_LOWER_OL DOUBLE,
    --CLNT_BNCHMRK_NOR_IQR_LOWER_OL DOUBLE,
    --CLNT_BNCHMRK_ORG_IQR_UPPER_OL DOUBLE,
    --CLNT_BNCHMRK_NOR_IQR_UPPER_OL DOUBLE,
    MNGR_ORG_MEAN DOUBLE,
    MNGR_NOR_MEAN DOUBLE,
    MNGR_ORG_STDDEV DOUBLE,
    MNGR_NOR_STDDEV DOUBLE,
    MNGR_MEAS_VAL DOUBLE,
    --MNGR_BNCHMRK_ORG_IQR_LOWER_OL DOUBLE,
    --MNGR_BNCHMRK_NOR_IQR_LOWER_OL DOUBLE,
    --MNGR_BNCHMRK_ORG_IQR_UPPER_OL DOUBLE,
    --MNGR_BNCHMRK_NOR_IQR_UPPER_OL DOUBLE,
    INDS_ORG_MEAN DOUBLE,
    INDS_NOR_MEAN DOUBLE,
    INDS_ORG_STDDEV DOUBLE,
    INDS_NOR_STDDEV DOUBLE,
    INDS_MEAS_VAL DOUBLE,
    --INDS_BNCHMRK_ORG_IQR_LOWER_OL DOUBLE,
    --INDS_BNCHMRK_NOR_IQR_LOWER_OL DOUBLE,
    --INDS_BNCHMRK_ORG_IQR_UPPER_OL DOUBLE,
    --INDS_BNCHMRK_NOR_IQR_UPPER_OL DOUBLE,
    CLNT_LVL_ORG_ZSCORE DOUBLE,
    CLNT_LVL_NOR_ZSCORE DOUBLE,
    MNGR_LVL_ORG_ZSCORE DOUBLE,
    MNGR_LVL_NOR_ZSCORE DOUBLE,
    INDS_LVL_ORG_ZSCORE DOUBLE,
    INDS_LVL_NOR_ZSCORE DOUBLE
    --ENVIRONMENT STRING
)USING PARQUET
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_fact_empl_ins
SELECT
  md5(concat(
        COALESCE(split(db_schema,'[|]')[0],'-'),
        COALESCE(clnt_obj_id,'-'),
        COALESCE(cast(year(rpt_dt) as string),'-'),
        COALESCE(cast(quarter(rpt_dt) as string),'-'),
        COALESCE(cast(month(rpt_dt) as string),'-'),
        COALESCE(mngr_pers_obj_id,'-'),
        '701',
        'trnovr_risk_headline',
        COALESCE(fctr_nm,'-'),
        COALESCE(sectorCode,'-')
        )) as insight_hash ,
  CLNT_OBJ_ID , 
  PERS_OBJ_ID ,
  RPT_DT , 
  TRNOVR_PRBLTY_PCT , 
  TRNOVR_PRBLTY_RANGE_DSC , 
  TRNOVR_PRBLTY_RANGE_LVL,
  clnt_empl_cnt,
  mngr_tot_rpt_cnt,
  mngr_job_empl_cnt,
  inds_clnt_cnt,
  inds_empl_cnt,
  'trnovr_risk_headline',
  FCTR_NM,
  FCTR_VAL ,
  NOR_FCTR_VAL,
  sectorCode , 
  db_schema , 
  MNGR_PERS_OBJ_ID , 
  JOB_CD ,
  PRMRY_STD_JOB_TITL_KY,
  MNGR_OUTLIER,
  CLNT_OUTLIER,
  INDS_OUTLIER,
  CLNT_ORG_MEAN,
  CLNT_NOR_MEAN,
  CLNT_ORG_STDDEV,
  CLNT_NOR_STDDEV,
  CLNT_MEAS_VAL,
  MNGR_ORG_MEAN,
  MNGR_NOR_MEAN,
  MNGR_ORG_STDDEV,
  MNGR_NOR_STDDEV,
  MNGR_MEAS_VAL,
  INDS_ORG_MEAN,
  INDS_NOR_MEAN,
  INDS_ORG_STDDEV,
  INDS_NOR_STDDEV,
  INDS_MEAS_VAL,
  CLNT_LVL_ORG_ZSCORE,
  CLNT_LVL_NOR_ZSCORE,
  MNGR_LVL_ORG_ZSCORE,
  MNGR_LVL_NOR_ZSCORE,
  INDS_LVL_ORG_ZSCORE,
  INDS_LVL_NOR_ZSCORE
FROM
(SELECT 
    main.CLNT_OBJ_ID , 
    PERS_OBJ_ID ,
    RPT_DT , 
    TRNOVR_PRBLTY_PCT , 
    TRNOVR_PRBLTY_RANGE_DSC , 
    TRNOVR_PRBLTY_RANGE_LVL,
    clnt_empl_cnt,
    mngr_tot_rpt_cnt,
    mngr_job_empl_cnt,
    inds_clnt_cnt,
    inds_empl_cnt,
    main.FCTR_NM,
    FCTR_VAL ,
    NOR_FCTR_VAL,
    main.sectorCode , 
    main.db_schema , 
    main.MNGR_PERS_OBJ_ID , 
    main.JOB_CD ,
    REG_TEMP_DSC,
    --FWA.WORK_LOC_CD,
	--FWA.ENVIRONMENT,
    main.PRMRY_STD_JOB_TITL_KY,
    CASE WHEN (main.FCTR_NM='annl_cmpn_amt' AND main.FCTR_VAL < MNGR_ORG_MEAN AND (((MNGR_ORG_MEAN - main.FCTR_VAL)/MNGR_ORG_MEAN)*100 >= (MNGR_DIFF_MEAN/MNGR_ORG_MEAN) * 100) AND (ROUND(MNGR_LVL_ORG_ZSCORE,4) < -1 OR ROUND(MNGR_LVL_NOR_ZSCORE,4) < -1)) THEN 'Y' 
         WHEN (main.FCTR_NM='qtrs_since_last_promotion' AND main.FCTR_VAL > MNGR_ORG_MEAN AND (((MNGR_ORG_MEAN - main.FCTR_VAL)/MNGR_ORG_MEAN)*100 <= (MNGR_DIFF_MEAN/MNGR_ORG_MEAN) * 100) AND (ROUND(MNGR_LVL_ORG_ZSCORE,4) > 1 OR ROUND(MNGR_LVL_NOR_ZSCORE,4) > 1)) THEN 'Y'
         WHEN (main.FCTR_NM='overtime_dbltime_hours' AND main.FCTR_VAL > MNGR_ORG_MEAN AND (((MNGR_ORG_MEAN - main.FCTR_VAL)/MNGR_ORG_MEAN)*100 <= (MNGR_DIFF_MEAN/MNGR_ORG_MEAN) * 100) AND (ROUND(MNGR_LVL_ORG_ZSCORE,4) > 1 OR ROUND(MNGR_LVL_NOR_ZSCORE,4) > 1)) THEN 'Y'
         ELSE 'N' END AS MNGR_OUTLIER,
    CASE WHEN (main.FCTR_NM='annl_cmpn_amt' AND main.FCTR_VAL < CLNT_ORG_MEAN AND (((CLNT_ORG_MEAN - main.FCTR_VAL)/CLNT_ORG_MEAN)*100 >= (CLNT_DIFF_MEAN/CLNT_ORG_MEAN) * 100) AND (ROUND(CLNT_LVL_ORG_ZSCORE,4) < -1 OR ROUND(CLNT_LVL_NOR_ZSCORE,4) < -1)) THEN 'Y' 
         WHEN (main.FCTR_NM='qtrs_since_last_promotion' AND main.FCTR_VAL > CLNT_ORG_MEAN AND (((CLNT_ORG_MEAN - main.FCTR_VAL)/CLNT_ORG_MEAN)*100 <= (CLNT_DIFF_MEAN/CLNT_ORG_MEAN) * 100) AND (ROUND(CLNT_LVL_ORG_ZSCORE,4) > 1 OR ROUND(CLNT_LVL_NOR_ZSCORE,4) > 1)) THEN 'Y'
         WHEN (main.FCTR_NM='overtime_dbltime_hours' AND main.FCTR_VAL > CLNT_ORG_MEAN AND (((CLNT_ORG_MEAN - main.FCTR_VAL)/CLNT_ORG_MEAN)*100 <= (CLNT_DIFF_MEAN/CLNT_ORG_MEAN) * 100) AND (ROUND(CLNT_LVL_ORG_ZSCORE,4) > 1 OR ROUND(CLNT_LVL_NOR_ZSCORE,4) > 1)) THEN 'Y'
         ELSE 'N' END 
    AS CLNT_OUTLIER,
    CASE WHEN (main.FCTR_NM IN ('annl_cmpn_amt','qtrs_since_last_promotion','overtime_dbltime_hours')  AND INDS_ORG_MEAN IS NULL) THEN NULL
         WHEN (main.FCTR_NM='annl_cmpn_amt' AND main.FCTR_VAL < INDS_ORG_MEAN AND (((INDS_ORG_MEAN - main.FCTR_VAL)/INDS_ORG_MEAN)*100 >= (INDS_DIFF_MEAN/INDS_ORG_MEAN) * 100) AND (ROUND(INDS_LVL_ORG_ZSCORE,4) < -1 OR ROUND(INDS_LVL_NOR_ZSCORE,4) < -1)) THEN 'Y' 
         WHEN (main.FCTR_NM='qtrs_since_last_promotion' AND main.FCTR_VAL > INDS_ORG_MEAN AND (((INDS_ORG_MEAN - main.FCTR_VAL)/INDS_ORG_MEAN)*100 <= (INDS_DIFF_MEAN/INDS_ORG_MEAN) * 100) AND (ROUND(INDS_LVL_ORG_ZSCORE,4) > 1 OR ROUND(INDS_LVL_NOR_ZSCORE,4) > 1)) THEN 'Y'
         WHEN (main.FCTR_NM='overtime_dbltime_hours' AND main.FCTR_VAL > INDS_ORG_MEAN AND (((INDS_ORG_MEAN - main.FCTR_VAL)/INDS_ORG_MEAN)*100 <= (INDS_DIFF_MEAN/INDS_ORG_MEAN) * 100) AND (ROUND(INDS_LVL_ORG_ZSCORE,4) > 1 OR ROUND(INDS_LVL_NOR_ZSCORE,4) > 1)) THEN 'Y'
         ELSE 'N' END
    AS INDS_OUTLIER,
    CLNT_ORG_MEAN,
    CLNT_NOR_MEAN,
    CLNT_ORG_STDDEV,
    CLNT_NOR_STDDEV,
    CLNT_MEAS_VAL,
    --CLNT_BNCHMRK_ORG_IQR_LOWER_OL,
    --CLNT_BNCHMRK_NOR_IQR_LOWER_OL,
    --CLNT_BNCHMRK_ORG_IQR_UPPER_OL,
    --CLNT_BNCHMRK_NOR_IQR_UPPER_OL,
    MNGR_ORG_MEAN,
    MNGR_NOR_MEAN,
    MNGR_ORG_STDDEV,
    MNGR_NOR_STDDEV,
    MNGR_MEAS_VAL,
    --MNGR_BNCHMRK_ORG_IQR_LOWER_OL,
    --MNGR_BNCHMRK_NOR_IQR_LOWER_OL,
    --MNGR_BNCHMRK_ORG_IQR_UPPER_OL,
    --MNGR_BNCHMRK_NOR_IQR_UPPER_OL,
    INDS_ORG_MEAN,
    INDS_NOR_MEAN,
    INDS_ORG_STDDEV,
    INDS_NOR_STDDEV,
    INDS_MEAS_VAL,
    --INDS_BNCHMRK_ORG_IQR_LOWER_OL,
    --INDS_BNCHMRK_NOR_IQR_LOWER_OL,
    --INDS_BNCHMRK_ORG_IQR_UPPER_OL,
    --INDS_BNCHMRK_NOR_IQR_UPPER_OL,
    CLNT_LVL_ORG_ZSCORE,
    CLNT_LVL_NOR_ZSCORE,
    MNGR_LVL_ORG_ZSCORE,
    MNGR_LVL_NOR_ZSCORE,
    INDS_LVL_ORG_ZSCORE,
    INDS_LVL_NOR_ZSCORE
FROM ${__BLUE_MAIN_DB__}.top_risk_insights main
INNER JOIN ${__BLUE_MAIN_DB__}.top_risk_mngr_diff_means mngr
ON main.clnt_obj_id = mngr.clnt_obj_id
AND main.db_Schema = mngr.db_schema
AND main.mngr_pers_obj_id = mngr.mngr_pers_obj_id
AND main.fctr_nm = mngr.fctr_nm
AND main.job_cd = mngr.job_cd
INNER JOIN ${__BLUE_MAIN_DB__}.top_risk_clnt_diff_means clnt
ON main.clnt_obj_id = clnt.clnt_obj_id
AND main.db_Schema = clnt.db_schema
AND main.fctr_nm = clnt.fctr_nm
AND main.job_cd = clnt.job_cd
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.top_risk_inds_diff_means inds
ON main.fctr_nm = inds.fctr_nm
AND main.sectorCode <=> inds.sectorCode
AND main.prmry_std_job_titl_ky <=> inds.prmry_std_job_titl_ky
WHERE fctr_val>0 and fctr_val is not null and 
mngr_tot_rpt_cnt>3 and mngr_job_empl_cnt >1)final
where MNGR_OUTLIER='Y' and CLNT_OUTLIER='Y';

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_ins_TOP_DW_EXTRACT;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.tmp_ins_TOP_DW_EXTRACT
USING PARQUET AS
select distinct clnt_obj_id ,
qtr_cd, 
pers_obj_id , 
lst_promo_dt , 
job_cd ,
COALESCE(annl_cmpn_amt,hr_annl_cmpn_amt) as cmpn_amt ,
qtrs_since_last_promotion ,
overtime_dbltime_hours,
db_schema
from ${__BLUE_MAIN_DB__}.TOP_DW_EXTRACT_TRH;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_ins_mngr_trnovr_risk_cube;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.tmp_ins_mngr_trnovr_risk_cube
USING PARQUET AS
select 
clnt_obj_id , 
mngr_pers_obj_id,
job_cd,
db_schema,
annl_cmpn_amt_org_mean,
overtime_dbltime_hours_org_mean,
qtrs_since_last_promotion_org_mean
from ${__GREEN_MAIN_DB__}.mngr_trnovr_risk_cube ;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_fact_empl_ins_stg;

CREATE TABLE ${__BLUE_MAIN_DB__}.t_fact_empl_ins_stg (
INS_HASH_VAL  STRING,
CLNT_OBJ_ID  STRING,
PERS_OBJ_ID  STRING,
YR_CD  STRING,
QTR_CD  STRING,
MNTH_CD  STRING,
WK_CD  STRING,
CMPNY_CD  STRING,
FLSA_STUS_CD  STRING,
FULL_TM_PART_TM_CD  STRING,
GNDR_CD  STRING,
HR_ORGN_ID  STRING,
JOB_CD  STRING,
PAY_GRP_CD  STRING,
PAYCD_GRP_CD  STRING,
PAYCD_ID  STRING,
PAY_RT_TYPE_CD  STRING,
PAYRL_ORGN_ID  STRING,
REG_TEMP_CD  STRING,
WORK_LOC_CD  STRING,
CITY_ID  STRING,
STATE_PROV_CD  STRING,
ISO_3_CHAR_CNTRY_CD  STRING,
TRMNT_RSN  STRING,
ADP_LENSE_CD  STRING,
INDS_CD  STRING,
SECTOR_CD  STRING,
SUPER_SECT_CD  STRING,
MNGR_PERS_OBJ_ID  STRING,
MTRC_KY int,
INS_SCOR double,
PCTL_RANK double,
INS_TYPE   STRING,
INS_RSN   STRING,
EMPL_CNT double,
PCT_EMPL_CNT double,
NBR_OF_DIMENTS int,
INS_JSON   STRING,
INS_EMPL_VAL double,
INS_TEAM_VAL double,
INS_ORG_VAL double,
INS_INDS_VAL double,
REC_CRT_TS DATE,
CATG_TYPE  STRING,
EXCP_TYPE  STRING,
SUPVR_PERS_OBJ_ID STRING,
RPT_TYPE STRING,
db_schema STRING) 
USING PARQUET

TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_fact_empl_ins_stg 
SELECT 
DISTINCT
ins.INS_HASH_VAL  ,
ins.CLNT_OBJ_ID  ,
ins.PERS_OBJ_ID  ,
year(rpt_dt) as YR_CD  ,
quarter(rpt_dt) as QTR_CD  ,
month(rpt_dt) as MNTH_CD  ,
null as WK_CD  ,
null as CMPNY_CD  ,
null as FLSA_STUS_CD  ,
null as FULL_TM_PART_TM_CD  ,
null as GNDR_CD  ,
null as HR_ORGN_ID  ,
ins.JOB_CD  ,
null as PAY_GRP_CD  ,
null as PAYCD_GRP_CD  ,
null as PAYCD_ID  ,
null as PAY_RT_TYPE_CD  ,
null as PAYRL_ORGN_ID  ,
null as REG_TEMP_CD  ,
null as WORK_LOC_CD  ,
null as CITY_ID  ,
null as STATE_PROV_CD  ,
null as ISO_3_CHAR_CNTRY_CD  ,
null as TRMNT_RSN  ,
null as ADP_LENSE_CD  ,
null as INDS_CD  ,
ins.sectorCode as SECTOR_CD  ,
null as SUPER_SECT_CD  ,
ins.MNGR_PERS_OBJ_ID  ,
null as  MTRC_KY ,
null as  INS_SCOR ,
null as PCTL_RANK ,
upper(ins.INS_TYPE) as INS_TYPE  ,
upper(ins.INS_RSN) as INS_RSN  ,
null as EMPL_CNT ,
null as PCT_EMPL_CNT ,
null as NBR_OF_DIMENTS ,
CONCAT(
        "{ \"annl_cmpn_amt\": ", COALESCE(cast(tde.cmpn_amt AS STRING),'null'),
        ",\"team_annl_cmpn_amt\": ", COALESCE(CAST(trc.annl_cmpn_amt_org_mean AS STRING),'null'),
        ",\"benchmark_annl_cmpn_amt\": ", COALESCE(CAST(BP.MEAS_50TH_PERCENTILE AS STRING),'null'),
        ",\"overtime_dbltime_hours\": ", COALESCE(CAST(tde.overtime_dbltime_hours AS STRING),'null'),
        ",\"team_overtime_dbltime_hours\": ", COALESCE(CAST(trc.overtime_dbltime_hours_org_mean AS STRING),'null'),
        ",\"qtrs_since_last_promotion\": ", COALESCE(CAST(tde.qtrs_since_last_promotion AS STRING),'null'),
        ",\"team_qtrs_since_last_promotion\": ", COALESCE(CAST(trc.qtrs_since_last_promotion_org_mean AS STRING),'null'),"}"
    ) as INS_JSON   ,
ins.FCTR_VAL as INS_EMPL_VAL ,
ins.MNGR_ORG_MEAN as INS_TEAM_VAL ,
ins.CLNT_ORG_MEAN as INS_ORG_VAL ,
ins.INDS_MEAS_VAL as INS_INDS_VAL ,
cast(from_unixtime (unix_timestamp (), 'MM/dd/yyyy') as DATE) as REC_CRT_TS,
ins.TRNOVR_PRBLTY_RANGE_DSC as CATG_TYPE  ,
null as EXCP_TYPE  ,
null as SUPVR_PERS_OBJ_ID ,
null as RPT_TYPE ,
ins.db_schema 
from 
${__BLUE_MAIN_DB__}.t_fact_empl_ins ins 
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_ins_top_dw_extract tde 
ON ins.clnt_obj_id<=> tde.clnt_obj_id 
and ins.pers_obj_id=tde.pers_obj_id
and ins.job_cd=tde.job_cd 
and ins.db_schema=tde.db_schema
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_ins_mngr_trnovr_risk_cube trc 
ON ins.clnt_obj_id<=> trc.clnt_obj_id 
and ins.mngr_pers_obj_id=trc.mngr_pers_obj_id
and ins.job_cd=trc.job_cd 
and ins.db_schema=trc.db_schema
LEFT OUTER JOIN (
    SELECT 
      STD_JOB_TITL_KY,
      meas_nm,
      SECTOR,
      CLNT_CNT,
      EMPL_CNT,
      MEAS_ORG_MEAN,
      MEAS_NOR_MEAN,
      MEAS_ORG_STDDEV,
      MEAS_NOR_STDDEV,
      MEAS_50TH_PERCENTILE
    FROM ${__BLUE_MAIN_DB__}.INDS_BNCHMRK_TRNOVR_RISK BP
    WHERE 
    MEAS_ORG_MEAN IS NOT NULL AND SUPERSECTOR='-1' AND STD_JOB_TITL_KY <> '-1' AND SECTOR <> '-1'
    AND QTR IN (SELECT MAX(QTR) FROM ${__BLUE_MAIN_DB__}.INDS_BNCHMRK_TRNOVR_RISK)
	AND meas_nm='annl_cmpn_amt') BP
ON BP.STD_JOB_TITL_KY <=> ins.PRMRY_STD_JOB_TITL_KY
AND bp.SECTOR <=> ins.sectorCode;