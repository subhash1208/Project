-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_t_supvr_rpt_to_hrchy_sec(
    `clnt_obj_id` STRING,
    `pers_obj_id` STRING,
    `supvr_pers_obj_id` STRING,
    `lvl_from_prnt_nbr` DOUBLE,    
    `db_schema` STRING,
    `mtrx_hrchy_ind` STRING,
    `rec_eff_strt_dt` TIMESTAMP,
    `rec_eff_end_dt` TIMESTAMP,
    `environment` STRING
)USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_t_supvr_rpt_to_hrchy_sec PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    ext.clnt_obj_id,
    ext.supvr_pers_obj_id AS pers_obj_id,
    ext.login_supvr_pers_obj_id AS supvr_pers_obj_id,
    lvl_from_prnt_nbr,
    ext.db_schema,
    ext.mtrx_hrchy_ind,
    ext.rec_eff_strt_dt,
    ext.rec_eff_end_dt,
    ext.environment
FROM
    ${__RO_BLUE_RAW_DB__}.dwh_t_supvr_hrchy_sec ext
    --INNER JOIN (
    --    SELECT
    --        clnt_obj_id,
    --        CASE WHEN LOWER(sor)="wfnportal" THEN "wfn"
    --             WHEN LOWER(sor)="hriiportal" THEN "vantage"
    --             WHEN LOWER(sor)="portalselfservice" THEN "ev5"
    --             ELSE "*"
    --        END AS sor
    --        FROM ${__GREEN_MAIN_DB__}.t_clnt_msg
    --        LATERAL VIEW posexplode(clnt_resource) clnt_obj_id AS seqp, clnt_obj_id
    --        LATERAL VIEW posexplode(sor_resource) sor AS seqc, sor            
    --    WHERE
    --        feature_code = 'SUPVSR_HIERARCHY_EFF_DT'
    --) eff_dt 
    --ON (CASE WHEN eff_dt.clnt_obj_id="*" THEN 1 = 1 ELSE ext.clnt_obj_id = eff_dt.clnt_obj_id END)
    --AND (CASE WHEN eff_dt.sor="*" THEN 1=1 ELSE LOWER(ext.source) = eff_dt.sor END)
    LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pers pers
    ON ext.clnt_obj_id = pers.clnt_obj_id
    AND ext.environment = pers.environment
    AND ext.login_supvr_pers_obj_id = pers.pers_obj_id
    WHERE (ext.clnt_live_ind = 'Y' OR ext.clnt_obj_id IN (select distinct clnt_obj_id from ${__BLUE_MAIN_DB__}.emi_non_live_clnts));