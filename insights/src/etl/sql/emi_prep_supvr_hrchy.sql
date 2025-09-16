-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_prep_supvr_rpt_hrchy;
--rpt to hrchy
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_supvr_rpt_hrchy (
    clnt_obj_id STRING,
    Login_supvr_pers_obj_id STRING, 
    supvr_pers_obj_id STRING,
    pers_obj_id STRING,
    lvl_from_prnt_nbr double,
    work_asgnmt_nbr STRING,
    db_schema STRING,
    environment STRING
)USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_prep_supvr_chpick;

-- cherry pick
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_supvr_chpick (
    clnt_obj_id STRING,
    Login_supvr_pers_obj_id STRING, 
    supvr_pers_obj_id STRING,
    pers_obj_id STRING,
    lvl_from_prnt_nbr double,
    work_asgnmt_nbr STRING,
    db_schema STRING,
    environment STRING
)USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_prep_supvr_temp_hrchy;

-- RPT + ChPICK 
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_supvr_temp_hrchy (
    clnt_obj_id STRING,
    Login_supvr_pers_obj_id STRING, 
    supvr_pers_obj_id STRING, 
    pers_obj_id STRING,
    lvl_from_prnt_nbr double,
    work_asgnmt_nbr STRING,
     db_schema STRING,
    environment STRING
)USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

--final table
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_supvr_hrchy (
    clnt_obj_id STRING,
    Login_supvr_pers_obj_id STRING, 
    supvr_pers_obj_id STRING, 
    pers_obj_id STRING,
    lvl_from_prnt_nbr double,
    rpt_access STRING,
    work_asgnmt_nbr STRING,
    db_schema STRING,
    environment STRING
)USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');


INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_supvr_rpt_hrchy PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    clnt_obj_id,
    Login_supvr_pers_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
    lvl_from_prnt_nbr,
    work_asgnmt_nbr,
    db_schema,
    environment
FROM
(select 
    DISTINCT
    rpt.clnt_obj_id,
    rpt.supvr_pers_obj_id as Login_supvr_pers_obj_id, --Login Supervisor
    rpt.pers_obj_id as supvr_pers_obj_id, -- Supervisor reporting to Login Supervisor
    waf.pers_obj_id as pers_obj_id,
    lvl_from_prnt_nbr,
    null as work_asgnmt_nbr,
    rpt.environment as environment,
    rpt.db_schema as db_schema
FROM ${__BLUE_MAIN_DB__}.emi_t_supvr_rpt_to_hrchy_sec rpt
inner join ${__BLUE_MAIN_DB__}.emi_base_hr_waf waf on 
rpt.db_schema = waf.db_schema
AND rpt.environment = waf.environment
AND rpt.clnt_obj_id = waf.clnt_obj_id
AND rpt.pers_obj_id = waf.supvr_pers_obj_id
where  
rpt.supvr_pers_obj_id is NOT null
and rpt.pers_obj_id is NOT null
AND waf.rec_eff_strt_dt >= rpt.rec_eff_strt_dt AND waf.rec_eff_end_dt <= rpt.rec_eff_end_dt)tmp;
--DISTRIBUTE BY environment,db_schema;


INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_supvr_chpick PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    clnt_obj_id,
    Login_supvr_pers_obj_id, --Login Supervisor
    supvr_pers_obj_id, -- Supervisor reporting to Login Supervisor
    pers_obj_id,
    lvl_from_prnt_nbr,
    work_asgnmt_nbr,
    db_schema,
    environment
FROM
(select
    DISTINCT
    chpick.clnt_obj_id,
    chpick.supvr_pers_obj_id as Login_supvr_pers_obj_id, --Login Supervisor
    chpick.supvr_pers_obj_id, -- Supervisor reporting to Login Supervisor
    chpick.pers_obj_id as pers_obj_id,
    0 as lvl_from_prnt_nbr,
    chpick.work_asgnmt_nbr as work_asgnmt_nbr,
    chpick.environment as environment,
    chpick.db_schema as db_schema
FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_supvr_cherrypick chpick  
INNER JOIN ${__BLUE_MAIN_DB__}.emi_base_hr_waf waf on
chpick.environment = waf.environment
AND chpick.db_schema = waf.db_schema
AND chpick.clnt_obj_id = waf.clnt_obj_id
AND chpick.pers_obj_id = waf.pers_obj_id
AND chpick.work_asgnmt_nbr = waf.work_asgnmt_nbr)tmp;
--DISTRIBUTE BY environment,db_schema;



INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_supvr_temp_hrchy PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    clnt_obj_id,
    Login_supvr_pers_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
    lvl_from_prnt_nbr,
    work_asgnmt_nbr,
    db_schema,
    environment
FROM
(select 
    COALESCE(rpt_clnt_obj_id,chpick_clnt_obj_id) as clnt_obj_id,
    COALESCE(rpt_Login_supvr_pers_obj_id,chpick_Login_supvr_pers_obj_id) as Login_supvr_pers_obj_id,
    COALESCE(rpt_supvr_pers_obj_id,chpick_supvr_pers_obj_id) as supvr_pers_obj_id,
    COALESCE(rpt_pers_obj_id,chpick_pers_obj_id) as pers_obj_id,
    CASE WHEN (rpt_lvl_from_prnt_nbr is not null and chpick_lvl_from_prnt_nbr is not null) then 0 
    WHEN rpt_lvl_from_prnt_nbr is not null then rpt_lvl_from_prnt_nbr
    WHEN chpick_lvl_from_prnt_nbr is not null then chpick_lvl_from_prnt_nbr END as lvl_from_prnt_nbr,
    work_asgnmt_nbr,
    COALESCE(rpt_environment,chpick_environment) as environment,
    COALESCE(rpt_db_schema,chpick_db_schema) as db_schema
FROM
(select
    rpt.clnt_obj_id as rpt_clnt_obj_id,
    chpick.clnt_obj_id as chpick_clnt_obj_id,
    rpt.Login_supvr_pers_obj_id as rpt_Login_supvr_pers_obj_id, 
    chpick.Login_supvr_pers_obj_id as chpick_Login_supvr_pers_obj_id, 
    rpt.supvr_pers_obj_id as rpt_supvr_pers_obj_id, 
    chpick.supvr_pers_obj_id as chpick_supvr_pers_obj_id, 
    rpt.pers_obj_id as rpt_pers_obj_id,
    chpick.pers_obj_id as chpick_pers_obj_id,
    rpt.lvl_from_prnt_nbr as rpt_lvl_from_prnt_nbr,
    chpick.lvl_from_prnt_nbr as chpick_lvl_from_prnt_nbr,
    chpick.work_asgnmt_nbr,
    rpt.environment as rpt_environment,
    chpick.environment as chpick_environment,
    rpt.db_schema as rpt_db_schema,
    chpick.db_schema as chpick_db_schema
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_supvr_rpt_hrchy rpt 
    FULL OUTER join
    ${__BLUE_MAIN_DB__}.emi_prep_supvr_chpick chpick on
    rpt.db_schema = chpick.db_schema
    AND rpt.environment=chpick.environment
    AND rpt.clnt_obj_id = chpick.clnt_obj_id
    --AND rpt.supvr_pers_obj_id = chpick.supvr_pers_obj_id
    AND rpt.Login_supvr_pers_obj_id = chpick.Login_supvr_pers_obj_id
AND rpt.pers_obj_id = chpick.pers_obj_id) hrchy)tmp;
--DISTRIBUTE BY environment,db_schema;
 

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_supvr_hrchy PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    clnt_obj_id,
    Login_supvr_pers_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
    lvl_from_prnt_nbr,
    rpt_access,
    work_asgnmt_nbr,
    db_schema,
    environment
FROM
(select 
    clnt_obj_id,
    Login_supvr_pers_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
    lvl_from_prnt_nbr,
    'D' as rpt_access,
    work_asgnmt_nbr,
    environment,
    db_schema
FROM ${__BLUE_MAIN_DB__}.emi_prep_supvr_temp_hrchy
WHERE 
lvl_from_prnt_nbr = 0

UNION ALL

select 
    clnt_obj_id,
    Login_supvr_pers_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
    lvl_from_prnt_nbr,
    'DI' as rpt_access,
    work_asgnmt_nbr,
    environment,
    db_schema
FROM ${__BLUE_MAIN_DB__}.emi_prep_supvr_temp_hrchy 
WHERE 
lvl_from_prnt_nbr >= 0)tmp;
--DISTRIBUTE BY environment,db_schema;