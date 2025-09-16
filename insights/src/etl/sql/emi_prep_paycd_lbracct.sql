-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=20000;
set hive.exec.max.dynamic.partitions.pernode=2000;


DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_prep_labor_acct_sec;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_labor_acct_sec (
    clnt_obj_id STRING,
    labor_acct_id STRING,
    mngr_accs_labor_acct_set_id STRING,
    mngr_pers_obj_id STRING,
    db_schema STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_prep_paycd_sec;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_paycd_sec (
    clnt_obj_id STRING,
    mngr_pers_obj_id STRING,
    mngr_view_paycd_accs_prfl_id STRING,
    paycd_id STRING,
    db_schema STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');
 
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_paycd_lbracct
(
    clnt_obj_id string,
    mngr_pers_obj_id string,
    labor_acct_id string,
    paycd_id string,
    mngr_view_paycd_accs_prfl_id string,
    mngr_accs_labor_acct_set_id string,
    is_labor_acct_set_null string,
    is_paycd_set_null string,
    db_schema STRING,
    flag STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_labor_acct_sec PARTITION (environment)
SELECT 
    /*+ COALESCE(800) */
    accs.clnt_obj_id ,
    lmap1.labor_acct_id ,
    accs.mngr_accs_labor_acct_set_id ,
    accs.mngr_pers_obj_id,
    accs.db_schema,
    accs.environment
FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_labor_acct_sec accs
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_fact_work_asgnmt fwa 
  ON accs.environment = fwa.environment
  AND accs.clnt_obj_id = fwa.clnt_obj_id
  AND accs.db_schema = fwa.db_schema
  AND accs.mngr_pers_obj_id = fwa.pers_obj_id
  AND fwa.prmry_work_asgnmt_ind = 1
  AND fwa.work_asgnmt_stus_cd IN ('P', 'A', 'L', 'S')
  AND current_date BETWEEN fwa.rec_eff_strt_dt AND fwa.rec_eff_end_dt
LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_labor_acct_set_map lmap1 
  ON accs.environment = lmap1.environment 
  AND accs.clnt_obj_id = lmap1.clnt_obj_id
  AND accs.db_schema = lmap1.db_schema
  AND accs.mngr_accs_labor_acct_set_id = lmap1.labor_acct_set_id
  AND accs.mngr_accs_labor_acct_set_id != '-1'
WHERE fwa.glob_excld_ind=0
DISTRIBUTE BY accs.environment, accs.clnt_obj_id, accs.mngr_pers_obj_id, accs.db_schema;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_paycd_sec PARTITION (environment)
SELECT 
    /*+ COALESCE(800) */
    accs.clnt_obj_id ,
    accs.mngr_pers_obj_id,
    accs.mngr_view_paycd_accs_prfl_id,
    lmap1.paycd_id,
    accs.db_schema,
    accs.environment
FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_paycd_sec accs
INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_fact_work_asgnmt fwa 
  ON accs.environment = fwa.environment
  AND accs.clnt_obj_id = fwa.clnt_obj_id
  AND accs.db_schema = fwa.db_schema
  AND accs.mngr_pers_obj_id = fwa.pers_obj_id
  AND fwa.prmry_work_asgnmt_ind = 1
  AND fwa.work_asgnmt_stus_cd IN ('P', 'A', 'L', 'S')
  AND current_date BETWEEN fwa.rec_eff_strt_dt AND fwa.rec_eff_end_dt
LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_paycd_accs_prfl_map lmap1 
  ON accs.environment = lmap1.environment
  AND accs.clnt_obj_id = lmap1.clnt_obj_id
  AND accs.db_schema = lmap1.db_schema
  AND accs.mngr_view_paycd_accs_prfl_id = lmap1.paycd_accs_prfl_id
  AND accs.mngr_view_paycd_accs_prfl_id != '-1'
WHERE fwa.glob_excld_ind=0
DISTRIBUTE BY accs.environment, accs.clnt_obj_id, accs.mngr_pers_obj_id, accs.db_schema;


INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_paycd_lbracct PARTITION (environment)
SELECT
    /*+ COALESCE(800) */
    clnt_obj_id,
    mngr_pers_obj_id,
    labor_acct_id,
    paycd_id,
    mngr_view_paycd_accs_prfl_id,
    mngr_accs_labor_acct_set_id,
    is_labor_acct_set_null,
    is_paycd_set_null,
    db_schema,
    flag,
    environment
FROM
(SELECT
    l.clnt_obj_id,
    l.mngr_pers_obj_id,
    l.labor_acct_id,
    p.paycd_id,
    p.mngr_view_paycd_accs_prfl_id,
    l.mngr_accs_labor_acct_set_id,
    CASE WHEN mngr_accs_labor_acct_set_id = '-1' THEN '-1' ELSE '0' END as is_labor_acct_set_null,
    CASE WHEN mngr_view_paycd_accs_prfl_id = '-1' THEN '-1' ELSE '0' END as is_paycd_set_null,
    l.db_schema,
    'PAY_LAB_SEC' as flag,
    l.environment
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_labor_acct_sec l
INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_paycd_sec p
    ON l.environment = p.environment
    AND l.db_schema = p.db_schema
    AND l.clnt_obj_id = p.clnt_obj_id
    AND l.mngr_pers_obj_id = p.mngr_pers_obj_id
WHERE
(
        l.clnt_obj_id NOT IN ('G3G7E0K05TQ92T61','G3DXGP3QFGB762KW','G40ZG5GCZNC000CT','G3VED14G68F3B652','G31YFM4Z16RA03VF',
        'G3Q60CDD3AE5RMFV','G3GB18BTVRQDH6JQ','G3FZHEA5EBTDF6F7','G3QC1S8R053FKNC4','G3NQJQHTNF8F3BPM',
        'G3C97EEGEYBNHBBH','G38NACXXVDQ7Y73P','G385HQ0GG37BHG07','G3TEKEE00VQS37YR','G31MXMRZESC0TFG8',
        '0F18AC8F6B8002A9','G3PRR0401C6ARSFC','G3GKPB5DZSY27ATG','0247K0T57BW0003R','G3DXGP3QFGB7THBN',
        'G3Y41H9HF5YHQZZR','G337ANBN148KAJAN','G3MY05GEB7X9Z0PF','G32NXNAMD700Q2S1','G3AMKMQ4X429D89D',
        'G31XE2REGFQ6FEV7','G30PTGK3Q31C4A9B','G3HZKGHPEKZJZ5YQ','EF0597B9DB00173D','G3CN2DVCRKM4113K',
        'G32EA4Y7R2NSERRJ','315DB8B8D9900700','G385HQ0GG37BZYFM','G3TP19G0WXAB9DZV','G3GCVC3NS9S72888',
        'G4EQAA2P98K0003M','G3D9VQVK98QSXX7X','G34NC2ARYPJN4ZMH','G3QPQMCM44YGE9RF','G3KEV4WCBYS09SJG')
    OR
    (mngr_view_paycd_accs_prfl_id = '-1' AND mngr_accs_labor_acct_set_id = '-1')
    )

UNION ALL

SELECT
    l.clnt_obj_id,
    l.mngr_pers_obj_id,
    l.labor_acct_id,   
    NULL AS paycd_id,
    NULL AS mngr_view_paycd_accs_prfl_id,
    l.mngr_accs_labor_acct_set_id,
    CASE WHEN mngr_accs_labor_acct_set_id = '-1' THEN '-1' ELSE '0' END as is_labor_acct_set_null,
    NULL AS is_paycd_set_null,
    l.db_schema,
    'LAB_SEC' as flag,
    l.environment
FROM
    ${__BLUE_MAIN_DB__}.emi_prep_labor_acct_sec l
WHERE
--l.environment = '${environment}'
(
        l.clnt_obj_id NOT IN ('G3G7E0K05TQ92T61','G3DXGP3QFGB762KW','G40ZG5GCZNC000CT','G3VED14G68F3B652','G31YFM4Z16RA03VF',
        'G3Q60CDD3AE5RMFV','G3GB18BTVRQDH6JQ','G3FZHEA5EBTDF6F7','G3QC1S8R053FKNC4','G3NQJQHTNF8F3BPM',
       'G3C97EEGEYBNHBBH','G38NACXXVDQ7Y73P','G385HQ0GG37BHG07','G3TEKEE00VQS37YR','G31MXMRZESC0TFG8',
        '0F18AC8F6B8002A9','G3PRR0401C6ARSFC','G3GKPB5DZSY27ATG','0247K0T57BW0003R','G3DXGP3QFGB7THBN',
        'G3Y41H9HF5YHQZZR','G337ANBN148KAJAN','G3MY05GEB7X9Z0PF','G32NXNAMD700Q2S1','G3AMKMQ4X429D89D',
        'G31XE2REGFQ6FEV7','G30PTGK3Q31C4A9B','G3HZKGHPEKZJZ5YQ','EF0597B9DB00173D','G3CN2DVCRKM4113K',
        'G32EA4Y7R2NSERRJ','315DB8B8D9900700','G385HQ0GG37BZYFM','G3TP19G0WXAB9DZV','G3GCVC3NS9S72888',
        'G4EQAA2P98K0003M','G3D9VQVK98QSXX7X','G34NC2ARYPJN4ZMH','G3QPQMCM44YGE9RF','G3KEV4WCBYS09SJG')
    OR
        (mngr_accs_labor_acct_set_id = '-1')
    )
)fianl
DISTRIBUTE BY db_schema,paycd_id,labor_acct_id;