-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_dim_today;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_dim_today (
    curr_yr INT,
    curr_qtr INT,
    curr_mnth INT,
    curr_qtr_num INT,
    day_cd STRING,
    environment STRING
)
STORED AS PARQUET;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_dim_today
SELECT 
distinct 
cast(yr_cd as int) as yr_cd,
cast(qtr_seq_nbr as int) as qtr_seq_nbr,
cast(mnth_seq_nbr as int) as mnth_seq_nbr,
qtr_in_yr_nbr,
date_format(day_dt,"yyyyMMdd") as day_cd,
environment
FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day 
WHERE 
to_date(day_dt)=current_date;


DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_fiscal_info;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_fiscal_info(
      clnt_obj_id string,
      dmn_ky decimal(38,0),
      curr_clnt_fisc_yr string,
      curr_clnt_fisc_qtr decimal(38,0),
      curr_clnt_fisc_mnth decimal(38,0),
      qtr_in_yr_nbr decimal(38,0),
      db_schema string
  ) PARTITIONED BY (environment string)
STORED AS PARQUET;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_fiscal_info PARTITION(environment)
SELECT
   clnt_obj_id,
   dmn_ky,
   yr_cd as curr_clnt_fisc_yr,
   cast(qtr_seq_nbr as int) as curr_clnt_fisc_qtr,
   cast(mnth_seq_nbr as int) as curr_clnt_fisc_mnth,
   qtr_in_yr_nbr,
   clndr.db_schema,
   clndr.environment
FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_fscl_clndr clndr 
INNER  JOIN (
    select distinct day_cd,environment from ${__BLUE_MAIN_DB__}.t_dim_today
) today
ON clndr.environment=today.environment
AND clndr.day_cd=today.day_cd
WHERE cast(clndr.yr_cd as int) >= (YEAR(CURRENT_DATE()) - 3);