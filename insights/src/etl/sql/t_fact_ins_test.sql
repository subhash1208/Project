-- Databricks notebook source
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.etl_tmp_t_fact_ins;

CREATE  TABLE  ${__BLUE_MAIN_DB__}.etl_tmp_t_fact_ins USING PARQUET as 
SELECT * from ${__BLUE_MAIN_DB__}.t_fact_ins;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_t_fact_ins_time_filter;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp_t_fact_ins_time_filter
USING PARQUET 
AS
select ins_hash_val from (
select 
row_number() over(partition by clnt_obj_Id,clnt_obj_id_cmpr_with , mngr_pers_obj_id ,mtrc_ky , ins_type , ins_rsn,
yr_cd , yr_cd_cmpr_with , qtr_cd , qtr_cd_cmpr_with  order by  nvl(mnth_cd , '0' ) desc , nvl(mnth_cd_cmpr_with , '0' ) desc ) as rn,
ins.*
from 
${__BLUE_MAIN_DB__}.etl_tmp_t_fact_ins  ins
where 
clnt_obj_Id is not null 
and clnt_obj_id_cmpr_with is not null
and job_cd is null  and work_loc_cd is null and hr_orgn_id is  null
and mngr_pers_obj_id is not null
and mtrc_ky not in (21,121)
) where rn>1 ;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp1_t_fact_ins;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp1_t_fact_ins
USING PARQUET 
AS
SELECT ins.* FROM ${__BLUE_MAIN_DB__}.etl_tmp_t_fact_ins ins left outer join 
${__GREEN_MAIN_DB__}.tmp_t_fact_ins_time_filter flt on ins.ins_hash_val=flt.ins_hash_val
where flt.ins_hash_val is null;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_t_fact_ins_dim_filter;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp_t_fact_ins_dim_filter
USING PARQUET 
AS
select distinct ins_hash_val from (
select 
DENSE_RANK () over (partition by clnt_obj_Id,clnt_obj_id_cmpr_with , mngr_pers_obj_id ,mtrc_ky ,  ins_rsn,
yr_cd , yr_cd_cmpr_with , qtr_cd , qtr_cd_cmpr_with,ins_empl_cnt order by code desc  ) as rn,
t.* 
from (
select ins.* , 
(case when ins.job_cd is null then 0 else 1 end)||
(case when ins.job_cd_cmpr_with is null then 0 else 1 end)||
(case when ins.work_loc_cd is null then 0 else 1 end)||
(case when ins.work_loc_cd_cmpr_with is null then 0 else 1 end)||
(case when ins.hr_orgn_id is null then 0 else 1 end)||
(case when ins.hr_orgn_id_cmpr_with is null then 0 else 1 end)||
(case when ins.yr_cd is null then 0 else 1 end)||
(case when ins.yr_cd_cmpr_with is null then 0 else 1 end)||
(case when ins.qtr_cd is null then 0 else 1 end)||
(case when ins.qtr_cd_cmpr_with is null then 0 else 1 end)||
(case when ins.MNTH_CD is null then 0 else 1 end)||
(case when ins.MNTH_CD_cmpr_with is null then 0 else 1 end)
as code,
insights.ins_empl_cnt
from ${__BLUE_MAIN_DB__}.tmp1_t_fact_ins ins 
inner join ${__BLUE_MAIN_DB__}.t_fact_insights insights 
on ins.ins_hash_val=insights.ins_hash_val
where ins.ins_type  in ('CLIENT_INTERNAL','CLIENT_INTERNAL_MYTEAM') 
and ins.clnt_obj_id_cmpr_with is not null 
and  ins.mngr_pers_obj_id is not null ) t )
where rn >1 ;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp2_t_fact_ins;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp2_t_fact_ins
USING PARQUET 
AS
SELECT ins.* FROM ${__BLUE_MAIN_DB__}.tmp1_t_fact_ins ins
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_t_fact_ins_dim_filter ins_filter on ins.ins_hash_val=ins_filter.ins_hash_val
where ins_filter.ins_hash_val is null
;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_fact_ins_test;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_fact_ins_test (
    ins_hash_val string,
    clnt_obj_id string,
    clnt_obj_id_cmpr_with string,
    yr_cd string,
    yr_cd_cmpr_with string,
    qtr_cd int,
    qtr_cd_cmpr_with int,
    mnth_cd int,
    mnth_cd_cmpr_with int,
    wk_cd int,
    wk_cd_cmpr_with int,
    flsa_stus_cd string,
    flsa_stus_cd_cmpr_with string,
    full_tm_part_tm_cd string,
    full_tm_part_tm_cd_cmpr_with string,
    gndr_cd string,
    gndr_cd_cmpr_with string,
    hr_orgn_id string,
    hr_orgn_id_cmpr_with string,
    job_cd string,
    job_cd_cmpr_with string,
    pay_rt_type_cd string,
    pay_rt_type_cd_cmpr_with string,
    reg_temp_cd string,
    reg_temp_cd_cmpr_with string,
    work_loc_cd string,
    work_loc_cd_cmpr_with string,
    city_id string,
    city_id_cmpr_with string,
    state_prov_cd string,
    state_prov_cd_cmpr_with string,
    iso_3_char_cntry_cd string,
    iso_3_char_cntry_cd_cmpr_with string,
    trmnt_rsn string,
    trmnt_rsn_cmpr_with string,
    adp_lense_cd string,
    adp_lense_cd_cmpr_with string,
    inds_cd string,
    inds_cd_cmpr_with string,
    sector_cd string,
    sector_cd_cmpr_with string,
    super_sect_cd string,
    super_sect_cd_cmpr_with string,
    mngr_pers_obj_id string,
    mngr_pers_obj_id_cmpr_with string,
    mtrc_ky int,
    ins_scor double,
    pctl_rank double,
    ins_type string,
    ins_rsn string,
    empl_cnt double,
    empl_cnt_cmpr_with double,
    pct_empl_cnt double,
    pct_empl_cnt_cmpr_with double,
    nbr_of_diments int,
    ins_json string,    
    rec_crt_ts DATE,
    excp_type string,
    excp_type_cmpr_with string,
    supvr_pers_obj_id string,
    supvr_pers_obj_id_cmpr_with string,
    RPT_TYPE STRING,
    db_schema STRING,
    environment string
) USING PARQUET
PARTITIONED BY (environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_fact_ins_test PARTITION(environment)
SELECT
    ins_hash_val,
    clnt_obj_id,
    clnt_obj_id_cmpr_with,
    yr_cd,
    yr_cd_cmpr_with,
    qtr_cd,
    qtr_cd_cmpr_with,
    mnth_cd,
    mnth_cd_cmpr_with,
    wk_cd,
    wk_cd_cmpr_with,
    flsa_stus_cd,
    flsa_stus_cd_cmpr_with,
    full_tm_part_tm_cd,
    full_tm_part_tm_cd_cmpr_with,
    gndr_cd,
    gndr_cd_cmpr_with,
    hr_orgn_id,
    hr_orgn_id_cmpr_with,
    job_cd,
    job_cd_cmpr_with,
    pay_rt_type_cd,
    pay_rt_type_cd_cmpr_with,
    reg_temp_cd,
    reg_temp_cd_cmpr_with,
    work_loc_cd,
    work_loc_cd_cmpr_with,
    city_id,
    city_id_cmpr_with,
    state_prov_cd,
    state_prov_cd_cmpr_with,
    iso_3_char_cntry_cd,
    iso_3_char_cntry_cd_cmpr_with,
    trmnt_rsn,
    trmnt_rsn_cmpr_with,
    adp_lense_cd,
    adp_lense_cd_cmpr_with,
    inds_cd,
    inds_cd_cmpr_with,
    sector_cd,
    sector_cd_cmpr_with,
    super_sect_cd,
    super_sect_cd_cmpr_with,
    mngr_pers_obj_id,
    mngr_pers_obj_id_cmpr_with,
    mtrc_ky,
    ins_scor,
    pctl_rank,
    ins_type,
    ins_rsn,
    empl_cnt,
    empl_cnt_cmpr_with,
    pct_empl_cnt,
    pct_empl_cnt_cmpr_with,
    nbr_of_diments,
    ins_json,
    rec_crt_ts,
    excp_type,
    excp_type_cmpr_with,
    supvr_pers_obj_id,
    supvr_pers_obj_id_cmpr_with,
    rpt_type,
    db_schema,
    environment 
	FROM ${__BLUE_MAIN_DB__}.tmp2_t_fact_ins;