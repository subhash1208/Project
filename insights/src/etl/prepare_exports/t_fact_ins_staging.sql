-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.tmp_t_fact_ins (
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

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_fact_ins_staging (
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

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_fact_ins_dups;

CREATE TABLE ${__BLUE_MAIN_DB__}.t_fact_ins_dups 
USING PARQUET 
AS
SELECT
  ins_hash_val
  FROM ${__BLUE_MAIN_DB__}.t_fact_insights
  GROUP BY ins_hash_val HAVING count(*)>1;

-- Include ONLY the insights which have not yet been exported to warehouse.
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.tmp_t_fact_ins PARTITION(environment)
SELECT
    /*+ COALESCE(800) */
    ins.ins_hash_val,
    ins.clnt_obj_id,
    ins.clnt_obj_id_cmpr_with,
    ins.yr_cd,
    ins.yr_cd_cmpr_with,
    ins.qtr_cd,
    ins.qtr_cd_cmpr_with,
    ins.mnth_cd,
    ins.mnth_cd_cmpr_with,
    ins.wk_cd,
    ins.wk_cd_cmpr_with,
    ins.flsa_stus_cd,
    ins.flsa_stus_cd_cmpr_with,
    ins.full_tm_part_tm_cd,
    ins.full_tm_part_tm_cd_cmpr_with,
    ins.gndr_cd,
    ins.gndr_cd_cmpr_with,
    ins.hr_orgn_id,
    ins.hr_orgn_id_cmpr_with,
    ins.job_cd,
    ins.job_cd_cmpr_with,
    ins.pay_rt_type_cd,
    ins.pay_rt_type_cd_cmpr_with,
    ins.reg_temp_cd,
    ins.reg_temp_cd_cmpr_with,
    ins.work_loc_cd,
    ins.work_loc_cd_cmpr_with,
    ins.city_id,
    ins.city_id_cmpr_with,
    ins.state_prov_cd,
    ins.state_prov_cd_cmpr_with,
    ins.iso_3_char_cntry_cd,
    ins.iso_3_char_cntry_cd_cmpr_with,
    ins.trmnt_rsn,
    ins.trmnt_rsn_cmpr_with,
    ins.adp_lense_cd,
    ins.adp_lense_cd_cmpr_with,
    ins.inds_cd,
    ins.inds_cd_cmpr_with,
    ins.sector_cd,
    ins.sector_cd_cmpr_with,
    ins.super_sect_cd,
    ins.super_sect_cd_cmpr_with,
    ins.mngr_pers_obj_id,
    ins.mngr_pers_obj_id_cmpr_with,
    ins.mtrc_ky,
    -- TODO: We would want to decay the weights of insights exponentially by their age. Should this decay logic go in application code??
    -- Ideally done using an expression like (1-r)^(x/y) where 
    -- r is a fraction indicating decay rate (eg = 0.8)
    -- x = number of days since the end of the period
    -- y = number of days any insight could be shown reasonably. This usually takes the form num_days_in_period/4.
    -- (i.e...) For yearly insights y=90. For quarterly insights y=~22. For monthly insights, y=~7.
    -- Eg plot of this function: http://fooplot.com/#W3sidHlwZSI6MCwiZXEiOiIoMC4yKV4oeC85MCkiLCJjb2xvciI6IiMwMDAwMDAifSx7InR5cGUiOjEwMDAsIndpbmRvdyI6WyItMTMxLjc2OTk5OTk5OTk5OTY0IiwiMjg0LjIzMDAwMDAwMDAwMDMiLCItMiIsIjIiXX1d
    ins.ins_scor,
    ins.pctl_rank,
    ins.ins_type,
    ins.ins_rsn,
    --CASE WHEN (summ.cnt >= 2 AND ins_rsn="MIN_PERCENTILE_RANKING") THEN "MIN_PERCENTILE_RANKING_MULTI"
    --     WHEN (summ.cnt >= 2 AND ins_rsn="MAX_PERCENTILE_RANKING") THEN "MAX_PERCENTILE_RANKING_MULTI" ELSE ins.ins_rsn END as ins_rsn,
    ins.empl_cnt,
    ins.empl_cnt_cmpr_with,
    ins.pct_empl_cnt,
    ins.pct_empl_cnt_cmpr_with,
    ins.nbr_of_diments,
    ins.ins_json,
    ins.rec_crt_ts,
    ins.excp_type,
    ins.excp_type_cmpr_with,
    ins.supvr_pers_obj_id,
    ins.supvr_pers_obj_id_cmpr_with,
    ins.rpt_type,
    ins.db_schema,
    ins.environment
FROM
    ${__BLUE_MAIN_DB__}.t_fact_insights ins
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.t_fact_ins_dups t_fact
ON ins.ins_hash_val = t_fact.ins_hash_val
 --LEFT OUTER JOIN (SELECT * FROM ${__BLUE_MAIN_DB__}.t_fact_insights_archive
--    WHERE '${hiveconf:incremental}' = 'true') arch
--    ON ins.environment = arch.environment
--    AND ins.db_schema = arch.db_schema
--    AND ins.ins_hash_val = arch.ins_hash_val
--    AND ins.mngr_pers_obj_id IS NULL -- We will refresh all manager insights by default all the time
--LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.clnt_min_max_mtrc_summ summ
--    ON ins.environment = summ.environment
--    AND ins.db_schema = summ.db_schema
--    AND ins.clnt_obj_id = summ.clnt_obj_id
--    AND ins.mngr_pers_obj_id <=> summ.mngr_pers_obj_id
--    AND ins.yr_cd <=> summ.yr_cd
--    AND ins.qtr_cd <=> summ.qtr_cd
--    AND ins.mnth_cd <=> summ.mnth_cd
--    AND ins.ins_rsn <=> summ.ins_rsn
--    AND ins.mtrc_ky <=> summ.mtrc_ky
--    AND (CASE WHEN ins.hr_orgn_id IS NOT NULL THEN "dept"
--              WHEN (ins.iso_3_char_cntry_cd IS NOT NULL  AND ins.state_prov_cd IS NOT NULL AND ins.work_loc_cd IS NULL) THEN "state"
--              WHEN (ins.iso_3_char_cntry_cd IS NOT NULL  AND ins.state_prov_cd IS NOT NULL AND ins.work_loc_cd IS NOT NULL) THEN "loc"
--              WHEN ins.job_cd IS NOT NULL THEN "job"
--              ELSE "trmn" END) <=> summ.grp
--    AND (CASE WHEN summ.grp = "state" THEN (ins.iso_3_char_cntry_cd = summ.iso_3_char_cntry_cd)
--              WHEN summ.grp = "loc" THEN (ins.iso_3_char_cntry_cd = summ.iso_3_char_cntry_cd AND ins.state_prov_cd = summ.state_prov_cd)
--              ELSE 1=1 END)
    WHERE
    --ins.environment = '${hiveconf:environment}'
    t_fact.ins_hash_val IS NULL
    AND (CASE WHEN clnt_obj_id='0F18AC8F6B800272' THEN hr_orgn_id ELSE NULL END) IS NULL
    --AND arch.ins_hash_val IS NULL
    AND ins.metric_value <> 0
    AND trim(ins.ins_rsn) != 'NO_INSIGHT'
    AND (ins.empl_cnt >= 3 OR ins.ins_rsn NOT LIKE '%PERCENTILE%' OR ins.mtrc_ky IN (74,63,56,73) )
    --AND ins.ins_type not in ('CLIENT_INTERNAL_MYTEAM','CLIENT_INTERNAL_BM_MYTEAM')
    AND ins.ins_rsn != 'PERCENTILE_RANKING'
    AND (CASE WHEN ins.ins_type = 'CLIENT_INTERNAL_BM' THEN ins.mngr_pers_obj_id ELSE 'DUMMY' END) IS NOT NULL
    AND (CASE WHEN ins.mtrc_ky = 73 THEN
                  CASE WHEN ins.ins_rsn IN ('MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') THEN ins.ins_rsn ELSE NULL END
              ELSE ins.ins_rsn END) IS NOT NULL;
--DISTRIBUTE BY environment, ins_rsn, ins_type;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_fact_ins_state_filt;

CREATE TABLE ${__BLUE_MAIN_DB__}.t_fact_ins_state_filt
USING PARQUET 
AS
SELECT * FROM ${__BLUE_MAIN_DB__}.tmp_t_fact_ins 
WHERE work_loc_cd IS NULL AND state_prov_cd IS NOT NULL 
AND STATE_PROV_CD=STATE_PROV_CD_CMPR_WITH;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.etl_tmp_t_fact_ins;

CREATE  TABLE  ${__BLUE_MAIN_DB__}.etl_tmp_t_fact_ins USING PARQUET as 
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
FROM
(SELECT
    ins.ins_hash_val,
    i.ins_hash_val as filter_ins_hash,
    ins.clnt_obj_id,
    ins.clnt_obj_id_cmpr_with,
    ins.yr_cd,
    ins.yr_cd_cmpr_with,
    ins.qtr_cd,
    ins.qtr_cd_cmpr_with,
    ins.mnth_cd,
    ins.mnth_cd_cmpr_with,
    ins.wk_cd,
    ins.wk_cd_cmpr_with,
    ins.flsa_stus_cd,
    ins.flsa_stus_cd_cmpr_with,
    ins.full_tm_part_tm_cd,
    ins.full_tm_part_tm_cd_cmpr_with,
    ins.gndr_cd,
    ins.gndr_cd_cmpr_with,
    ins.hr_orgn_id,
    ins.hr_orgn_id_cmpr_with,
    ins.job_cd,
    ins.job_cd_cmpr_with,
    ins.pay_rt_type_cd,
    ins.pay_rt_type_cd_cmpr_with,
    ins.reg_temp_cd,
    ins.reg_temp_cd_cmpr_with,
    ins.work_loc_cd,
    ins.work_loc_cd_cmpr_with,
    ins.city_id,
    ins.city_id_cmpr_with,
    ins.state_prov_cd,
    ins.state_prov_cd_cmpr_with,
    ins.iso_3_char_cntry_cd,
    ins.iso_3_char_cntry_cd_cmpr_with,
    ins.trmnt_rsn,
    ins.trmnt_rsn_cmpr_with,
    ins.adp_lense_cd,
    ins.adp_lense_cd_cmpr_with,
    ins.inds_cd,
    ins.inds_cd_cmpr_with,
    ins.sector_cd,
    ins.sector_cd_cmpr_with,
    ins.super_sect_cd,
    ins.super_sect_cd_cmpr_with,
    ins.mngr_pers_obj_id,
    ins.mngr_pers_obj_id_cmpr_with,
    ins.mtrc_ky,
    ins.ins_scor,
    ins.pctl_rank,
    ins.ins_type,
    ins.ins_rsn,
    ins.empl_cnt,
    ins.empl_cnt_cmpr_with,
    ins.pct_empl_cnt,
    ins.pct_empl_cnt_cmpr_with,
    ins.nbr_of_diments,
    ins.ins_json,
    ins.rec_crt_ts,
    ins.excp_type,
    ins.excp_type_cmpr_with,
    ins.supvr_pers_obj_id,
    ins.supvr_pers_obj_id_cmpr_with,
    ins.rpt_type,
    ins.db_schema,
    ins.environment
from ${__BLUE_MAIN_DB__}.tmp_t_fact_ins ins 
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.t_fact_ins_state_filt i 
ON  I.CLNT_OBJ_ID = INS.CLNT_OBJ_ID 
AND nvl(I.CLNT_OBJ_ID_CMPR_WITH,'UNKNOWN')=nvl(INS.CLNT_OBJ_ID_CMPR_WITH,'UNKNOWN')
AND nvl(I.YR_CD,'UNKNOWN')=nvl(INS.YR_CD,'UNKNOWN') AND nvl(I.YR_CD_CMPR_WITH,'UNKNOWN')=nvl(INS.YR_CD_CMPR_WITH,'UNKNOWN')
AND nvl(I.QTR_CD,'UNKNOWN')=nvl(INS.QTR_CD,'UNKNOWN') AND nvl(I.QTR_CD_CMPR_WITH,'UNKNOWN')=nvl(INS.QTR_CD_CMPR_WITH,'UNKNOWN')
AND nvl(I.MNTH_CD,'UNKNOWN')=nvl(INS.MNTH_CD,'UNKNOWN') AND nvl(I.MNTH_CD_CMPR_WITH,'UNKNOWN')=nvl(INS.MNTH_CD_CMPR_WITH,'UNKNOWN')
AND NVL(I.WORK_LOC_CD,'UNKNOWN')=NVL(INS.WORK_LOC_CD,'UNKNOWN') AND NVL(I.WORK_LOC_CD_CMPR_WITH,'UNKNOWN')=NVL(INS.WORK_LOC_CD_CMPR_WITH,'UNKNOWN')
AND nvl(I.STATE_PROV_CD,'UNKNOWN')=nvl(INS.STATE_PROV_CD,'UNKNOWN') AND nvl(I.STATE_PROV_CD_CMPR_WITH,'UNKNOWN')=nvl(INS.STATE_PROV_CD_CMPR_WITH,'UNKNOWN')
AND nvl(I.ISO_3_CHAR_CNTRY_CD,'UNKNOWN')=nvl(INS.ISO_3_CHAR_CNTRY_CD,'UNKNOWN') AND nvl(I.ISO_3_CHAR_CNTRY_CD_CMPR_WITH,'UNKNOWN')=nvl(INS.ISO_3_CHAR_CNTRY_CD_CMPR_WITH,'UNKNOWN')
AND nvl(I.MNGR_PERS_OBJ_ID,'UNKNOWN')=nvl(INS.MNGR_PERS_OBJ_ID,'UNKNOWN') AND I.MTRC_KY=INS.MTRC_KY
and i.environment=ins.environment and i.db_schema=ins.db_schema)final
where filter_ins_hash is null
union all
select 
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
from ${__BLUE_MAIN_DB__}.t_fact_ins_risk
;

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
${__BLUE_MAIN_DB__}.tmp_t_fact_ins_time_filter flt on ins.ins_hash_val=flt.ins_hash_val
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

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_fact_ins_staging PARTITION(environment)
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
    from ${__BLUE_MAIN_DB__}.tmp2_t_fact_ins;