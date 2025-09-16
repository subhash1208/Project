-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_wef (
    clnt_obj_id STRING,
    d_cmpny_cd STRING,
    d_flsa_stus_cd STRING,
    pers_stus_cd STRING,
    d_full_tm_part_tm_cd STRING,
    d_gndr_cd STRING,
    d_hr_orgn_id STRING,
    d_job_cd STRING,
    d_pay_rt_type_cd STRING,
    d_reg_temp_cd STRING,
    d_work_loc_cd STRING,
    d_work_state_cd STRING,
    d_work_city_cd STRING,
    d_work_cntry_cd STRING,
    mngr_pers_obj_id STRING,
    mnth_cd STRING,
    pers_obj_id STRING,
    qtr_cd STRING,
    event_cd STRING,
    event_rsn_cd STRING,
    d_trmnt_rsn STRING,
    months_between_promotion DOUBLE,
    promo_ind DOUBLE,
    trmnt_ind DOUBLE,
    vlntry_trmnt_ind DOUBLE,
    invlntry_trmnt_ind DOUBLE,
    avdble_trmnt_ind DOUBLE,
    hir_ind DOUBLE,
    new_hir_ind DOUBLE,
    int_mob_hir_ind DOUBLE,
    reh_ind DOUBLE,
    retirement_ind DOUBLE,
    dem_ind DOUBLE,
    xfr_ind DOUBLE,
    flsa_excl_ind INT,
    pay_grp_excl_ind INT,
    int_mobility_rate_ind INT,
    work_asgnmt_nbr STRING,
    work_asgnmt_stus_cd STRING,
    yr_wk_cd STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

--DROP TABLE IF EXISTS ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_wef;
--
--CREATE TABLE ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_wef
--USING PARQUET
--AS
--SELECT environment, min(yr_cd) as last_load_clndr_yr_cd
--FROM 
--    (SELECT environment, clnt_obj_id, max(yr_cd) as yr_cd 
--    FROM ${__GREEN_MAIN_DB__}.emi_prep_hr_wef 
--    --WHERE environment = '${environment}'
--    GROUP BY environment, clnt_obj_id) IGN
--GROUP BY environment;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_hr_wef PARTITION(environment)
SELECT 
    /*+ COALESCE(800) */
    clnt_obj_id,
    d_cmpny_cd,
    d_flsa_stus_cd,
    pers_stus_cd,
    d_full_tm_part_tm_cd,
    d_gndr_cd,
    CASE WHEN trim(d_hr_orgn_id)='-' THEN 'UNKNOWN' ELSE  d_hr_orgn_id END AS d_hr_orgn_id,
    CASE WHEN trim(d_job_cd)='-' THEN 'UNKNOWN' ELSE  d_job_cd END AS d_job_cd,
    d_pay_rt_type_cd,
    d_reg_temp_cd,
    CASE WHEN trim(d_work_loc_cd)='-' THEN 'UNKNOWN' ELSE  d_work_loc_cd END AS d_work_loc_cd,
    CASE WHEN trim(d_work_state_cd)='-' THEN 'UNKNOWN' ELSE  d_work_state_cd END AS d_work_state_cd,
    CASE WHEN trim(d_work_city_cd)='-' THEN 'UNKNOWN' ELSE  d_work_city_cd END AS d_work_city_cd,
    CASE WHEN trim(d_work_cntry_cd)='-' THEN 'UNKNOWN' ELSE  d_work_cntry_cd END AS d_work_cntry_cd,
    mngr_pers_obj_id,
    mnth_cd,
    pers_obj_id,
    qtr_cd,
    event_cd,
    event_rsn_cd,
    CASE WHEN event_cd = 'TER' THEN (CASE WHEN trim(event_rsn_cd)='-' THEN 'UNKNOWN' ELSE  event_rsn_cd END) ELSE NULL END as d_trmnt_rsn,
    months_between_promotion,
    promo_ind,
    trmnt_ind,
    vlntry_trmnt_ind,
    invlntry_trmnt_ind,
    avdble_trmnt_ind,
    hir_ind,
    new_hir_ind,
    int_mob_hir_ind,
    reh_ind,
    retirement_ind,
    dem_ind,
    xfr_ind,
    flsa_excl_ind,
    pay_grp_excl_ind,
    int_mobility_rate_ind,
    work_asgnmt_nbr,
    work_asgnmt_stus_cd,
    yr_wk_cd,
    db_schema,
    yr_cd,
    environment
FROM 
    (SELECT
        /*+ BROADCAST(dwh_t_dim_job,dwh_t_dim_work_loc) */
        wef.clnt_obj_id,
        wef.d_cmpny_cd,
        tdj.flsa_stus_cd as d_flsa_stus_cd,
        pers_stus_cd,
        wef.d_full_tm_part_tm_cd,
        wef.d_gndr_cd,
        wef.d_hr_orgn_id,
        wef.d_job_cd,
        wef.d_pay_rt_type_cd,
        wef.d_reg_temp_cd,
        wef.d_work_loc_cd,
        worklocs.state_prov_cd AS d_work_state_cd,
        worklocs.city_id AS d_work_city_cd,
        wef.d_work_cntry_cd,
        wef.mngr_pers_obj_id,
        wef.work_asgnmt_nbr,
        wef.work_asgnmt_stus_cd,
        wef.mnth_cd,
        wef.pers_obj_id,
        wef.qtr_cd,
        wef.event_cd,
        wef.event_rsn_cd,
        CASE WHEN wef.event_cd = 'PRO' 
        THEN months_between(wef.event_eff_dt, NVL(wef.lst_promo_dt, wef.event_eff_dt)) 
        ELSE NULL END AS months_between_promotion,
        promo_ind,
        trmnt_ind,
        vlntry_trmnt_ind,
        invlntry_trmnt_ind,
        avdble_trmnt_ind,
        hir_ind,
        new_hir_ind,
        int_mob_hir_ind,
        reh_ind,
        retirement_ind,
        dem_ind,
        xfr_ind,
        wef.environment,
        wef.db_schema,
        wef.yr_cd,
        wef.yr_wk_cd,
        pay_grp_excl_ind,
        CASE WHEN(wef.event_cd = 'PRO') THEN 1
             WHEN(wef.event_cd = 'DEM') THEN 1
             WHEN(wef.event_cd = 'XFR') THEN 1
             WHEN(wef.event_cd = 'HIR') THEN 1
             WHEN(wef.event_cd = 'REH') THEN 1
             WHEN(wef.event_cd = 'TER') THEN 1
             WHEN(wef.event_cd = 'RET') THEN 1 ELSE 0 END AS int_mobility_rate_ind,
        tdj.flsa_excl_ind,
        row_number() OVER (PARTITION BY wef.db_schema, 
            wef.clnt_obj_id,
            wef.pers_obj_id,
            yr_wk_cd,
            wef.event_cd,
            wef.event_rsn_cd,
            event_eff_dt ORDER BY event_eff_dt DESC) AS ranking
       FROM ${__BLUE_MAIN_DB__}.emi_base_hr_wef wef       
        LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_job tdj
            ON  wef.clnt_obj_id = tdj.clnt_obj_id
            AND wef.db_schema = tdj.db_schema
            AND wef.d_job_cd = tdj.job_cd
            AND wef.environment = tdj.environment
        LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_loc worklocs
            ON wef.clnt_obj_id  = worklocs.clnt_obj_id
            AND wef.d_work_loc_cd = worklocs.work_loc_cd
            AND wef.db_schema = worklocs.db_schema
            AND wef.environment = worklocs.environment
        WHERE 
        wef.yr_cd >= (YEAR(CURRENT_DATE()) - 3)
    ) ignored
WHERE
    ranking = 1;