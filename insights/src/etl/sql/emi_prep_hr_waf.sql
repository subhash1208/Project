-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_waf;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_waf(
    act_wk_end_dt TIMESTAMP,
    act_wk_strt_dt TIMESTAMP,
    birth_yr DOUBLE,
    clnt_obj_id STRING,
    d_cmpny_cd STRING,
    d_eeo1_job_catg_cd STRING,
    d_eeo1_job_catg_dsc STRING,
    d_eeo_ethncty_clsfn_dsc STRING,
    d_flsa_stus_cd STRING,
    d_flsa_stus_dsc STRING,
    d_full_tm_part_tm_cd STRING,
    d_eeo_ethncty_clsfn_cd INT,
    d_gndr_cd STRING,
    d_hr_orgn_id STRING,
    d_job_cd STRING,
    d_martl_stus_cd STRING,
    d_pay_rt_type_cd STRING,
    d_reg_temp_cd STRING,
    d_reg_temp_dsc STRING,
    d_work_loc_cd STRING,
    d_work_state_cd STRING,
    d_work_city_cd STRING,
    d_work_cntry_cd STRING,
    ee_range_band_ky DOUBLE,
    eff_person_days DOUBLE,
    f_hr_annl_cmpn_amt DOUBLE,
    f_hrly_cmpn_amt DOUBLE,
    f_compa_rt DOUBLE,
    f_work_asgnmt_actv_ind DOUBLE,
    f_work_asgnmt_stus_cd STRING,
    fwa_rec_eff_end_dt TIMESTAMP,
    fwa_rec_eff_strt_dt TIMESTAMP,
    inds_ky DOUBLE,
    ltst_hire_dt TIMESTAMP,
    mngr_pers_obj_id STRING,
    supvr_pers_obj_id STRING,
    mnth_cd STRING,
    pers_obj_id STRING,
    qtr_cd STRING,
    rec_eff_end_dt TIMESTAMP,
    rec_eff_strt_dt TIMESTAMP,
    rev_range_band_ky DOUBLE,
    tnur_dt TIMESTAMP,
    tnur DOUBLE,
    --is_manager INT,
    flsa_excl_ind INT,
    pay_grp_excl_ind STRING,
    yr_end_leave_ind INT,
    qtr_end_leave_ind INT,
    mnth_end_leave_ind INT,  
    is_active_in_yr_end INT,    
    is_active_in_qtr_end INT,    
    is_active_in_mnth_end INT,
    yr_end_dt TIMESTAMP,
    qtr_end_dt TIMESTAMP,
    mnth_end_dt TIMESTAMP,
    work_asgnmt_nbr STRING,
    yr_wk_cd STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

--DROP TABLE IF EXISTS ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_waf;
--
--CREATE TABLE ${__GREEN_MAIN_DB__}.tmp_emi_clnt_last_load_yr_waf
--USING PARQUET
--AS
--SELECT environment, min(yr_cd) as last_load_clndr_yr_cd
--FROM 
--    (SELECT environment, clnt_obj_id, max(yr_cd) as yr_cd 
--    FROM ${__GREEN_MAIN_DB__}.emi_prep_hr_waf
--    --WHERE environment = '${environment}'
--    GROUP BY environment, clnt_obj_id) IGN
--GROUP BY environment;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_hr_waf PARTITION(environment)
SELECT 
    /*+ COALESCE(800) */
    act_wk_end_dt,
    act_wk_strt_dt,
    birth_yr,
    clnt_obj_id,
    d_cmpny_cd,
    d_eeo1_job_catg_cd,
    d_eeo1_job_catg_dsc,
    d_eeo_ethncty_clsfn_dsc,
    d_flsa_stus_cd,
    d_flsa_stus_dsc,
    d_full_tm_part_tm_cd,
    d_eeo_ethncty_clsfn_cd,
    d_gndr_cd,
    CASE WHEN trim(d_hr_orgn_id)='-' THEN 'UNKNOWN' ELSE  d_hr_orgn_id END AS d_hr_orgn_id,
    CASE WHEN trim(d_job_cd)='-' THEN 'UNKNOWN' ELSE  d_job_cd END AS d_job_cd,
    d_martl_stus_cd,
    d_pay_rt_type_cd,
    d_reg_temp_cd,
    d_reg_temp_dsc,
    CASE WHEN trim(d_work_loc_cd)='-' THEN 'UNKNOWN' ELSE  d_work_loc_cd END AS d_work_loc_cd,
    CASE WHEN trim(d_work_state_cd)='-' THEN 'UNKNOWN' ELSE  d_work_state_cd END AS d_work_state_cd,
    CASE WHEN trim(d_work_city_cd)='-' THEN 'UNKNOWN' ELSE  d_work_city_cd END AS d_work_city_cd,
    CASE WHEN trim(d_work_cntry_cd)='-' THEN 'UNKNOWN' ELSE  d_work_cntry_cd END AS d_work_cntry_cd,
    ee_range_band_ky,
    eff_person_days,
    f_hr_annl_cmpn_amt,
    f_hrly_cmpn_amt,
    f_compa_rt,
    f_work_asgnmt_actv_ind,
    f_work_asgnmt_stus_cd,
    fwa_rec_eff_end_dt,
    fwa_rec_eff_strt_dt,
    inds_ky,
    ltst_hire_dt,
    mngr_pers_obj_id,
    supvr_pers_obj_id,
    mnth_cd,
    pers_obj_id,
    qtr_cd,
    rec_eff_end_dt,
    rec_eff_strt_dt,
    rev_range_band_ky,
    tnur_dt,
    tnur,
    --is_manager,
    flsa_excl_ind,
    pay_grp_excl_ind,
    yr_end_leave_ind,
    qtr_end_leave_ind,
    mnth_end_leave_ind,
    is_active_in_yr_end,
    is_active_in_qtr_end,
    is_active_in_mnth_end,
    yr_end_dt,
    qtr_end_dt,
    mnth_end_dt,
    work_asgnmt_nbr,
    yr_wk_cd,
    db_schema,
    yr_cd,
    environment
FROM 
    (SELECT
        /*+ BROADCAST(dwh_t_dim_job,dwh_t_dim_pay_grp,dwh_t_dim_work_loc) */
        act_wk_end_dt,
        act_wk_strt_dt,
        birth_yr,
        waf.work_asgnmt_nbr,
        waf.clnt_obj_id,
        d_cmpny_cd,
        tdj.eeo1_job_catg_cd AS d_eeo1_job_catg_cd,
        CASE
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%exec%'
             THEN 'Executive/Senior Level Officials and Managers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%first%'
             THEN 'First/Mid Level Officials and Managers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%profes%'
             THEN 'Professionals'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%technician%'
             THEN 'Technicians'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%sales%'
             THEN 'Sales Workers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%support%'
             THEN 'Administrative Support Workers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%admin%'
             THEN 'Administrative Support Workers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%admn%'
             THEN 'Administrative Support Workers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%craft%'
             THEN 'Craft Workers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%operative%'
             THEN 'Operatives'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%labor%'
             THEN 'Laborers and Helpers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%labour%'
             THEN 'Laborers and Helpers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%help%'
             THEN 'Laborers and Helpers'
             WHEN lower(tdj.eeo1_job_catg_dsc) LIKE '%service%'
             THEN 'Service Workers'
             ELSE NULL
        END AS d_eeo1_job_catg_dsc,
        d_eeo_ethncty_clsfn_dsc,
        tdj.flsa_stus_cd AS d_flsa_stus_cd,
        CASE
            WHEN tdj.flsa_stus_cd = 'N'
            OR UPPER(tdj.flsa_stus_dsc) LIKE '%NON%EXEMPT%'
            THEN 'NONEXEMPT'
            ELSE 'EXEMPT'
        END             AS d_flsa_stus_dsc,
        d_full_tm_part_tm_cd,
        cast(d_eeo_ethncty_clsfn_cd as INT) as d_eeo_ethncty_clsfn_cd,
        d_gndr_cd,
        d_hr_orgn_id,
        d_job_cd,
        d_martl_stus_cd,
        d_pay_rt_type_cd,
        d_reg_temp_cd,
        d_reg_temp_dsc,
        d_work_loc_cd,
        worklocs.state_prov_cd AS d_work_state_cd,
        worklocs.city_id AS d_work_city_cd,
        d_work_cntry_cd,
        waf.ee_range_band_ky,
        eff_person_days,
        f_hr_annl_cmpn_amt,
        f_hrly_cmpn_amt,
        f_compa_rt,
        f_work_asgnmt_actv_ind,
        f_work_asgnmt_stus_cd,
        fwa_rec_eff_end_dt,
        fwa_rec_eff_strt_dt,
        waf.inds_ky,
        ltst_hire_dt,
        waf.mngr_pers_obj_id,
        waf.supvr_pers_obj_id,
        mnth_cd,
        waf.pers_obj_id,
        qtr_cd,
        rec_eff_end_dt,
        rec_eff_strt_dt,
        waf.rev_range_band_ky,
        waf.tnur_dt,
        CASE WHEN to_date(act_wk_end_dt) <> to_date(waf.yr_end_dt)
                THEN NULL
             ELSE
                MONTHS_BETWEEN(waf.yr_end_dt,waf.tnur_dt)/12
         END AS tnur,
        yr_end_dt,
        qtr_end_dt,
        mnth_end_dt,
        waf.environment,
        waf.db_schema,
        yr_cd,
        yr_wk_cd,
        CASE WHEN ((CAST(waf.rec_eff_end_dt AS DATE) = CAST(yr_end_dt AS DATE)) AND f_work_asgnmt_stus_cd in ('L','P')) THEN 1 ELSE 0 END AS yr_end_leave_ind,
        CASE WHEN ((CAST(waf.rec_eff_end_dt AS DATE) = CAST(qtr_end_dt AS DATE)) AND f_work_asgnmt_stus_cd in ('L','P')) THEN 1 ELSE 0 END AS qtr_end_leave_ind,
        CASE WHEN ((CAST(waf.rec_eff_end_dt AS DATE) = CAST(mnth_end_dt AS DATE)) AND f_work_asgnmt_stus_cd in ('L','P')) THEN 1 ELSE 0 END AS mnth_end_leave_ind,
        CASE WHEN CAST(waf.rec_eff_end_dt AS DATE) = CAST(yr_end_dt AS DATE)
                THEN 1 ELSE 0 END AS is_active_in_yr_end,
        CASE WHEN CAST(waf.rec_eff_end_dt AS DATE) = CAST(qtr_end_dt AS DATE)
                THEN 1 ELSE 0 END AS is_active_in_qtr_end,
        CASE WHEN CAST(waf.rec_eff_end_dt AS DATE) = CAST(mnth_end_dt AS DATE)
                THEN 1 ELSE 0 END AS is_active_in_mnth_end,
        --CASE WHEN (rl.pers_obj_id IS NULL AND rl_eff.pers_obj_id IS NULL) THEN 0 ELSE 1 END as is_manager,
        tdj.flsa_excl_ind,
        tpg.pay_grp_excl_ind
    FROM
         ${__BLUE_MAIN_DB__}.emi_base_hr_waf waf
        LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_job tdj
        ON 
            waf.clnt_obj_id = tdj.clnt_obj_id
            AND waf.db_schema = tdj.db_schema
            AND waf.d_job_cd = tdj.job_cd
            AND waf.environment = tdj.environment
        LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pay_grp tpg
        ON
            waf.clnt_obj_id = tpg.clnt_obj_id 
            AND waf.db_schema = tpg.db_schema
            AND waf.d_pay_grp_cd = tpg.pay_grp_cd
            AND waf.environment = tpg.environment
        LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_loc worklocs
            ON waf.clnt_obj_id  = worklocs.clnt_obj_id
            AND waf.d_work_loc_cd = worklocs.work_loc_cd
            AND waf.db_schema = worklocs.db_schema
            AND waf.environment = worklocs.environment
        WHERE 
            --waf.environment = '${environment}'
            waf.prmry_work_asgnmt_ind=1
            AND waf.yr_cd >= (YEAR(CURRENT_DATE()) - 3)
        ) ignored;