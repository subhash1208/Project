-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_pr_pef (
    clnt_obj_id STRING,
    pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    --wk_cd STRING,
    d_job_cd STRING,
    d_hr_orgn_id STRING,
    d_work_loc_cd STRING,
    d_work_state_cd STRING,
    d_work_city_cd STRING,
    d_work_cntry_cd STRING,
    --d_gndr_cd STRING,
    d_ot_ind TINYINT,
    flsa_excl_ind TINYINT,
    earn_amt DOUBLE,
    hrs_nbr DOUBLE,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_pr_pef PARTITION (environment)
SELECT 
       /*+ COALESCE(800) */
       pef.clnt_obj_id,
       pef.pers_obj_id,
       pef.qtr_cd,
       pef.mnth_cd,
       --wk_cd,
       d_job_cd,
       d_hr_orgn_id,
       d_work_loc_cd,
       d_work_state_cd,
       d_work_city_cd,
       d_work_cntry_cd,
       --d_gndr_cd,
       d_ot_ind,
       flsa_excl_ind,
       earn_amt,
       hrs_nbr,
       pef.db_schema,
       pef.yr_cd,
       pef.environment
  FROM
    (SELECT 
            /*+ BROADCAST(dwh_t_dim_job,dwh_t_dim_work_loc) */
            p.clnt_obj_id,
            p.pers_obj_id,
            p.qtr_cd,
            p.mnth_cd,
            --wk_cd,
            CASE WHEN trim(p.job_cd)='-' THEN 'UNKNOWN' ELSE  p.job_cd END AS d_job_cd,
            CASE WHEN trim(p.hr_orgn_id)='-'  THEN 'UNKNOWN' ELSE p.hr_orgn_id END as d_hr_orgn_id,
            CASE WHEN trim(p.work_loc_cd)='-' THEN 'UNKNOWN' ELSE p.work_loc_cd END as d_work_loc_cd,
            CASE WHEN trim(worklocs.state_prov_cd)='-' THEN 'UNKNOWN' ELSE worklocs.state_prov_cd END AS d_work_state_cd,
            CASE WHEN trim(worklocs.city_id)='-' THEN 'UNKNOWN' ELSE worklocs.city_id END AS d_work_city_cd,
            CASE WHEN trim(p.work_cntry_cd)='-' THEN  'UNKNOWN' ELSE p.work_cntry_cd END as d_work_cntry_cd,
            --d_gndr_cd,
            d_ot_ind,
            tdj.flsa_excl_ind as flsa_excl_ind,
            earn_amt,
            hrs_nbr,
            mngr_pers_obj_id,
            yr_cd,
            p.db_schema,
            p.environment
       FROM 
       (
        SELECT 
            clnt_obj_id,
            environment,
            db_schema,
            pers_obj_id,
            yr_wk_cd,
            mnth_cd,
            qtr_cd,
            yr_cd,
            clndr_wk_cd,
            pay_grp_cd,
            earn_cd,
            strgt_tm_ind,
            mngr_pers_obj_id,
            job_cd,
            work_loc_cd,
            work_loc_cntry_cd AS work_cntry_cd,
            hr_orgn_id,
            full_tm_part_tm_cd AS d_full_tm_part_tm_cd,
            reg_temp_cd AS d_reg_temp_cd,
            hr_cmpn_freq_cd,
            pay_rt_type_cd AS d_pay_rt_type_cd,
            work_asgnmt_nbr,
            cmpny_cd,
            pers_clsfn_cd,
            SUM(hrs_nbr)  AS hrs_nbr,
            SUM(earn_amt) AS earn_amt,
            AVG(hrly_rt)  AS hrly_rt, -- APPROXIMATE. FAULTY.
            MAX(d_ot_ind) AS d_ot_ind
        FROM ${__BLUE_MAIN_DB__}.emi_base_pr_pef p
        GROUP BY 
            clnt_obj_id,
            environment,
            db_schema,
            pers_obj_id,
            yr_wk_cd,
            mnth_cd,
            qtr_cd,
            yr_cd,
            clndr_wk_cd,
            pay_grp_cd,
            earn_cd,
            strgt_tm_ind,
            mngr_pers_obj_id,
            job_cd,
            work_loc_cd,
            work_loc_cntry_cd,
            hr_orgn_id,
            full_tm_part_tm_cd,
            reg_temp_cd,
            work_asgnmt_nbr,
            cmpny_cd,
            pers_clsfn_cd,
            hr_cmpn_freq_cd,
            pay_rt_type_cd
       )p
       LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_job tdj
        ON  p.clnt_obj_id = tdj.clnt_obj_id
            AND p.db_schema = tdj.db_schema
            AND p.job_cd = tdj.job_cd
            AND p.environment = tdj.environment
       LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_loc worklocs
            ON p.clnt_obj_id  = worklocs.clnt_obj_id
            AND p.work_loc_cd = worklocs.work_loc_cd
            AND p.db_schema = worklocs.db_schema
            AND p.environment = worklocs.environment
      WHERE 
       --p.environment = '${environment}'
      yr_cd >= cast(year(add_months(current_date, -24)) as string)
        ) pef;