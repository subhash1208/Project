-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_pr_pef_mngr (
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
    d_ot_ind TINYINT,
    flsa_excl_ind TINYINT,
    earn_amt DOUBLE, 
    mngr_pers_obj_id STRING,
    hrs_nbr DOUBLE,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_pr_pef_mngr PARTITION (environment)
SELECT 
        /*+ BROADCAST(dwh_t_dim_job,dwh_t_dim_work_loc) */
       pef.clnt_obj_id,
       pef.pers_obj_id,
       pef.qtr_cd,
       pef.mnth_cd,
       --wk_cd,
       CASE WHEN trim(pef.job_cd)='-' THEN 'UNKNOWN' ELSE  pef.job_cd END AS d_job_cd,
       CASE WHEN trim(pef.hr_orgn_id)='-'  THEN 'UNKNOWN' ELSE pef.hr_orgn_id END as d_hr_orgn_id,
       CASE WHEN trim(pef.work_loc_cd)='-' THEN 'UNKNOWN' ELSE pef.work_loc_cd END as d_work_loc_cd,
       CASE WHEN trim(worklocs.state_prov_cd)='-' THEN 'UNKNOWN' ELSE worklocs.state_prov_cd END AS d_work_state_cd,
       CASE WHEN trim(worklocs.city_id)='-' THEN 'UNKNOWN' ELSE worklocs.city_id END AS d_work_city_cd,
       CASE WHEN trim(pef.work_cntry_cd)='-' THEN  'UNKNOWN' ELSE pef.work_cntry_cd END as d_work_cntry_cd,
       pef.d_ot_ind,
       tdj.flsa_excl_ind as flsa_excl_ind,
       pef.earn_amt, 
       pef.mngr_pers_obj_id,
       pef.hrs_nbr,
       pef.db_schema,
       pef.yr_cd,
       pef.environment
  FROM
    (SELECT p.clnt_obj_id,
            p.pers_obj_id,
            p.qtr_cd,
            p.mnth_cd,
            --wk_cd,
            job_cd,
            hr_orgn_id,
            work_loc_cd,
            work_cntry_cd,
            d_ot_ind,
            earn_amt,
            hrs_nbr,
            p.login_mngr_pers_obj_id AS mngr_pers_obj_id,
            p.yr_cd,
            p.db_schema,
            p.environment
       FROM(
           SELECT 
           /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
            p.clnt_obj_id,
            p.environment,
            p.db_schema,
            p.pers_obj_id,
            yr_wk_cd,
            mnth_cd,
            qtr_cd,
            yr_cd,
            clndr_wk_cd,
            pay_grp_cd,
            earn_cd,
            strgt_tm_ind,
            p.mngr_pers_obj_id,
            rl.mngr_pers_obj_id as login_mngr_pers_obj_id,
            job_cd,
            work_loc_cd,
            work_loc_cntry_cd as work_cntry_cd,
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
        INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
            ON p.environment = rl.environment
            AND p.db_schema = rl.db_schema
            AND p.clnt_obj_id = rl.clnt_obj_id
            AND p.mngr_pers_obj_id = rl.pers_obj_id
            AND rl.mngr_pers_obj_id IS NOT NULL
        WHERE p.check_dt BETWEEN rl.rec_eff_strt_dt AND rl.rec_eff_end_dt 
        GROUP BY 
            p.clnt_obj_id,
            p.environment,
            p.db_schema,
            p.pers_obj_id,
            yr_wk_cd,
            mnth_cd,
            qtr_cd,
            yr_cd,
            clndr_wk_cd,
            pay_grp_cd,
            earn_cd,
            strgt_tm_ind,
            p.mngr_pers_obj_id,
            rl.mngr_pers_obj_id,
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
      
      UNION ALL

      SELECT p.clnt_obj_id,
            p.pers_obj_id,
            p.qtr_cd,
            p.mnth_cd,
            --wk_cd,
            job_cd,
            hr_orgn_id,
            work_loc_cd,
            work_cntry_cd,
            d_ot_ind,
            earn_amt,
            hrs_nbr,
            p.login_mngr_pers_obj_id AS mngr_pers_obj_id,
            p.yr_cd,
            p.db_schema,
            p.environment
       FROM (
            SELECT 
                 /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
                p.clnt_obj_id,
                p.environment,
                p.db_schema,
                p.pers_obj_id,
                yr_wk_cd,
                mnth_cd,
                qtr_cd,
                yr_cd,
                clndr_wk_cd,
                pay_grp_cd,
                earn_cd,
                strgt_tm_ind,
                p.mngr_pers_obj_id,
                rl.mngr_pers_obj_id as login_mngr_pers_obj_id,
                job_cd,
                work_loc_cd,
                work_loc_cntry_cd as work_cntry_cd,
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
            INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
                ON p.environment = rl.environment
                AND p.db_schema = rl.db_schema
                AND p.clnt_obj_id = rl.clnt_obj_id
                AND p.pers_obj_id = rl.pers_obj_id
                AND mtrx_hrchy_ind = 1
                AND rl.mngr_pers_obj_id IS NOT NULL
            GROUP BY 
                p.clnt_obj_id,
                p.environment,
                p.db_schema,
                p.pers_obj_id,
                yr_wk_cd,
                mnth_cd,
                qtr_cd,
                yr_cd,
                clndr_wk_cd,
                pay_grp_cd,
                earn_cd,
                strgt_tm_ind,
                p.mngr_pers_obj_id,
                rl.mngr_pers_obj_id,
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

       )pef
       LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_job tdj
        ON  pef.clnt_obj_id = tdj.clnt_obj_id
            AND pef.db_schema = tdj.db_schema
            AND pef.job_cd = tdj.job_cd
            AND pef.environment = tdj.environment
       LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_loc worklocs
            ON pef.clnt_obj_id  = worklocs.clnt_obj_id
            AND pef.work_loc_cd = worklocs.work_loc_cd
            AND pef.db_schema = worklocs.db_schema
            AND pef.environment = worklocs.environment
      WHERE 
        --pef.environment = '${environment}'
         yr_cd >= cast(year(add_months(current_date, -24)) as string);

-- Temporary table which will hold which persons are already captured as a part of manager hierarchy
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_pef;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_pef 
USING PARQUET 
AS
SELECT
    DISTINCT waf.clnt_obj_id,
    waf.pers_obj_id,
    rl.mngr_pers_obj_id,   
    waf.environment,
    waf.db_schema,
    waf.yr_cd,
    waf.qtr_cd,
    waf.mnth_cd
FROM
    ${__BLUE_MAIN_DB__}.emi_base_pr_pef waf
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
            ON waf.environment = rl.environment
            AND waf.db_schema = rl.db_schema
            AND waf.clnt_obj_id = rl.clnt_obj_id
            AND waf.mngr_pers_obj_id = rl.pers_obj_id
            AND rl.mngr_pers_obj_id IS NOT NULL
WHERE waf.check_dt BETWEEN rl.rec_eff_strt_dt  AND rl.rec_eff_end_dt;


-- Cherry-pick records
INSERT INTO TABLE ${__BLUE_MAIN_DB__}.emi_prep_pr_pef_mngr PARTITION (environment)
SELECT 
       /*+ BROADCAST(dwh_t_dim_mngr_cherrypick,tmp_emi_hrchy_mngrs_pef) */
       pef.clnt_obj_id,
       pef.pers_obj_id,
       pef.qtr_cd,
       pef.mnth_cd,
       --wk_cd,
       CASE WHEN trim(job_cd)='-' THEN 'UNKNOWN' ELSE  job_cd END AS d_job_cd,
       CASE WHEN trim(hr_orgn_id)='-'  THEN 'UNKNOWN' ELSE hr_orgn_id END as d_hr_orgn_id,
       CASE WHEN trim(work_loc_cd)='-' THEN 'UNKNOWN' ELSE work_loc_cd END as d_work_loc_cd,
       CASE WHEN trim(work_state_cd)='-' THEN 'UNKNOWN' ELSE work_state_cd END AS d_work_state_cd,
       CASE WHEN trim(work_city_cd)='-' THEN 'UNKNOWN' ELSE work_city_cd END AS d_work_city_cd,
       CASE WHEN trim(work_cntry_cd)='-' THEN  'UNKNOWN' ELSE work_cntry_cd END as d_work_cntry_cd,
       d_ot_ind,
       flsa_excl_ind,
       earn_amt, 
       rl.mngr_pers_obj_id,
       hrs_nbr,
       pef.db_schema,
       pef.yr_cd,
       pef.environment
  FROM
    (SELECT 
            /*+ BROADCAST(dwh_t_dim_job,dwh_t_dim_work_loc) */
            p.clnt_obj_id,
            pers_obj_id,
            qtr_cd,
            mnth_cd,
            --wk_cd,
            p.job_cd,
            hr_orgn_id,
            p.work_loc_cd,
            worklocs.state_prov_cd     AS work_state_cd,
            worklocs.city_id      AS work_city_cd,
            p.work_loc_cntry_cd as work_cntry_cd,
            d_ot_ind,
            tdj.flsa_excl_ind as flsa_excl_ind,
            earn_amt,
            hrs_nbr,
            p.mngr_pers_obj_id,
            work_asgnmt_nbr,
            p.yr_cd,
            p.db_schema,
            p.environment
       FROM ${__BLUE_MAIN_DB__}.emi_base_pr_pef p
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
        AND mngr_pers_obj_id IS NOT NULL 
        AND mngr_pers_obj_id != 'UNKNOWN'
        ) pef
      INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_mngr_cherrypick rl
        ON pef.environment = rl.environment
        AND pef.db_schema = rl.db_schema
        AND pef.clnt_obj_id = rl.clnt_obj_id
        AND pef.pers_obj_id = rl.pers_obj_id 
        AND pef.work_asgnmt_nbr = rl.work_asgnmt_nbr
      LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_pef prev
            ON pef.environment = prev.environment
            AND pef.db_schema = prev.db_schema
            AND pef.clnt_obj_id = prev.clnt_obj_id
            AND pef.pers_obj_id = prev.pers_obj_id
            AND rl.mngr_pers_obj_id = prev.mngr_pers_obj_id
            AND pef.yr_cd = prev.yr_cd
            AND pef.qtr_cd = prev.qtr_cd
            AND pef.mnth_cd = prev.mnth_cd
            -- to exclude cases where the manager is included as a part of the reporting hierarchy
      WHERE prev.mngr_pers_obj_id IS NULL;

--DROP TABLE ${__GREEN_MAIN_DB__}.tmp_emi_hrchy_mngrs_pef;
