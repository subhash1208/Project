-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_mngr;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_mngr (
    clnt_obj_id STRING,
    pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    d_job_cd STRING,
    d_hr_orgn_id STRING,
    d_work_loc_cd STRING,
    d_work_state_cd STRING,
    d_work_city_cd STRING,
    d_work_cntry_cd STRING,
    d_gndr_cd STRING,  
    f_work_asgnmt_stus_cd STRING,
    f_work_asgnmt_actv_ind INT,
    d_full_tm_part_tm_cd STRING,
    f_compa_rt DOUBLE,
    rec_eff_strt_dt TIMESTAMP,
    rec_eff_end_dt TIMESTAMP,
    d_reg_temp_cd STRING,
    eff_person_days DOUBLE,
    tnur DOUBLE,
    mngr_pers_obj_id STRING,
    --is_manager INT,
    yr_end_leave_ind INT,
    qtr_end_leave_ind INT,
    mnth_end_leave_ind INT,
    is_active_in_yr_end INT,
    is_active_in_qtr_end INT,
    is_active_in_mnth_end INT,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_mngr PARTITION(environment)
SELECT
       clnt_obj_id,
       pers_obj_id,
       qtr_cd,
       mnth_cd,
       CASE WHEN trim(d_job_cd)='-' THEN 'UNKNOWN' ELSE  d_job_cd END AS d_job_cd,
       CASE WHEN trim(d_hr_orgn_id)='-' THEN 'UNKNOWN' ELSE  d_hr_orgn_id END AS d_hr_orgn_id,
       CASE WHEN trim(d_work_loc_cd)='-' THEN 'UNKNOWN' ELSE  d_work_loc_cd END AS d_work_loc_cd,
       CASE WHEN trim(d_work_state_cd)='-' THEN 'UNKNOWN' ELSE  d_work_state_cd END AS d_work_state_cd,
       CASE WHEN trim(d_work_city_cd)='-' THEN 'UNKNOWN' ELSE  d_work_city_cd END AS d_work_city_cd,
       CASE WHEN trim(d_work_cntry_cd)='-' THEN 'UNKNOWN' ELSE  d_work_cntry_cd END AS d_work_cntry_cd,
       d_gndr_cd,    
       f_work_asgnmt_stus_cd,
       f_work_asgnmt_actv_ind,
       d_full_tm_part_tm_cd,
       f_compa_rt,
       rec_eff_strt_dt,
       rec_eff_end_dt,
       d_reg_temp_cd,
       eff_person_days,
       tnur,
       mngr_pers_obj_id,
       --is_manager,
       yr_end_leave_ind,
       qtr_end_leave_ind,
       mnth_end_leave_ind,
       is_active_in_yr_end,
       is_active_in_qtr_end,
       is_active_in_mnth_end,
       db_schema,
       yr_cd,
       environment
  FROM
      (    SELECT 
              waf.clnt_obj_id,
              waf.pers_obj_id,
              waf.qtr_cd,
              waf.mnth_cd,
              waf.d_job_cd,
              waf.d_hr_orgn_id,
              waf.d_work_loc_cd,
              waf.d_work_state_cd,
              waf.d_work_city_cd,
              waf.d_work_cntry_cd,
              waf.d_gndr_cd,
              waf.f_work_asgnmt_stus_cd,
              waf.f_work_asgnmt_actv_ind,
              waf.d_full_tm_part_tm_cd,
              waf.f_compa_rt,
              waf.rec_eff_strt_dt,
              waf.rec_eff_end_dt,
              waf.d_reg_temp_cd,
              waf.eff_person_days,
              waf.tnur,
              login_mngr_pers_obj_id  AS mngr_pers_obj_id,
              --waf.is_manager,
              yr_end_leave_ind,
              qtr_end_leave_ind,
              mnth_end_leave_ind,
              is_active_in_mnth_end,
              is_active_in_qtr_end,
              is_active_in_yr_end,
              waf.environment,
              waf.db_schema,
              waf.yr_cd
         FROM (SELECT 
                      environment,
                      db_schema,
                      clnt_obj_id,
                      pers_obj_id,
                      yr_cd,
                      qtr_cd,
                      mnth_cd,
                      d_job_cd,
                      d_hr_orgn_id,
                      d_work_loc_cd,
                      d_work_state_cd,
                      d_work_city_cd,
                      d_work_cntry_cd,
                      d_gndr_cd,
                      f_work_asgnmt_stus_cd,
                      f_work_asgnmt_actv_ind,
                      d_full_tm_part_tm_cd,
                      d_reg_temp_cd,
                      mngr_pers_obj_id,
                      login_mngr_pers_obj_id,
                      --max(is_manager) as is_manager,
                      sum(eff_person_days) AS eff_person_days,
                      max(tnur) AS tnur,
                      max(f_compa_rt) AS f_compa_rt,
                      min(rec_eff_strt_dt) AS rec_eff_strt_dt,
                      max(rec_eff_end_dt) AS rec_eff_end_dt,
                      sum(eff_hrch_yr_end_leave_ind) AS yr_end_leave_ind,
                      sum(eff_hrch_qtr_end_leave_ind) AS qtr_end_leave_ind,
                      sum(eff_hrch_mnth_end_leave_ind) AS mnth_end_leave_ind,
                      max(eff_hrch_is_active_in_mnth_end) AS is_active_in_mnth_end,
                      max(eff_hrch_is_active_in_qtr_end) AS is_active_in_qtr_end,
                      max(eff_hrch_is_active_in_yr_end) AS is_active_in_yr_end
                FROM 
                (
                    SELECT
                      /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
                      r.environment,
                      r.db_schema,
                      r.clnt_obj_id,
                      r.pers_obj_id,
                      r.yr_cd,
                      r.qtr_cd,
                      r.mnth_cd,
                      d_job_cd,
                      d_hr_orgn_id,
                      d_work_loc_cd,
                      d_work_state_cd,
                      d_work_city_cd,
                      d_work_cntry_cd,
                      d_gndr_cd,
                      f_work_asgnmt_stus_cd,
                      f_work_asgnmt_actv_ind,
                      d_full_tm_part_tm_cd,
                      --d_eeo_ethncty_clsfn_cd,
                      d_reg_temp_cd,
                      rl.mngr_pers_obj_id as login_mngr_pers_obj_id,
                      r.mngr_pers_obj_id,                      
                      --is_manager,
                      eff_person_days,
                      tnur,
                      f_compa_rt,
                      r.rec_eff_strt_dt,
                      r.rec_eff_end_dt,
                      CASE WHEN ((CAST(r.rec_eff_end_dt AS DATE) = CAST(yr_end_dt AS DATE)) AND f_work_asgnmt_stus_cd in ('L','P')) THEN 1 ELSE 0 END AS eff_hrch_yr_end_leave_ind,
                      CASE WHEN ((CAST(r.rec_eff_end_dt AS DATE) = CAST(qtr_end_dt AS DATE)) AND f_work_asgnmt_stus_cd in ('L','P')) THEN 1 ELSE 0 END AS eff_hrch_qtr_end_leave_ind,
                      CASE WHEN ((CAST(r.rec_eff_end_dt AS DATE) = CAST(mnth_end_dt AS DATE)) AND f_work_asgnmt_stus_cd in ('L','P')) THEN 1 ELSE 0 END AS eff_hrch_mnth_end_leave_ind,
                      CASE WHEN CAST(r.rec_eff_end_dt AS DATE) = CAST(yr_end_dt AS DATE)
                            THEN 1 ELSE 0 END AS eff_hrch_is_active_in_yr_end,
                      CASE WHEN CAST(r.rec_eff_end_dt AS DATE) = CAST(qtr_end_dt AS DATE)
                            THEN 1 ELSE 0 END AS eff_hrch_is_active_in_qtr_end,
                      CASE WHEN CAST(r.rec_eff_end_dt AS DATE) = CAST(mnth_end_dt AS DATE)
                            THEN 1 ELSE 0 END AS eff_hrch_is_active_in_mnth_end                    
                    FROM
                      ${__BLUE_MAIN_DB__}.emi_prep_hr_waf r
                      INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
                        ON r.environment = rl.environment
                        AND r.db_schema = rl.db_schema
                        AND r.clnt_obj_id = rl.clnt_obj_id
                        AND r.mngr_pers_obj_id = rl.pers_obj_id 
                        AND rl.mngr_pers_obj_id IS NOT NULL                 
                    WHERE 
                    --r.environment = '${environment}' 
                    r.yr_cd >= cast(year(add_months(current_date, -24)) as string)
                    AND r.rec_eff_strt_dt >= rl.rec_eff_strt_dt AND r.rec_eff_end_dt <= rl.rec_eff_end_dt
                )r             
                GROUP BY 
                         environment,
                         db_schema,
                         clnt_obj_id,
                         pers_obj_id,
                         yr_cd,
                         qtr_cd,
                         mnth_cd,
                         d_job_cd,
                         d_hr_orgn_id,
                         d_work_loc_cd,
                         d_work_state_cd,
                         d_work_city_cd,
                         d_work_cntry_cd,
                         d_gndr_cd,                         
                         f_work_asgnmt_stus_cd,
                         f_work_asgnmt_actv_ind,
                         d_full_tm_part_tm_cd,                        
                         d_reg_temp_cd,                         
                         mngr_pers_obj_id,
                         login_mngr_pers_obj_id
              ) waf

    UNION ALL

    SELECT 
              waf.clnt_obj_id,
              waf.pers_obj_id,
              waf.qtr_cd,
              waf.mnth_cd,
              waf.d_job_cd,
              waf.d_hr_orgn_id,
              waf.d_work_loc_cd,
              waf.d_work_state_cd,
              waf.d_work_city_cd,
              waf.d_work_cntry_cd,
              waf.d_gndr_cd,
              waf.f_work_asgnmt_stus_cd,
              waf.f_work_asgnmt_actv_ind,
              waf.d_full_tm_part_tm_cd,
              waf.f_compa_rt,
              waf.rec_eff_strt_dt,
              waf.rec_eff_end_dt,
              waf.d_reg_temp_cd,
              waf.eff_person_days,
              waf.tnur,
              login_mngr_pers_obj_id as mngr_pers_obj_id,
              --waf.is_manager,
              waf.yr_end_leave_ind,
              waf.qtr_end_leave_ind,
              waf.mnth_end_leave_ind,
              waf.is_active_in_mnth_end,
              waf.is_active_in_qtr_end,
              waf.is_active_in_yr_end,
              waf.environment,
              waf.db_schema,
              waf.yr_cd
         FROM (SELECT 
                      environment,
                      db_schema,
                      clnt_obj_id,
                      pers_obj_id,
                      yr_cd,
                      qtr_cd,
                      mnth_cd,
                      d_job_cd,
                      d_hr_orgn_id,
                      d_work_loc_cd,
                      d_work_state_cd,
                      d_work_city_cd,
                      d_work_cntry_cd,
                      d_gndr_cd,
                      f_work_asgnmt_stus_cd,
                      f_work_asgnmt_actv_ind,
                      d_full_tm_part_tm_cd,
                      --d_eeo_ethncty_clsfn_cd,
                      d_reg_temp_cd,
                      mngr_pers_obj_id,
                      login_mngr_pers_obj_id,
                      --max(is_manager) as is_manager,
                      sum(eff_person_days) AS eff_person_days,
                      max(tnur) AS tnur,
                      max(f_compa_rt) AS f_compa_rt,
                      min(rec_eff_strt_dt) AS rec_eff_strt_dt,
                      max(rec_eff_end_dt) AS rec_eff_end_dt,                      
                      sum(yr_end_leave_ind) AS yr_end_leave_ind,
                      sum(qtr_end_leave_ind) AS qtr_end_leave_ind,
                      sum(mnth_end_leave_ind) AS mnth_end_leave_ind,                      
                      max(is_active_in_mnth_end) AS is_active_in_mnth_end,
                      max(is_active_in_qtr_end) AS is_active_in_qtr_end,
                      max(is_active_in_yr_end) AS is_active_in_yr_end                      
                FROM 
                (
                    SELECT
                      /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
                      r.environment,
                      r.db_schema,
                      r.clnt_obj_id,
                      r.pers_obj_id,
                      r.yr_cd,
                      r.qtr_cd,
                      r.mnth_cd,
                      d_job_cd,
                      d_hr_orgn_id,
                      d_work_loc_cd,
                      d_work_state_cd,
                      d_work_city_cd,
                      d_work_cntry_cd,
                      d_gndr_cd,
                      f_work_asgnmt_stus_cd,
                      f_work_asgnmt_actv_ind,
                      d_full_tm_part_tm_cd,
                      --d_eeo_ethncty_clsfn_cd,
                      d_reg_temp_cd,
                      rl.mngr_pers_obj_id as login_mngr_pers_obj_id,
                      r.mngr_pers_obj_id,                      
                      --is_manager,
                      eff_person_days,
                      tnur,
                      f_compa_rt,
                      r.rec_eff_strt_dt,
                      r.rec_eff_end_dt,
                      yr_end_leave_ind,
                      qtr_end_leave_ind,
                      mnth_end_leave_ind,
                      is_active_in_mnth_end,
                      is_active_in_qtr_end,
                      is_active_in_yr_end
                    FROM
                      ${__BLUE_MAIN_DB__}.emi_prep_hr_waf r
                      INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
                        ON r.environment = rl.environment
                        AND r.db_schema = rl.db_schema
                        AND r.clnt_obj_id = rl.clnt_obj_id
                        AND r.pers_obj_id = rl.pers_obj_id 
                        AND mtrx_hrchy_ind = 1
                        AND rl.mngr_pers_obj_id IS NOT NULL
                    WHERE 
                    --r.environment = '${environment}' 
                    r.yr_cd >= cast(year(add_months(current_date, -24)) as string)                      
                )r              
                GROUP BY 
                    environment,
                    db_schema,
                    clnt_obj_id,
                    pers_obj_id,
                    yr_cd,
                    qtr_cd,
                    mnth_cd,
                    d_job_cd,
                    d_hr_orgn_id,
                    d_work_loc_cd,
                    d_work_state_cd,
                    d_work_city_cd,
                    d_work_cntry_cd,
                    d_gndr_cd,                    
                    f_work_asgnmt_stus_cd,
                    f_work_asgnmt_actv_ind,
                    d_full_tm_part_tm_cd,                    
                    d_reg_temp_cd,                    
                    mngr_pers_obj_id,
                    login_mngr_pers_obj_id
              ) waf

     
      ) final
    WHERE mngr_pers_obj_id IS NOT NULL;

-- Temporary table which will hold which persons are already captured as a part of manager hierarchy
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs 
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
    ${__BLUE_MAIN_DB__}.emi_prep_hr_waf waf
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
            ON waf.environment = rl.environment
            AND waf.db_schema = rl.db_schema
            AND waf.clnt_obj_id = rl.clnt_obj_id
            AND waf.mngr_pers_obj_id = rl.pers_obj_id
            AND rl.mngr_pers_obj_id IS NOT NULL 
WHERE  waf.rec_eff_strt_dt >= rl.rec_eff_strt_dt
        AND waf.rec_eff_end_dt <= rl.rec_eff_end_dt;


-- Insert Cherrypick records into the main table first
INSERT INTO TABLE ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_mngr PARTITION(environment)
SELECT
       clnt_obj_id,
       pers_obj_id,
       qtr_cd,
       mnth_cd,
       CASE WHEN trim(d_job_cd)='-' THEN 'UNKNOWN' ELSE  d_job_cd END AS d_job_cd,
       CASE WHEN trim(d_hr_orgn_id)='-' THEN 'UNKNOWN' ELSE  d_hr_orgn_id END AS d_hr_orgn_id,
       CASE WHEN trim(d_work_loc_cd)='-' THEN 'UNKNOWN' ELSE  d_work_loc_cd END AS d_work_loc_cd,
       CASE WHEN trim(d_work_state_cd)='-' THEN 'UNKNOWN' ELSE  d_work_state_cd END AS d_work_state_cd,
       CASE WHEN trim(d_work_city_cd)='-' THEN 'UNKNOWN' ELSE  d_work_city_cd END AS d_work_city_cd,
       CASE WHEN trim(d_work_cntry_cd)='-' THEN 'UNKNOWN' ELSE  d_work_cntry_cd END AS d_work_cntry_cd,
       d_gndr_cd,
       f_work_asgnmt_stus_cd,
       f_work_asgnmt_actv_ind,
       d_full_tm_part_tm_cd,
       f_compa_rt,
       rec_eff_strt_dt,
       rec_eff_end_dt,
       --d_eeo_ethncty_clsfn_cd,
       d_reg_temp_cd,
       eff_person_days,
       tnur,
       mngr_pers_obj_id,
       --is_manager,
       yr_end_leave_ind,
       qtr_end_leave_ind,
       mnth_end_leave_ind,
       is_active_in_yr_end,
       is_active_in_qtr_end,
       is_active_in_mnth_end,
       db_schema,
       yr_cd,
       environment
  FROM
      (SELECT 
              /*+ BROADCAST(dwh_t_dim_mngr_cherrypick,tmp_emi_hrchy_mngrs) */
              waf.clnt_obj_id,
              waf.pers_obj_id,
              waf.qtr_cd,
              waf.mnth_cd,
              waf.d_job_cd,
              waf.d_hr_orgn_id,
              waf.d_work_loc_cd,
              waf.d_work_state_cd,
              waf.d_work_city_cd,
              waf.d_work_cntry_cd,
              waf.d_gndr_cd,
              waf.f_work_asgnmt_stus_cd,
              waf.f_work_asgnmt_actv_ind,
              waf.d_full_tm_part_tm_cd,
              waf.d_eeo_ethncty_clsfn_cd,
              waf.f_compa_rt,
              waf.rec_eff_strt_dt,
              waf.rec_eff_end_dt,
              waf.d_reg_temp_cd,
              waf.eff_person_days,
              waf.tnur,
              rl.mngr_pers_obj_id,
              --waf.is_manager,
              waf.yr_end_leave_ind,
              waf.qtr_end_leave_ind,
              waf.mnth_end_leave_ind,
              waf.is_active_in_mnth_end,
              waf.is_active_in_qtr_end,
              waf.is_active_in_yr_end,
              waf.environment,
              waf.db_schema,
              waf.yr_cd
         FROM (SELECT 
                    environment,
                    r.db_schema,
                    clnt_obj_id,
                    pers_obj_id,
                    work_asgnmt_nbr,
                    yr_cd,
                    qtr_cd,
                    mnth_cd,
                    d_job_cd,
                    d_hr_orgn_id,
                    d_work_loc_cd,
                    d_work_state_cd,
                    d_work_city_cd,
                    d_work_cntry_cd,
                    d_gndr_cd,
                    f_work_asgnmt_stus_cd,
                    f_work_asgnmt_actv_ind,
                    d_full_tm_part_tm_cd,
                    d_eeo_ethncty_clsfn_cd,
                    d_reg_temp_cd,
                    mngr_pers_obj_id,
                    --max(is_manager) as is_manager,
                    sum(eff_person_days) AS eff_person_days,
                    max(tnur) AS tnur,
                    max(f_compa_rt) AS f_compa_rt,
                    min(rec_eff_strt_dt) AS rec_eff_strt_dt,
                    max(rec_eff_end_dt) AS rec_eff_end_dt,
                    sum(yr_end_leave_ind) AS yr_end_leave_ind,
                    sum(qtr_end_leave_ind) AS qtr_end_leave_ind,
                    sum(mnth_end_leave_ind) AS mnth_end_leave_ind,
                    max(is_active_in_mnth_end) AS is_active_in_mnth_end,
                    max(is_active_in_qtr_end) AS is_active_in_qtr_end,
                    max(is_active_in_yr_end) AS is_active_in_yr_end
                FROM ${__BLUE_MAIN_DB__}.emi_prep_hr_waf r
                WHERE 
                --environment = '${environment}' 
                yr_cd >= cast(year(add_months(current_date, -24)) as string)
                GROUP BY 
                    environment,
                    r.db_schema,
                    clnt_obj_id,
                    pers_obj_id,
                    work_asgnmt_nbr,
                    yr_cd,
                    qtr_cd,
                    mnth_cd,
                    d_job_cd,
                    d_hr_orgn_id,
                    d_work_loc_cd,
                    d_work_state_cd,
                    d_work_city_cd,
                    d_work_cntry_cd,
                    d_gndr_cd,
                    f_work_asgnmt_stus_cd,
                    f_work_asgnmt_actv_ind,
                    d_full_tm_part_tm_cd,
                    d_eeo_ethncty_clsfn_cd,
                    d_reg_temp_cd,
                    mngr_pers_obj_id
              ) waf
        INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_mngr_cherrypick rl
            ON waf.environment = rl.environment
            AND waf.db_schema = rl.db_schema
            AND waf.clnt_obj_id = rl.clnt_obj_id
            AND waf.pers_obj_id = rl.pers_obj_id 
            AND waf.work_asgnmt_nbr = rl.work_asgnmt_nbr
        LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs prev
            ON waf.environment = prev.environment
            AND waf.db_schema = prev.db_schema
            AND waf.clnt_obj_id = prev.clnt_obj_id
            AND waf.pers_obj_id = prev.pers_obj_id
            AND rl.mngr_pers_obj_id = prev.mngr_pers_obj_id
            AND waf.yr_cd = prev.yr_cd
            AND waf.qtr_cd = prev.qtr_cd
            AND waf.mnth_cd = prev.mnth_cd
        WHERE prev.mngr_pers_obj_id IS NULL -- to exclude cases where the manager is included as a part of the reporting hierarchy
      ) final;

--DROP TABLE ${__GREEN_MAIN_DB__}.tmp_emi_hrchy_mngrs;