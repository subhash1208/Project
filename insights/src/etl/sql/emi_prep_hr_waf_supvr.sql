-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_supvr (
    clnt_obj_id STRING,
    pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    f_work_asgnmt_stus_cd STRING,
    rec_eff_strt_dt TIMESTAMP,
    rec_eff_end_dt TIMESTAMP,
    eff_person_days DOUBLE,
    supvr_pers_obj_id STRING,
    rpt_access STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) 
USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_supvr_headcnt(
    clnt_obj_id STRING,
    supvr_pers_obj_id STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    supvr_headcount DOUBLE,
    rpt_access STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
)USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_supvr PARTITION(environment)
SELECT
       /*+ COALESCE(800) */
       clnt_obj_id,
       pers_obj_id,
       qtr_cd,
       mnth_cd,
       f_work_asgnmt_stus_cd,
       rec_eff_strt_dt,
       rec_eff_end_dt,
       eff_person_days,
       supvr_pers_obj_id,
       rpt_access,
       db_schema,
       yr_cd,
       environment
  FROM
      (SELECT 
              waf.clnt_obj_id,
              waf.pers_obj_id,
              waf.qtr_cd,
              waf.mnth_cd,
              waf.d_job_cd,
              waf.f_work_asgnmt_stus_cd,
              waf.rec_eff_strt_dt,
              waf.rec_eff_end_dt,
              waf.eff_person_days,
              waf.tnur,
              login_supvr_pers_obj_id AS supvr_pers_obj_id,
              rpt_access,
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
                      f_work_asgnmt_stus_cd,
                      supvr_pers_obj_id,
                      login_supvr_pers_obj_id,
                      sum(eff_person_days) AS eff_person_days,
                      max(tnur) AS tnur,
                      min(rec_eff_strt_dt) AS rec_eff_strt_dt,
                      max(rec_eff_end_dt) AS rec_eff_end_dt,
                      rpt_access
                FROM 
                (
                    SELECT
                      /*+ BROADCAST(emi_prep_supvr_hrchy) */
                      r.environment,
                      r.db_schema,
                      r.clnt_obj_id,
                      r.pers_obj_id,
                      r.yr_cd,
                      r.qtr_cd,
                      r.mnth_cd,
                      d_job_cd,
                      f_work_asgnmt_stus_cd,
                      rl.login_supvr_pers_obj_id as login_supvr_pers_obj_id,
                      r.supvr_pers_obj_id,                      
                      eff_person_days,
                      tnur,
                      rec_eff_strt_dt,
                      rec_eff_end_dt,
                      rl.rpt_access
                    FROM
                      ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_tm_tf r
                      INNER JOIN ${__BLUE_MAIN_DB__}.emi_prep_supvr_hrchy rl
                        ON r.environment = rl.environment
                        AND r.db_schema = rl.db_schema
                        AND r.clnt_obj_id = rl.clnt_obj_id
                        AND (case when rl.work_asgnmt_nbr is null then r.supvr_pers_obj_id = rl.supvr_pers_obj_id else r.work_asgnmt_nbr=rl.work_asgnmt_nbr end)
                        AND r.pers_obj_id = rl.pers_obj_id
                        AND rl.login_supvr_pers_obj_id IS NOT NULL
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
                         f_work_asgnmt_stus_cd,  
                         supvr_pers_obj_id,
                         login_supvr_pers_obj_id,
                         rpt_access
              ) waf       

         
    -- UNION ALL

    -- SELECT 
    --           waf.clnt_obj_id,
    --           waf.pers_obj_id,
    --           waf.qtr_cd,
    --           waf.mnth_cd,
    --           waf.d_job_cd,
    --           waf.f_work_asgnmt_stus_cd,
    --           waf.rec_eff_strt_dt,
    --           waf.rec_eff_end_dt,
    --           waf.eff_person_days,
    --           waf.tnur,
    --           login_supvr_pers_obj_id AS supvr_pers_obj_id,
    --           waf.environment,
    --           waf.db_schema,
    --           waf.yr_cd
    --      FROM (SELECT 
    --                   environment,
    --                   db_schema,
    --                   clnt_obj_id,
    --                   pers_obj_id,
    --                   yr_cd,
    --                   qtr_cd,
    --                   mnth_cd,
    --                   d_job_cd,
    --                   f_work_asgnmt_stus_cd,
    --                   supvr_pers_obj_id,
    --                   login_supvr_pers_obj_id,
    --                   sum(eff_person_days) AS eff_person_days,
    --                   max(tnur) AS tnur,
    --                   min(rec_eff_strt_dt) AS rec_eff_strt_dt,
    --                   max(rec_eff_end_dt) AS rec_eff_end_dt
    --             FROM 
    --             (
    --                 SELECT
    --                   r.environment,
    --                   r.db_schema,
    --                   r.clnt_obj_id,
    --                   r.pers_obj_id,
    --                   r.yr_cd,
    --                   r.qtr_cd,
    --                   r.mnth_cd,
    --                   d_job_cd,
    --                   f_work_asgnmt_stus_cd,
    --                   rl.supvr_pers_obj_id as login_supvr_pers_obj_id,
    --                   r.supvr_pers_obj_id,                      
    --                   eff_person_days,
    --                   tnur,
    --                   r.rec_eff_strt_dt,
    --                   r.rec_eff_end_dt               
    --                 FROM
    --                   ${__GREEN_MAIN_DB__}.emi_prep_hr_waf_tm_tf r
    --                   INNER JOIN ${__GREEN_MAIN_DB__}.emi_t_supvr_rpt_to_hrchy_sec rl
    --                     ON r.environment = rl.environment
    --                     AND r.db_schema = rl.db_schema
    --                     AND r.clnt_obj_id = rl.clnt_obj_id
    --                     AND r.supvr_pers_obj_id = rl.pers_obj_id 
    --                     AND rl.supvr_pers_obj_id IS NOT NULL                 
    --                 WHERE r.environment = '${environment}' AND r.yr_cd >= cast(year(add_months(current_date, -24)) as string)
    --                 AND r.rec_eff_strt_dt >= rl.rec_eff_strt_dt AND r.rec_eff_end_dt <= rl.rec_eff_end_dt
    --             )r             
    --             GROUP BY 
    --                      environment,
    --                      db_schema,
    --                      clnt_obj_id,
    --                      pers_obj_id,
    --                      yr_cd,
    --                      qtr_cd,
    --                      mnth_cd,
    --                      d_job_cd,                   
    --                      f_work_asgnmt_stus_cd,
    --                      supvr_pers_obj_id,
    --                      login_supvr_pers_obj_id
    --           ) waf 
      ) final
    WHERE supvr_pers_obj_id IS NOT NULL;

-- Temporary table which will hold which persons are already captured as a part of manager hierarchy
-- DROP TABLE IF EXISTS ${__GREEN_MAIN_DB__}.tmp_emi_hrchy_supvrs;

-- CREATE TABLE ${__GREEN_MAIN_DB__}.tmp_emi_hrchy_supvrs 
-- STORED AS PARQUET 
-- AS 
-- SELECT
--     DISTINCT waf.clnt_obj_id,
--     waf.pers_obj_id,
--     rl.supvr_pers_obj_id,    
--     waf.environment,
--     waf.db_schema,
--     waf.yr_cd,
--     waf.qtr_cd,
--     waf.mnth_cd
-- FROM
--     ${__GREEN_MAIN_DB__}.emi_prep_hr_waf_tm_tf waf
--     INNER JOIN ${__GREEN_MAIN_DB__}.emi_t_supvr_rpt_to_hrchy rl
--         ON waf.environment = rl.environment
--         AND waf.db_schema = rl.db_schema
--         AND waf.clnt_obj_id = rl.clnt_obj_id
--         AND waf.supvr_pers_obj_id = rl.pers_obj_id             
-- WHERE 
--      waf.supvr_pers_obj_id IS NOT NULL

-- UNION ALL

-- SELECT
--     DISTINCT waf.clnt_obj_id,
--     waf.pers_obj_id,
--     rl.supvr_pers_obj_id,
--     waf.environment,
--     waf.db_schema,
--     waf.yr_cd,
--     waf.qtr_cd,
--     waf.mnth_cd
-- FROM
--     ${__GREEN_MAIN_DB__}.emi_prep_hr_waf_tm_tf waf
--     INNER JOIN ${__GREEN_MAIN_DB__}.emi_t_supvr_rpt_to_hrchy_sec rl
--             ON waf.environment = rl.environment
--             AND waf.db_schema = rl.db_schema
--             AND waf.clnt_obj_id = rl.clnt_obj_id
--             AND waf.supvr_pers_obj_id = rl.pers_obj_id
--             AND rl.supvr_pers_obj_id IS NOT NULL 
-- WHERE  waf.rec_eff_strt_dt >= rl.rec_eff_strt_dt
--         AND waf.rec_eff_end_dt <= rl.rec_eff_end_dt;


-- -- Insert Cherrypick records into the main table first
-- INSERT INTO TABLE ${__GREEN_MAIN_DB__}.emi_prep_hr_waf_supvr PARTITION(environment, yr_cd)
-- SELECT
--        clnt_obj_id,
--        pers_obj_id,
--        qtr_cd,
--        mnth_cd,
--        f_work_asgnmt_stus_cd,
--        rec_eff_strt_dt,
--        rec_eff_end_dt,
--        eff_person_days,
--        supvr_pers_obj_id,
--        db_schema,
--        environment,
--        yr_cd
--   FROM
--       (SELECT 
--               waf.clnt_obj_id,
--               waf.pers_obj_id,
--               waf.qtr_cd,
--               waf.mnth_cd,
--               waf.d_job_cd,
--                waf.f_work_asgnmt_stus_cd,
--               waf.rec_eff_strt_dt,
--               waf.rec_eff_end_dt,
--               waf.eff_person_days,
--               waf.tnur,
--               rl.supvr_pers_obj_id,
--               waf.environment,
--               waf.db_schema,
--               waf.yr_cd
--          FROM (SELECT 
--                     environment,
--                     r.db_schema,
--                     clnt_obj_id,
--                     pers_obj_id,
--                     work_asgnmt_nbr,
--                     yr_cd,
--                     qtr_cd,
--                     mnth_cd,
--                     d_job_cd,
--                     f_work_asgnmt_stus_cd,
--                     supvr_pers_obj_id,                    
--                     sum(eff_person_days) AS eff_person_days,
--                     max(tnur) AS tnur,
--                     min(rec_eff_strt_dt) AS rec_eff_strt_dt,
--                     max(rec_eff_end_dt) AS rec_eff_end_dt
--                 FROM ${__GREEN_MAIN_DB__}.emi_prep_hr_waf_tm_tf r
--                 WHERE environment = '${environment}' AND yr_cd >= cast(year(add_months(current_date, -24)) as string)
--                 GROUP BY 
--                     environment,
--                     r.db_schema,
--                     clnt_obj_id,
--                     pers_obj_id,
--                     work_asgnmt_nbr,
--                     yr_cd,
--                     qtr_cd,
--                     mnth_cd,
--                     d_job_cd,
--                     f_work_asgnmt_stus_cd,
--                     supvr_pers_obj_id
--               ) waf
--         INNER JOIN ${__RO_GREEN_RAW_DB__}.dwh_t_dim_supvr_cherrypick rl
--             ON waf.environment = rl.environment
--             AND waf.db_schema = rl.db_schema
--             AND waf.clnt_obj_id = rl.clnt_obj_id
--             AND waf.pers_obj_id = rl.pers_obj_id 
--             AND waf.work_asgnmt_nbr = rl.work_asgnmt_nbr
--         LEFT OUTER JOIN ${__GREEN_MAIN_DB__}.tmp_emi_hrchy_supvrs prev
--             ON waf.environment = prev.environment
--             AND waf.db_schema = prev.db_schema
--             AND waf.clnt_obj_id = prev.clnt_obj_id
--             AND waf.pers_obj_id = prev.pers_obj_id
--             AND rl.supvr_pers_obj_id = prev.supvr_pers_obj_id
--             AND waf.yr_cd = prev.yr_cd
--             AND waf.qtr_cd = prev.qtr_cd
--             AND waf.mnth_cd = prev.mnth_cd
--         WHERE prev.supvr_pers_obj_id IS NULL -- to exclude cases where the manager is included as a part of the reporting hierarchy
--       ) final 
-- DISTRIBUTE BY environment, db_schema, yr_cd, mnth_cd;

-- DROP TABLE ${__GREEN_MAIN_DB__}.tmp_emi_hrchy_supvrs;

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_supvr_headcnt PARTITION(environment)
SELECT 
  /*+ COALESCE(800) */ 
  clnt_obj_id,
  supvr_pers_obj_id,
  qtr_cd,
  mnth_cd,
  SUM(eff_person_days)/(DATEDIFF(MAX(rec_eff_end_dt),MIN(rec_eff_strt_dt))+1) AS supvr_headcount,
  rpt_access,
  db_schema,
  yr_cd,
  environment
FROM ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_supvr
WHERE 
f_work_asgnmt_stus_cd IN ('P','A','L','S')
GROUP BY environment,
  db_schema,
  clnt_obj_id,
  rpt_access,
  supvr_pers_obj_id,
  yr_cd,
  qtr_cd,
  mnth_cd GROUPING SETS((environment,db_schema,clnt_obj_id, supvr_pers_obj_id,rpt_access,yr_cd,qtr_cd), 
  (environment,db_schema,clnt_obj_id, supvr_pers_obj_id,rpt_access,yr_cd,qtr_cd,mnth_cd));