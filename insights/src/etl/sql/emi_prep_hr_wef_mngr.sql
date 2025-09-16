-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_wef_mngr;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_hr_wef_mngr (
    clnt_obj_id STRING,
    pers_obj_id STRING,
    work_asgnmt_stus_cd STRING,
    pers_stus_cd STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    d_job_cd STRING,
    d_hr_orgn_id STRING,
    d_work_loc_cd STRING,
    d_work_state_cd STRING,
    d_work_city_cd STRING,
    d_work_cntry_cd STRING,
    d_gndr_cd STRING,
    event_cd STRING,
    event_rsn_cd STRING,
    d_trmnt_rsn STRING,
    vlntry_trmnt_ind DOUBLE,
    trmnt_ind DOUBLE,
    months_between_promotion DOUBLE,
    promo_ind DOUBLE,
    hir_ind DOUBLE,
    int_mob_hir_ind DOUBLE,
    reh_ind DOUBLE,
    retirement_ind DOUBLE,
    dem_ind DOUBLE,
    xfr_ind DOUBLE,
    new_hir_ind DOUBLE,
    int_mobility_rate_ind DOUBLE,
    mngr_pers_obj_id STRING,
    db_schema STRING,
    yr_cd STRING,
    environment STRING
) USING PARQUET
PARTITIONED BY(environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.emi_prep_hr_wef_mngr PARTITION(environment)
SELECT
       /*+ COALESCE(800) */
       clnt_obj_id,
       pers_obj_id,
       work_asgnmt_stus_cd,
       pers_stus_cd,
       qtr_cd,
       mnth_cd,
       CASE WHEN trim(d_job_cd)='-' THEN 'UNKNOWN' ELSE  d_job_cd END AS d_job_cd,
       CASE WHEN trim(d_hr_orgn_id)='-' THEN 'UNKNOWN' ELSE  d_hr_orgn_id END AS d_hr_orgn_id,
       CASE WHEN trim(d_work_loc_cd)='-' THEN 'UNKNOWN' ELSE  d_work_loc_cd END AS d_work_loc_cd,
       CASE WHEN trim(d_work_state_cd)='-' THEN 'UNKNOWN' ELSE  d_work_state_cd END AS d_work_state_cd,
       CASE WHEN trim(d_work_city_cd)='-' THEN 'UNKNOWN' ELSE  d_work_city_cd END AS d_work_city_cd,
       CASE WHEN trim(d_work_cntry_cd)='-' THEN 'UNKNOWN' ELSE  d_work_cntry_cd END AS d_work_cntry_cd,  
       d_gndr_cd,
       event_cd,
       event_rsn_cd,
       CASE WHEN trim(d_trmnt_rsn)='-' THEN 'UNKNOWN' ELSE  d_trmnt_rsn END AS d_trmnt_rsn,
       vlntry_trmnt_ind,
       trmnt_ind,
       months_between_promotion,
       promo_ind,
       hir_ind,
       int_mob_hir_ind,
       reh_ind,
       retirement_ind,
       dem_ind,
       xfr_ind,
       new_hir_ind,
       int_mobility_rate_ind,
       mngr_pers_obj_id,
       db_schema,
       yr_cd,
       environment
  FROM
      (SELECT 
              wef.clnt_obj_id,
              wef.pers_obj_id,
              wef.work_asgnmt_stus_cd,
              wef.pers_stus_cd,
              wef.qtr_cd,
              wef.mnth_cd,
              wef.d_job_cd,
              wef.d_hr_orgn_id,
              wef.d_work_loc_cd,
              wef.d_work_state_cd,
              wef.d_work_city_cd,
              wef.d_work_cntry_cd,
              wef.d_gndr_cd,
              wef.event_cd,
              wef.event_rsn_cd,
              wef.d_trmnt_rsn,
              wef.vlntry_trmnt_ind,
              wef.trmnt_ind,
              wef.promo_ind,
              wef.hir_ind,
              wef.int_mob_hir_ind,
              wef.int_mobility_rate_ind,
              wef.reh_ind,
              wef.retirement_ind,
              wef.dem_ind,
              wef.xfr_ind,
              wef.new_hir_ind,
              wef.months_between_promotion,
              wef.login_mngr_pers_obj_id AS mngr_pers_obj_id,
              wef.environment,
              wef.db_schema,
              wef.yr_cd
         FROM (SELECT 
                      /*+ BROADCAST(emi_t_rpt_to_hrchy_sec,dwh_t_dim_work_loc) */
                      r.environment,
                      r.db_schema,
                      r.clnt_obj_id,
                      r.pers_obj_id,
                      work_asgnmt_nbr,
                      work_asgnmt_stus_cd,
                      pers_stus_cd,
                      yr_cd,
                      qtr_cd,
                      mnth_cd,
                      d_job_cd,
                      d_hr_orgn_id,
                      d_work_loc_cd,
                      worklocs.state_prov_cd     AS d_work_state_cd,
                      worklocs.city_id      AS d_work_city_cd,
                      r.d_work_cntry_cd,
                      d_gndr_cd,
                      d_reg_temp_cd,
                      r.mngr_pers_obj_id,
                      rl.mngr_pers_obj_id as login_mngr_pers_obj_id,
                      event_cd,
                      event_rsn_cd,  -- Ideally, this should not be a part of the group-by. But, app is doing this way...
                      max(CASE WHEN event_cd = 'TER' THEN event_rsn_cd ELSE NULL END) AS d_trmnt_rsn,
                      sum(vlntry_trmnt_ind) AS vlntry_trmnt_ind,
                      sum(trmnt_ind) AS trmnt_ind,
                      sum(promo_ind) AS promo_ind,
                      sum(hir_ind) AS hir_ind,
                      sum(reh_ind) AS reh_ind,
                      sum(retirement_ind) AS retirement_ind,
                      sum(dem_ind) AS dem_ind,
                      sum(xfr_ind) AS xfr_ind,
                      sum(new_hir_ind) AS new_hir_ind,
                      sum(int_mob_hir_ind) AS int_mob_hir_ind,
                      sum(CASE WHEN(event_cd = 'PRO') THEN 1
                           WHEN(event_cd = 'DEM') THEN 1
                           WHEN(event_cd = 'XFR') THEN 1
                           WHEN(event_cd = 'HIR') THEN 1
                           WHEN(event_cd = 'REH') THEN 1
                           WHEN(event_cd = 'TER') THEN 1
                           WHEN(event_cd = 'RET') THEN 1 ELSE 0 END) AS int_mobility_rate_ind,
                      avg(months_between_promotion) AS months_between_promotion
                 FROM ${__BLUE_MAIN_DB__}.emi_base_hr_wef r
                 LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_loc worklocs
                      ON r.clnt_obj_id  = worklocs.clnt_obj_id
                      AND r.d_work_loc_cd = worklocs.work_loc_cd
                      AND r.db_schema = worklocs.db_schema
                      AND r.environment = worklocs.environment
                 INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
                      ON r.environment = rl.environment
                      AND r.db_schema = rl.db_schema
                      AND r.clnt_obj_id = rl.clnt_obj_id
                      AND r.mngr_pers_obj_id = rl.pers_obj_id
                      AND rl.mngr_pers_obj_id IS NOT NULL              
                      AND r.event_eff_dt BETWEEN rl.rec_eff_strt_dt AND rl.rec_eff_end_dt
                WHERE 
                --r.environment = '${environment}' 
                 r.yr_cd >= cast(year(add_months(current_date, -24)) as string)
                GROUP BY 
                         r.environment,
                         r.db_schema,
                         r.clnt_obj_id,
                         r.pers_obj_id,
                         work_asgnmt_nbr,
                         work_asgnmt_stus_cd,
                         pers_stus_cd,
                         yr_cd,
                         qtr_cd,
                         mnth_cd,
                         d_job_cd,
                         d_hr_orgn_id,
                         d_work_loc_cd,
                         worklocs.state_prov_cd,
                         worklocs.city_id,
                         r.d_work_cntry_cd,
                         d_gndr_cd,
                         d_reg_temp_cd,
                         event_cd,
                         event_rsn_cd,
                         r.mngr_pers_obj_id,
                         rl.mngr_pers_obj_id
              ) wef

    UNION ALL

    SELECT 
              /*+ BROADCAST(emi_t_rpt_to_hrchy_sec) */
              wef.clnt_obj_id,
              wef.pers_obj_id,
              wef.work_asgnmt_stus_cd,
              wef.pers_stus_cd,
              wef.qtr_cd,
              wef.mnth_cd,
              wef.d_job_cd,
              wef.d_hr_orgn_id,
              wef.d_work_loc_cd,
              wef.d_work_state_cd,
              wef.d_work_city_cd,
              wef.d_work_cntry_cd,
              wef.d_gndr_cd,
              wef.event_cd,
              wef.event_rsn_cd,
              wef.d_trmnt_rsn,
              wef.vlntry_trmnt_ind,
              wef.trmnt_ind,
              wef.promo_ind,
              wef.hir_ind,
              wef.int_mob_hir_ind,
              wef.int_mobility_rate_ind,
              wef.reh_ind,
              wef.retirement_ind,
              wef.dem_ind,
              wef.xfr_ind,
              wef.new_hir_ind,
              wef.months_between_promotion,
              rl.mngr_pers_obj_id,
              wef.environment,
              wef.db_schema,
              wef.yr_cd
         FROM (SELECT 
                      /*+ BROADCAST(dwh_t_dim_work_loc) */
                      r.environment,
                      r.db_schema,
                      r.clnt_obj_id,
                      r.pers_obj_id,
                      work_asgnmt_nbr,
                      work_asgnmt_stus_cd,
                      pers_stus_cd,
                      yr_cd,
                      qtr_cd,
                      mnth_cd,
                      d_job_cd,
                      d_hr_orgn_id,
                      d_work_loc_cd,
                      worklocs.state_prov_cd     AS d_work_state_cd,
                      worklocs.city_id      AS d_work_city_cd,
                      r.d_work_cntry_cd,
                      d_gndr_cd,
                      d_reg_temp_cd,
                      mngr_pers_obj_id,
                      event_cd,
                      event_rsn_cd,  -- Ideally, this should not be a part of the group-by. But, app is doing this way...
                      max(CASE WHEN event_cd = 'TER' THEN event_rsn_cd ELSE NULL END) AS d_trmnt_rsn,
                      sum(vlntry_trmnt_ind) AS vlntry_trmnt_ind,
                      sum(trmnt_ind) AS trmnt_ind,
                      sum(promo_ind) AS promo_ind,
                      sum(hir_ind) AS hir_ind,
                      sum(reh_ind) AS reh_ind,
                      sum(retirement_ind) AS retirement_ind,
                      sum(dem_ind) AS dem_ind,
                      sum(xfr_ind) AS xfr_ind,
                      sum(new_hir_ind) AS new_hir_ind,
                      sum(int_mob_hir_ind) AS int_mob_hir_ind,
                      sum(CASE WHEN(event_cd = 'PRO') THEN 1
                           WHEN(event_cd = 'DEM') THEN 1
                           WHEN(event_cd = 'XFR') THEN 1
                           WHEN(event_cd = 'HIR') THEN 1
                           WHEN(event_cd = 'REH') THEN 1
                           WHEN(event_cd = 'TER') THEN 1
                           WHEN(event_cd = 'RET') THEN 1 ELSE 0 END) AS int_mobility_rate_ind,
                      avg(months_between_promotion) AS months_between_promotion
                 FROM ${__BLUE_MAIN_DB__}.emi_base_hr_wef r
                 LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_loc worklocs
                      ON r.clnt_obj_id  = worklocs.clnt_obj_id
                      AND r.d_work_loc_cd = worklocs.work_loc_cd
                      AND r.db_schema = worklocs.db_schema
                      AND r.environment = worklocs.environment
                WHERE 
                --r.environment = '${environment}' 
                r.yr_cd >= cast(year(add_months(current_date, -24)) as string)
                GROUP BY 
                         r.environment,
                         r.db_schema,
                         r.clnt_obj_id,
                         r.pers_obj_id,
                         work_asgnmt_nbr,
                         work_asgnmt_stus_cd,
                         pers_stus_cd,
                         yr_cd,
                         qtr_cd,
                         mnth_cd,
                         d_job_cd,
                         d_hr_orgn_id,
                         d_work_loc_cd,
                         worklocs.state_prov_cd,
                         worklocs.city_id,
                         r.d_work_cntry_cd,
                         d_gndr_cd,
                         d_reg_temp_cd,
                         event_cd,
                         event_rsn_cd,
                         mngr_pers_obj_id
              ) wef
        INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
            ON wef.environment = rl.environment
            AND wef.db_schema = rl.db_schema
            AND wef.clnt_obj_id = rl.clnt_obj_id
            AND wef.pers_obj_id = rl.pers_obj_id
            AND mtrx_hrchy_ind = 1
            AND rl.mngr_pers_obj_id IS NOT NULL
            
      ) final
      WHERE mngr_pers_obj_id IS NOT NULL;

-- Temporary table which will hold which persons are already captured as a part of manager hierarchy
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_wef;

CREATE TABLE ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_wef 
USING PARQUET 
AS
SELECT
    DISTINCT wef.clnt_obj_id,
    wef.pers_obj_id,
    rl.mngr_pers_obj_id,
    wef.environment,
    wef.db_schema,
    wef.yr_cd,
    wef.qtr_cd,
    wef.mnth_cd
FROM
    ${__BLUE_MAIN_DB__}.emi_base_hr_wef wef
    INNER JOIN ${__BLUE_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
        ON wef.environment = rl.environment
        AND wef.db_schema = rl.db_schema
        AND wef.clnt_obj_id = rl.clnt_obj_id
        AND wef.mngr_pers_obj_id = rl.pers_obj_id
        AND wef.event_eff_dt BETWEEN rl.rec_eff_strt_dt AND rl.rec_eff_end_dt
WHERE 
    wef.mngr_pers_obj_id IS NOT NULL
;

-- Cherrypick records
INSERT INTO TABLE ${__BLUE_MAIN_DB__}.emi_prep_hr_wef_mngr PARTITION(environment)
SELECT
       /*+ COALESCE(800) */
       clnt_obj_id,
       pers_obj_id,
       work_asgnmt_stus_cd,
       pers_stus_cd,
       qtr_cd,
       mnth_cd,
       CASE WHEN trim(d_job_cd)='-' THEN 'UNKNOWN' ELSE  d_job_cd END AS d_job_cd,
       CASE WHEN trim(d_hr_orgn_id)='-' THEN 'UNKNOWN' ELSE  d_hr_orgn_id END AS d_hr_orgn_id,
       CASE WHEN trim(d_work_loc_cd)='-' THEN 'UNKNOWN' ELSE  d_work_loc_cd END AS d_work_loc_cd,
       CASE WHEN trim(d_work_state_cd)='-' THEN 'UNKNOWN' ELSE  d_work_state_cd END AS d_work_state_cd,
       CASE WHEN trim(d_work_city_cd)='-' THEN 'UNKNOWN' ELSE  d_work_city_cd END AS d_work_city_cd,
       CASE WHEN trim(d_work_cntry_cd)='-' THEN 'UNKNOWN' ELSE  d_work_cntry_cd END AS d_work_cntry_cd,
       d_gndr_cd,
       event_cd,
       event_rsn_cd,
       CASE WHEN trim(d_trmnt_rsn)='-' THEN 'UNKNOWN' ELSE  d_trmnt_rsn END AS d_trmnt_rsn,
       vlntry_trmnt_ind,
       trmnt_ind,
       months_between_promotion,
       promo_ind,
       hir_ind,
       int_mob_hir_ind,
       reh_ind,
       retirement_ind,
       dem_ind,
       xfr_ind,
       new_hir_ind,
       int_mobility_rate_ind,
       mngr_pers_obj_id,
       db_schema,
       yr_cd,
       environment
  FROM
      (SELECT 
              /*+ BROADCAST(tmp_emi_hrchy_mngrs_wef,dwh_t_dim_mngr_cherrypick) */ 
              wef.clnt_obj_id,
              wef.pers_obj_id,
              wef.work_asgnmt_stus_cd,
              wef.pers_stus_cd,
              wef.qtr_cd,
              wef.mnth_cd,
              wef.d_job_cd,
              wef.d_hr_orgn_id,
              wef.d_work_loc_cd,
              wef.d_work_state_cd,
              wef.d_work_city_cd,
              wef.d_work_cntry_cd,
              wef.d_gndr_cd,
              wef.event_cd,
              wef.event_rsn_cd,
              wef.d_trmnt_rsn,
              wef.vlntry_trmnt_ind,
              wef.trmnt_ind,
              wef.promo_ind,
              wef.hir_ind,
              wef.int_mob_hir_ind,
              wef.reh_ind,
              wef.retirement_ind,
              wef.dem_ind,
              wef.xfr_ind,
              wef.new_hir_ind,
              wef.int_mobility_rate_ind,
              wef.months_between_promotion,
              rl.mngr_pers_obj_id as mngr_pers_obj_id,
              wef.environment,
              wef.db_schema,
              wef.yr_cd
         FROM (SELECT 
                      /*+ BROADCAST(dwh_t_dim_work_loc) */
                      r.environment,
                      r.db_schema,
                      r.clnt_obj_id,
                      r.pers_obj_id,
                      work_asgnmt_nbr,
                      work_asgnmt_stus_cd,
                      pers_stus_cd,
                      yr_cd,
                      qtr_cd,
                      mnth_cd,
                      d_job_cd,
                      d_hr_orgn_id,
                      d_work_loc_cd,
                      worklocs.state_prov_cd     AS d_work_state_cd,
                      worklocs.city_id      AS d_work_city_cd,
                      r.d_work_cntry_cd,
                      d_gndr_cd,
                      d_reg_temp_cd,
                      mngr_pers_obj_id,
                      event_cd,
                      event_rsn_cd,  -- Ideally, this should not be a part of the group-by. But, app is doing this way...
                      max(CASE WHEN event_cd = 'TER' THEN event_rsn_cd ELSE NULL END) AS d_trmnt_rsn,
                      max(vlntry_trmnt_ind) AS vlntry_trmnt_ind,
                      max(trmnt_ind) AS trmnt_ind,
                      max(promo_ind) AS promo_ind,
                      max(hir_ind) AS hir_ind,
                      max(int_mob_hir_ind) AS int_mob_hir_ind,
                      max(reh_ind) AS reh_ind,
                      max(retirement_ind) AS retirement_ind,
                      max(dem_ind) AS dem_ind,
                      max(xfr_ind) AS xfr_ind,
                      max(new_hir_ind) AS new_hir_ind,
                      sum(CASE WHEN(event_cd = 'PRO') THEN 1
                           WHEN(event_cd = 'DEM') THEN 1
                           WHEN(event_cd = 'XFR') THEN 1
                           WHEN(event_cd = 'HIR') THEN 1
                           WHEN(event_cd = 'REH') THEN 1
                           WHEN(event_cd = 'TER') THEN 1
                           WHEN(event_cd = 'RET') THEN 1 ELSE 0 END) AS int_mobility_rate_ind,
                      avg(months_between_promotion) AS months_between_promotion
                 FROM ${__BLUE_MAIN_DB__}.emi_base_hr_wef r
                 LEFT OUTER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_work_loc worklocs
                      ON r.clnt_obj_id  = worklocs.clnt_obj_id
                      AND r.d_work_loc_cd = worklocs.work_loc_cd
                      AND r.db_schema = worklocs.db_schema
                      AND r.environment = worklocs.environment
                WHERE 
                --r.environment = '${environment}' 
                 r.yr_cd >= cast(year(add_months(current_date, -24)) as string)
                GROUP BY 
                         r.environment,
                         r.db_schema,
                         r.clnt_obj_id,
                         r.pers_obj_id,
                         work_asgnmt_nbr,
                         work_asgnmt_stus_cd,
                         pers_stus_cd,
                         yr_cd,
                         qtr_cd,
                         mnth_cd,
                         d_job_cd,
                         d_hr_orgn_id,
                         d_work_loc_cd,
                         worklocs.state_prov_cd,
                         worklocs.city_id,
                         r.d_work_cntry_cd,
                         d_gndr_cd,
                         d_reg_temp_cd,
                         event_cd,
                         event_rsn_cd,
                         mngr_pers_obj_id
                DISTRIBUTE BY r.clnt_obj_id, r.db_schema
              ) wef
        INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_mngr_cherrypick rl
            ON wef.environment = rl.environment
            AND wef.db_schema = rl.db_schema
            AND wef.clnt_obj_id = rl.clnt_obj_id
            AND wef.pers_obj_id = rl.pers_obj_id 
            AND wef.work_asgnmt_nbr = rl.work_asgnmt_nbr
        --LEFT OUTER JOIN(
        --     SELECT
        --       DISTINCT wef.clnt_obj_id,
        --       wef.pers_obj_id,
        --       rl.mngr_pers_obj_id,
        --       wef.environment,
        --       wef.db_schema,
        --       wef.yr_cd,
        --       wef.qtr_cd,
        --       wef.mnth_cd
        --     FROM
        --       ${__GREEN_MAIN_DB__}.emi_base_hr_wef wef
        --       INNER JOIN ${__GREEN_MAIN_DB__}.emi_t_rpt_to_hrchy_sec rl
        --       ON wef.environment = rl.environment
        --       AND wef.db_schema = rl.db_schema
        --       AND wef.clnt_obj_id = rl.clnt_obj_id
        --       AND wef.mngr_pers_obj_id = rl.pers_obj_id
        --       AND wef.event_eff_dt BETWEEN rl.rec_eff_strt_dt AND rl.rec_eff_end_dt
        --     WHERE wef.mngr_pers_obj_id IS NOT NULL
        --)prev 
        LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.tmp_emi_hrchy_mngrs_wef prev
            ON wef.environment = prev.environment
            AND wef.db_schema = prev.db_schema
            AND wef.clnt_obj_id = prev.clnt_obj_id
            AND wef.pers_obj_id = prev.pers_obj_id
            AND rl.mngr_pers_obj_id = prev.mngr_pers_obj_id
            AND wef.yr_cd = prev.yr_cd
            AND wef.qtr_cd = prev.qtr_cd
            AND wef.mnth_cd = prev.mnth_cd
        WHERE prev.mngr_pers_obj_id IS NULL -- to exclude cases where the manager is included as a part of the reporting hierarchy
      ) final
      WHERE mngr_pers_obj_id IS NOT NULL;

--DROP TABLE ${__GREEN_MAIN_DB__}.tmp_emi_hrchy_mngrs_wef;