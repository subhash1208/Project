-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_dim_ins_arm_stats (
    bandit_arm string,
    clicks_cnt INT,
    non_clicks_cnt INT,
    learning_rt DOUBLE,
    decay_clicks_rt DOUBLE,
    decay_non_clicks_rt DOUBLE,
    environment string
) USING PARQUET
PARTITIONED BY (environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_dim_ins_arm_stats_bkp;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_dim_ins_arm_stats_bkp (
    bandit_arm string,
    clicks_cnt INT,
    non_clicks_cnt INT,
    learning_rt DOUBLE,
    decay_clicks_rt DOUBLE,
    decay_non_clicks_rt DOUBLE,
    environment string
) USING PARQUET
PARTITIONED BY (environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_dim_ins_arm_stats_bkp PARTITION(environment)
SELECT
  bandit_arm ,
  clicks_cnt ,
  non_clicks_cnt ,
  learning_rt ,
  decay_clicks_rt ,
  decay_non_clicks_rt ,
  environment
FROM ${__BLUE_MAIN_DB__}.t_dim_ins_arm_stats;
 
--ALTER TABLE ${__GREEN_MAIN_DB__}.t_dim_ins_arm_stats DROP IF EXISTS PARTITION (environment = '${environment}');
 
INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_dim_ins_arm_stats PARTITION(environment)
SELECT
    curr_wk.bandit_arm,
    COALESCE(curr_wk.wk_clicks_cnt + case when arm_stats.clicks_cnt is not null then arm_stats.clicks_cnt else 0 end ,0) as clicks_cnt,
    COALESCE(curr_wk.wk_no_clicks_cnt + case when arm_stats.non_clicks_cnt  is not null then arm_stats.non_clicks_cnt else 0 end ,0) as non_clicks_cnt,
    cast('${__RECOMM_LEARNING_RATE__}' as DOUBLE) as learning_rate,
    COALESCE(curr_wk.wk_clicks_cnt + (case when arm_stats.decay_clicks_rt is not null then arm_stats.decay_clicks_rt else 0 end * cast('${__RECOMM_LEARNING_RATE__}' as DOUBLE)),0) as decay_clicks_rt,
    COALESCE(curr_wk.wk_no_clicks_cnt + (case when arm_stats.decay_non_clicks_rt is not null then arm_stats.decay_non_clicks_rt else 0 end * cast('${__RECOMM_LEARNING_RATE__}' as DOUBLE)),0) as decay_non_clicks_rt,
    curr_wk.environment
FROM
 (  
  SELECT
    hash_dimension_list as bandit_arm,
    SUM(cur_wk_clicks) as wk_clicks_cnt,
    SUM(cur_wk_no_clicks) as wk_no_clicks_cnt,
    environment
    FROM
    (
      SELECT
        hist.ins_hash_val,
        CONCAT_WS("_",
           SORT_ARRAY(
              SPLIT ((CONCAT_WS("-",
                          CASE WHEN ins.yr_cd IS NOT NULL THEN "yr_cd" END,
                          CASE WHEN ins.qtr_cd IS NOT NULL THEN "qtr_cd" END,
                          CASE WHEN ins.mnth_cd IS NOT NULL THEN "mnth_cd" END,
                          CASE WHEN ins.hr_orgn_id IS NOT NULL THEN "hr_orgn_id" END,
                          CASE WHEN ins.job_cd IS NOT NULL THEN "job_cd" END,
                          CASE WHEN ins.iso_3_char_cntry_cd IS NOT NULL THEN "iso_3_char_cntry_cd" END,
                          CASE WHEN ins.state_prov_cd IS NOT NULL THEN "state_prov_cd" END,
                          CASE WHEN ins.work_loc_cd IS NOT NULL THEN "work_loc_cd" END,
                          CASE WHEN ins.trmnt_rsn IS NOT NULL THEN "trmnt_rsn" END,
                          CASE WHEN ins.adp_lense_cd IS NOT NULL THEN "adp_len_cd" END,
                          CASE WHEN ins.inds_cd IS NOT NULL THEN "inds_cd" END,
                          CASE WHEN ins.sector_cd IS NOT NULL THEN "sector_cd" END,
                          CASE WHEN ins.super_sect_cd IS NOT NULL THEN "super_sect_cd" END,
                          CASE WHEN ins.mngr_pers_obj_id  IS NOT NULL THEN "mngr_pers_obj_id" END,
                          CASE WHEN ins.excp_type IS NOT NULL THEN "excp_type" END,
                          lower(ins.ins_type),
                          cast(ins.mtrc_ky as string)
                                )),"-")
                      )
                  )  AS hash_dimension_list,
       CASE WHEN usr_view_ind is not null and cast(usr_view_ind as int) > 0 THEN 1 ELSE 0 END as cur_wk_clicks,
       CASE WHEN usr_view_ind is not null and cast(usr_view_ind as int) = 0 THEN 1 ELSE 0 END as cur_wk_no_clicks,
       ins.environment
     FROM
      ${__BLUE_MAIN_DB__}.t_fact_ins ins INNER JOIN
      ${__RO_BLUE_RAW_DB__}.dwh_t_fact_ins_hist hist 
      ON  ins.ins_hash_val = hist.ins_hash_val
      AND ins.environment = hist.environment
      AND hist.ins_serv_dt > DATE_SUB(current_date(),7)
      --AND hist.bandit_ind = 1
    --WHERE ins.environment = '${environment}'
     ) curr_arm_stats
    GROUP BY hash_dimension_list,environment
    ) curr_wk
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.t_dim_ins_arm_stats_bkp arm_stats
ON curr_wk.bandit_arm = arm_stats.bandit_arm;