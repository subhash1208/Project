-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_fact_insights_clnt_int;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_fact_insights_clnt_int (
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
    --insight_events double,
    ins_rsn string,
    ins_empl_cnt double,
    ins_empl_cnt_cmpr_with double,
    empl_cnt double,
    empl_cnt_cmpr_with double,
    pct_empl_cnt double,
    pct_empl_cnt_cmpr_with double,
    nbr_of_diments int,
    retn_period_strt_person_cnt double,
    retn_period_end_person_cnt double,
    retn_period_strt_person_cnt_r double,
    retn_period_end_person_cnt_r double,
    metric_value double,
    metric_value_r double,
    diff double,
    percentage_diff double,
    normalised_diff double,
    normalised_percentage_diff double,
    zscore_diff double,
    zscore_percentage_diff double,
    normalised_zscore_diff double,
    normalised_zscore_percentage_diff double,
    --min_max_zscore double,
    --min_max_zscore_norm double,
    min_metric_value double,
    max_metric_value double,
    ins_json string,
    dmn_ky string,
    export_type string,
    excp_type string,
    excp_type_cmpr_with string,
    supvr_pers_obj_id string,
    supvr_pers_obj_id_cmpr_with string,
    rpt_access STRING,
    db_schema string,
    environment string
) USING PARQUET
PARTITIONED BY (environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');


INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_fact_insights_clnt_int PARTITION(environment)
SELECT
  /*+ COALESCE(800) */
  insight_hash,
  clnt_obj_id,
  clnt_obj_id_r,
  yr_cd,
  yr_cd_r,
  cast(qtr_cd as int) as qtr_cd,
  cast(qtr_cd_r as int) as qtr_cd_r,
  cast(mnth_cd as int) as mnth_cd,
  cast(mnth_cd_r as int) as mnth_cd_r,
  wk_cd,
  wk_cd_r,
  flsa_stus_cd,
  flsa_stus_cd_r,
  full_tm_part_tm_cd,
  full_tm_part_tm_cd_r,
  gndr_cd,
  gndr_cd_r,
  hr_orgn_id,
  hr_orgn_id_r,
  job_cd,
  job_cd_r,
  pay_rt_type_cd,
  pay_rt_type_cd_r,
  reg_temp_cd,
  reg_temp_cd_r,
  work_loc_cd,
  work_loc_cd_r,
  work_city_cd,
  work_city_cd_r,
  work_state_cd,
  work_state_cd_r,
  work_cntry_cd,
  work_cntry_cd_r,
  trmnt_rsn,
  trmnt_rsn_r,
  adp_lens_cd,
  adp_lens_cd_r,
  inds_ky,
  inds_ky_r,
  sector_cd,
  sector_cd_r,
  supersector_cd,
  supersector_cd_r,
  mngr_pers_obj_id,
  mngr_pers_obj_id_r,
  cast(metric_ky as int) as metric_ky,
  insight_score,
  percentile_rank,
  insight_type,
  --insight_events,
  insight_reason,
  ins_empl_cnt,
  ins_empl_cnt_r,
  empl_count,
  empl_count_r,
  pctg_empl_count,
  pctg_empl_count_r,
  num_dimensions,
  retn_period_strt_person_cnt,
  retn_period_end_person_cnt,
  retn_period_strt_person_cnt_r,
  retn_period_end_person_cnt_r,
  metric_value,
  metric_value_r,
  diff,
  percentage_diff,
  normalised_diff,
  normalised_percentage_diff,
  zscore_diff,
  zscore_percentage_diff,
  normalised_zscore_diff,
  normalised_zscore_percentage_diff,
  --min_max_zscore,
  --min_max_zscore_norm,
  min_metric_value,
  max_metric_value,
  insights_json,
  dmn_ky,
  export_type,
  excp_type,
  excp_type_r,
  supvr_pers_obj_id,
  supvr_pers_obj_id_r,
  rpt_access,
  db_schema,
  environment
FROM
(
--Manager insights
SELECT
  /*+ BROADCAST(today,clnt_fisc_info) */
  md5(concat(
       COALESCE(split(mngr_int_ins.db_schema,'[|]')[0],'-'),
       COALESCE(mngr_int_ins.clnt_obj_id,'-'),
       COALESCE(clnt_obj_id_r,'-'),
       COALESCE(yr_cd,'-'),
       COALESCE(yr_cd_r,'-'),
       COALESCE(qtr_cd,'-'),
       COALESCE(qtr_cd_r,'-'),
       COALESCE(mnth_cd,'-'),
       COALESCE(mnth_cd_r,'-'),   
       COALESCE(job_cd,'-'),
       COALESCE(job_cd_r,'-'),
       COALESCE(hr_orgn_id,'-'),
       COALESCE(hr_orgn_id_r,'-'),
       COALESCE(work_cntry_cd,'-'),
       COALESCE(work_cntry_cd_r,'-'),
       COALESCE(work_state_cd,'-'),
       COALESCE(work_state_cd_r,'-'),
       COALESCE(work_loc_cd,'-'),
       COALESCE(work_loc_cd_r,'-'),
       COALESCE(mngr_pers_obj_id,'-'),
       COALESCE(mngr_pers_obj_id_r,'-'),
       COALESCE(metric_ky,'-'),
       COALESCE(insight_type,'-'),
       COALESCE(CASE WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = min_metric_value THEN 'MIN_PERCENTILE_RANKING'
                 WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = max_metric_value THEN 'MAX_PERCENTILE_RANKING'
                  ELSE insight_reason END,'-')
     )) as insight_hash,
  mngr_int_ins.clnt_obj_id,
  mngr_int_ins.clnt_obj_id_r,
  CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN yr_cd ELSE CONCAT('FY',yr_cd) END as yr_cd,
  CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN yr_cd_r ELSE CONCAT('FY',yr_cd_r) END as yr_cd_r,
  qtr_cd,
  qtr_cd_r,
  mnth_cd,
  mnth_cd_r,
  NULL AS wk_cd,
  NULL AS wk_cd_r,
  NULL AS flsa_stus_cd,
  NULL AS flsa_stus_cd_r,
  NULL AS full_tm_part_tm_cd,
  NULL AS full_tm_part_tm_cd_r,
  NULL AS gndr_cd,
  NULL AS gndr_cd_r,
  hr_orgn_id,
  hr_orgn_id_r,
  job_cd,
  job_cd_r,
  NULL AS pay_rt_type_cd,
  NULL AS pay_rt_type_cd_r,
  NULL AS reg_temp_cd,
  NULL AS reg_temp_cd_r,
  work_loc_cd,
  work_loc_cd_r,
  NULL AS work_city_cd,
  NULL AS work_city_cd_r,
  work_state_cd,
  work_state_cd_r,
  work_cntry_cd,
  work_cntry_cd_r,
  NULL AS trmnt_rsn,
  NULL AS trmnt_rsn_r,
  NULL AS adp_lens_cd,
  NULL AS adp_lens_cd_r,
  NULL AS inds_ky,
  NULL AS inds_ky_r,
  NULL AS sector_cd,
  NULL AS sector_cd_r,
  NULL AS supersector_cd,
  NULL AS supersector_cd_r,
  mngr_pers_obj_id,
  mngr_pers_obj_id_r,
  metric_ky,
  (insight_score * metric_wgt *
     (CASE WHEN job_cd IS NOT NULL THEN job_cd_wgt ELSE 1 END) *
     (CASE WHEN hr_orgn_id IS NOT NULL THEN hr_orgn_id_wgt ELSE 1 END) *
     (CASE WHEN mngr_int_ins.qtr_seq_nbr IS NOT NULL THEN qtr_seq_nbr_wgt ELSE 1 END) *
     (CASE WHEN mngr_int_ins.mnth_seq_nbr IS NOT NULL THEN mnth_seq_nbr_wgt ELSE 1 END) *
     (CASE WHEN work_state_cd IS NOT NULL THEN work_state_cd_wgt ELSE 1 END) *
     (CASE WHEN work_loc_cd IS NOT NULL THEN work_loc_cd_wgt ELSE 1 END)
     ) AS insight_score,
  percentile_rank,
  CASE WHEN (mngr_int_ins.metric_ky<>53 AND insight_reason not in('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') AND job_cd IS NULL AND hr_orgn_id IS NULL AND work_cntry_cd IS NULL) THEN 'CLIENT_INTERNAL_MYTEAM' ELSE insight_type END as insight_type,
  insight_events,
  CASE WHEN insight_reason IN ('NORM_PCTG_DIFF','PCTG_DIFF') THEN
            CASE WHEN (mngr_int_ins.yr_cd <=> mngr_int_ins.yr_cd_r AND mngr_int_ins.qtr_cd <=> mngr_int_ins.qtr_cd_r AND mngr_int_ins.mnth_cd <=> mngr_int_ins.mnth_cd_r) THEN 'PCTG_DIFF'
            ELSE 'PCTG_DIFF_TIME'  END                   
       WHEN insight_reason  IN ('NORM_ABS_DIFF','ABS_DIFF') THEN
           CASE WHEN (mngr_int_ins.yr_cd <=> mngr_int_ins.yr_cd_r AND mngr_int_ins.qtr_cd <=> mngr_int_ins.qtr_cd_r AND mngr_int_ins.mnth_cd <=> mngr_int_ins.mnth_cd_r) THEN 'ABS_DIFF'
           ELSE 'ABS_DIFF_TIME' END
       WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = min_metric_value THEN 'MIN_PERCENTILE_RANKING'
       WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = max_metric_value THEN 'MAX_PERCENTILE_RANKING'
       ELSE insight_reason
  END AS insight_reason,
  ins_empl_cnt,
  ins_empl_cnt_r,
  empl_count,
  empl_count_r,
  pctg_empl_count,
  pctg_empl_count_r,
  num_dimensions,
  NULL AS retn_period_strt_person_cnt,
  NULL AS retn_period_end_person_cnt,
  NULL AS retn_period_strt_person_cnt_r,
  NULL AS retn_period_end_person_cnt_r,
  metric_value,
  metric_value_r,
  diff,
  percentage_diff,
  normalised_diff,
  normalised_percentage_diff,
  zscore_diff,
  zscore_percentage_diff,
  normalised_zscore_diff,
  normalised_zscore_percentage_diff,
  min_max_zscore,
  min_max_zscore_norm,
  min_metric_value,
  max_metric_value,
  insights_json,
  mngr_int_ins.dmn_ky as dmn_ky,
  'mngr_int_ins' AS export_type,
  NULL AS excp_type,
  NULL AS excp_type_r,
  NULL AS supvr_pers_obj_id,
  NULL AS supvr_pers_obj_id_r,
  NULL AS rpt_access,
  mngr_int_ins.db_schema,
  mngr_int_ins.environment
FROM
(SELECT
    /*+ BROADCAST(lqtr,rqtr,lmnth,rmnth,emi_meta_dw_metrics) */
    rosie.clnt_obj_id,
    clnt_obj_id_r,
    yr_cd,
    yr_cd_r,
    lqtr.qtr_cd AS qtr_cd,
    rqtr.qtr_cd AS qtr_cd_r,
    lmnth.mnth_cd AS mnth_cd,
    rmnth.mnth_cd AS mnth_cd_r,
    rosie.qtr_seq_nbr,
    qtr_seq_nbr_r,
    rosie.mnth_seq_nbr,
    mnth_seq_nbr_r, 
    job_cd,
    job_cd_r,
    hr_orgn_id,
    hr_orgn_id_r,
    work_cntry_cd,
    work_cntry_cd_r,
    work_state_cd,
    work_state_cd_r,
    work_loc_cd,
    work_loc_cd_r,
    mngr_pers_obj_id,
    mngr_pers_obj_id_r,
    rosie.metric_ky,
    insight_reason,
    insight_score,
    insight_type,
    insight_events,
    percentile_rank,
    ins_empl_cnt,
    ins_empl_cnt_r,
    empl_count,
    empl_count_r,
    pctg_empl_count,
    pctg_empl_count_r,
    num_dimensions,
    metric_value,
    metric_value_r,
    diff,
    percentage_diff,
    normalised_diff,
    normalised_percentage_diff,
    zscore_diff,
    zscore_percentage_diff,
    normalised_zscore_diff,
    normalised_zscore_percentage_diff,
    min_max_zscore,
    min_max_zscore_norm,
    min_metric_value,
    max_metric_value,
    insights_json,
    dmn_ky,
    metric_wgt ,
    job_cd_wgt ,
    gndr_cd_wgt ,
    qtr_seq_nbr_wgt,
    mnth_seq_nbr_wgt,
    work_state_cd_wgt,
    work_loc_cd_wgt,
    hr_orgn_id_wgt,
    trmnt_rsn_wgt,
    rosie.environment,
    fact_sch.db_schema
FROM
(SELECT
     clnt_obj_id,
     clnt_obj_id_r,
     yr_cd,
     yr_cd_r,
     mnth_seq_nbr,
     mnth_seq_nbr_r,
     qtr_seq_nbr,
     qtr_seq_nbr_r, 
     job_cd,
     job_cd_r,
     hr_orgn_id,
     hr_orgn_id_r,
     work_cntry_cd,
     work_cntry_cd_r,
     work_state_cd,
     work_state_cd_r,
     work_loc_cd,
     work_loc_cd_r,
     mngr_pers_obj_id,
     mngr_pers_obj_id_r,
     metrics.metric_ky,
     metrics.insight_reason,
     metrics.insight_score,
     metrics.insight_type,
     metrics.insight_events,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS percentile_rank,
     ins_empl_cnt,
     ins_empl_cnt_r,
     empl_count,
     empl_count_r,
     pctg_empl_count,
     pctg_empl_count_r,
     num_dimensions,
     CAST(metrics.insight_metrics[0] AS DOUBLE) AS metric_value,
     CAST(metrics.insight_metrics[1] AS DOUBLE) AS metric_value_r,
     CAST(metrics.insight_metrics[4] AS DOUBLE) AS diff,
     CAST(metrics.insight_metrics[5] AS DOUBLE) AS percentage_diff,
     CAST(metrics.insight_metrics[6] AS DOUBLE) AS normalised_diff,
     CAST(metrics.insight_metrics[7] AS DOUBLE) AS normalised_percentage_diff,
     CAST(metrics.insight_metrics[8] AS DOUBLE) AS zscore_diff,
     CAST(metrics.insight_metrics[9] AS DOUBLE) AS zscore_percentage_diff,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS normalised_zscore_diff,
     CAST(metrics.insight_metrics[11] AS DOUBLE) AS normalised_zscore_percentage_diff,
     CAST(metrics.insight_metrics[13] AS DOUBLE) AS min_metric_value,
     CAST(metrics.insight_metrics[14] AS DOUBLE) AS max_metric_value,
     CAST(metrics.insight_metrics[15] AS DOUBLE) AS min_max_zscore,
     CAST(metrics.insight_metrics[16] AS DOUBLE) AS min_max_zscore_norm,
     CONCAT(
        "{ \"this\": ", COALESCE(CAST(metrics.insight_metrics[0] AS STRING),'null'),
         ",\"that\": ", COALESCE(CAST(metrics.insight_metrics[1] AS STRING),'null'),
         ",\"this_events\": ", COALESCE(CAST(metrics.insight_metrics[2] AS STRING),'null'),
         ",\"that_events\": ", COALESCE(CAST(metrics.insight_metrics[3] AS STRING),'null'),
         ",\"insight\": {\"diff\" : ", COALESCE(CAST(metrics.insight_metrics[4] AS STRING),'null'),
         ",\"percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[5] AS STRING),'null'),
         ",\"normalised_diff\" : ", COALESCE(CAST(metrics.insight_metrics[6] AS STRING),'null'),
         ",\"normalised_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[7] AS STRING),'null'),
         ",\"ins_empl_cnt\" : ", COALESCE(CAST(ins_empl_cnt AS STRING),'null'),
         ",\"ins_empl_cnt_cmpr_with\" : ", COALESCE(CAST(ins_empl_cnt_r AS STRING),'null'),
         ",\"zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[8] AS STRING),'null'),
         ",\"zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[9] AS STRING),'null'),
         ",\"normalised_zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[10] AS STRING),'null'),
         ",\"normalised_zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[11] AS STRING),'null'),
         ",\"percentile_rank\" : ", COALESCE(CAST(metrics.insight_metrics[12] AS STRING),'null'),
         ",\"percentile_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[15] AS STRING),'null'),
         ",\"percentile_normalised_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[16] AS STRING),'null'),
         ",\"mean_diff\" : ", COALESCE(CAST(metrics.insight_metrics[17] AS STRING),'null'),
         ",\"stddev_diff\" : ", COALESCE(CAST(metrics.insight_metrics[18] AS STRING),'null'),
         ",\"mean_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[19] AS STRING),'null'),
         ",\"stddev_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[20] AS STRING),'null'),
         ",\"mean_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[21] AS STRING),'null'),
         ",\"stddev_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[22] AS STRING),'null'),
         ",\"mean_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[23] AS STRING),'null'),
         ",\"stddev_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[24] AS STRING),'null'),
         ",\"percentile_mean\" : ", COALESCE(CAST(metrics.insight_metrics[25] AS STRING),'null'),
         ",\"percentile_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[26] AS STRING),'null'),
         ",\"percentile_norm_mean\" : ", COALESCE(CAST(metrics.insight_metrics[27] AS STRING),'null'),
         ",\"percentile_norm_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[28] AS STRING),'null'), "}}"
     ) AS insights_json,
     2 AS dmn_ky,
     environment
     FROM
        (SELECT
          clnt_obj_id,
          clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr as int)    AS yr_cd,
          cast(yr_seq_nbr_r as int)    AS yr_cd_r,      
          d_job_cd        AS job_cd,
          d_job_cd_r      AS job_cd_r,
          d_hr_orgn_id    AS hr_orgn_id,
          d_hr_orgn_id_r  AS hr_orgn_id_r,
          d_work_cntry_cd AS work_cntry_cd,
          d_work_cntry_cd_r AS work_cntry_cd_r,
          d_work_state_cd AS work_state_cd,
          d_work_state_cd_r AS work_state_cd_r,
          d_work_loc_cd   AS work_loc_cd,
          d_work_loc_cd_r AS work_loc_cd_r,
          mngr_pers_obj_id,
          mngr_pers_obj_id_r,
          insight_reason_turnover_rate,
          insight_score_turnover_rate,
          insight_type_turnover_rate,
          turnover_rate_metrics,
          turnover_rate_events,
          insight_reason_voln_turnover_rate,
          insight_score_voln_turnover_rate,
          insight_type_voln_turnover_rate,
          voln_turnover_rate_metrics,
          voln_turnover_rate_events,
          insight_reason_internal_mobility_rate,
          insight_score_internal_mobility_rate,
          insight_type_internal_mobility_rate,
          internal_mobility_rate_metrics,
          internal_mobility_rate_events,
          insight_reason_newhire_turnover_rate,
          insight_score_newhire_turnover_rate,
          insight_type_newhire_turnover_rate,
          newhire_turnover_rate_metrics,
          newhire_turnover_rate_events,
          insight_reason_average_tenure,
          insight_score_average_tenure,
          insight_type_average_tenure,
          average_tenure_metrics,
          average_tenure_events,
          insight_reason_avg_time_to_promotion,
          insight_score_avg_time_to_promotion,
          insight_type_avg_time_to_promotion,
          avg_time_to_promotion_metrics,
          avg_time_to_promotion_events,
          insight_reason_retention_rate,
          insight_score_retention_rate,
          insight_type_retention_rate,
          retention_rate_metrics,
          retention_rate_events,
          insight_reason_num_hires,
          insight_score_num_hires,
          insight_type_num_hires,
          num_hires_metrics,
          num_hires_events,
          insight_reason_num_terminations,
          insight_score_num_terminations,
          insight_type_num_terminations,
          num_terminations_metrics,
          num_terminations_events,
          insight_reason_headcount,
          insight_score_headcount,
          insight_type_headcount,
          headcount_metrics,
          headcount_events,
          insight_reason_leave_percentage,
          insight_score_leave_percentage,
          insight_type_leave_percentage,
          leave_percentage_metrics,
          leave_percentage_events,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount AS empl_count,
          tot_rpt_headcount_r AS empl_count_r,
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r      AS pctg_empl_count_r,
          num_dimensions,
          environment
        FROM ${__BLUE_MAIN_DB__}.emi_ins_hr_waf_manager
        DISTRIBUTE BY clnt_obj_id
        ) hr
     LATERAL VIEW stack(11,  -- Total number of rows each row is exploded to
                            '76', insight_reason_turnover_rate,insight_score_turnover_rate,insight_type_turnover_rate,turnover_rate_metrics,turnover_rate_events,
                            '53', insight_reason_voln_turnover_rate,insight_score_voln_turnover_rate,insight_type_voln_turnover_rate,voln_turnover_rate_metrics,voln_turnover_rate_events,
                            '65', insight_reason_internal_mobility_rate,insight_score_internal_mobility_rate,insight_type_internal_mobility_rate,internal_mobility_rate_metrics,internal_mobility_rate_events,
                            '69', insight_reason_newhire_turnover_rate,insight_score_newhire_turnover_rate,insight_type_newhire_turnover_rate,newhire_turnover_rate_metrics,newhire_turnover_rate_events,
                            '60', insight_reason_average_tenure,insight_score_average_tenure,insight_type_average_tenure,average_tenure_metrics,average_tenure_events,
                            '79', insight_reason_avg_time_to_promotion,insight_score_avg_time_to_promotion,insight_type_avg_time_to_promotion,avg_time_to_promotion_metrics,avg_time_to_promotion_events,
                            '78', insight_reason_retention_rate,insight_score_retention_rate,insight_type_retention_rate,retention_rate_metrics,retention_rate_events,
                            '74', insight_reason_num_hires,insight_score_num_hires,insight_type_num_hires,num_hires_metrics,num_hires_events,
                            '63', insight_reason_num_terminations,insight_score_num_terminations,insight_type_num_terminations,num_terminations_metrics,num_terminations_events,
                            '57', insight_reason_headcount,insight_score_headcount,insight_type_headcount,headcount_metrics,headcount_events,
                            '59', insight_reason_leave_percentage,insight_score_leave_percentage,insight_type_leave_percentage,leave_percentage_metrics,leave_percentage_events
                          ) metrics AS metric_ky, insight_reason, insight_score, insight_type, insight_metrics, insight_events
   UNION ALL
   SELECT
     clnt_obj_id,
     clnt_obj_id_r,
     yr_cd,
     yr_cd_r,
     mnth_seq_nbr,
     mnth_seq_nbr_r,
     qtr_seq_nbr,
     qtr_seq_nbr_r, 
     job_cd,
     job_cd_r,
     hr_orgn_id,
     hr_orgn_id_r,
     work_cntry_cd,
     work_cntry_cd_r,
     work_state_cd,
     work_state_cd_r,
     work_loc_cd,
     work_loc_cd_r,
     mngr_pers_obj_id,
     mngr_pers_obj_id_r,
     metrics.metric_ky,
     metrics.insight_reason,
     metrics.insight_score,
     metrics.insight_type,
     metrics.insight_events,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS percentile_rank,
     ins_empl_cnt,
     ins_empl_cnt_r,
     empl_count,
     empl_count_r,
     pctg_empl_count,
     pctg_empl_count_r,
     num_dimensions,
     CAST(metrics.insight_metrics[0] AS DOUBLE) AS metric_value,
     CAST(metrics.insight_metrics[1] AS DOUBLE) AS metric_value_r,
     CAST(metrics.insight_metrics[4] AS DOUBLE) AS diff,
     CAST(metrics.insight_metrics[5] AS DOUBLE) AS percentage_diff,
     CAST(metrics.insight_metrics[6] AS DOUBLE) AS normalised_diff,
     CAST(metrics.insight_metrics[7] AS DOUBLE) AS normalised_percentage_diff,
     CAST(metrics.insight_metrics[8] AS DOUBLE) AS zscore_diff,
     CAST(metrics.insight_metrics[9] AS DOUBLE) AS zscore_percentage_diff,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS normalised_zscore_diff,
     CAST(metrics.insight_metrics[11] AS DOUBLE) AS normalised_zscore_percentage_diff,
     CAST(metrics.insight_metrics[13] AS DOUBLE) AS min_metric_value,
     CAST(metrics.insight_metrics[14] AS DOUBLE) AS max_metric_value,
     CAST(metrics.insight_metrics[15] AS DOUBLE) AS min_max_zscore,
     CAST(metrics.insight_metrics[16] AS DOUBLE) AS min_max_zscore_norm,
     CONCAT(
       "{ \"this\": ", COALESCE(CAST(metrics.insight_metrics[0] AS STRING),'null'),
        ",\"that\": ", COALESCE(CAST(metrics.insight_metrics[1] AS STRING),'null'),
        ",\"this_events\": ", COALESCE(CAST(metrics.insight_metrics[2] AS STRING),'null'),
         ",\"that_events\": ", COALESCE(CAST(metrics.insight_metrics[3] AS STRING),'null'),
         ",\"insight\": {\"diff\" : ", COALESCE(CAST(metrics.insight_metrics[4] AS STRING),'null'),
         ",\"percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[5] AS STRING),'null'),
         ",\"normalised_diff\" : ", COALESCE(CAST(metrics.insight_metrics[6] AS STRING),'null'),
         ",\"normalised_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[7] AS STRING),'null'),
         ",\"ins_empl_cnt\" : ", COALESCE(CAST(ins_empl_cnt AS STRING),'null'),
         ",\"ins_empl_cnt_cmpr_with\" : ", COALESCE(CAST(ins_empl_cnt_r AS STRING),'null'),
         ",\"zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[8] AS STRING),'null'),
         ",\"zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[9] AS STRING),'null'),
         ",\"normalised_zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[10] AS STRING),'null'),
         ",\"normalised_zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[11] AS STRING),'null'),
         ",\"percentile_rank\" : ", COALESCE(CAST(metrics.insight_metrics[12] AS STRING),'null'),
         ",\"percentile_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[15] AS STRING),'null'),
         ",\"percentile_normalised_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[16] AS STRING),'null'),
         ",\"mean_diff\" : ", COALESCE(CAST(metrics.insight_metrics[17] AS STRING),'null'),
         ",\"stddev_diff\" : ", COALESCE(CAST(metrics.insight_metrics[18] AS STRING),'null'),
         ",\"mean_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[19] AS STRING),'null'),
         ",\"stddev_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[20] AS STRING),'null'),
         ",\"mean_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[21] AS STRING),'null'),
         ",\"stddev_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[22] AS STRING),'null'),
         ",\"mean_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[23] AS STRING),'null'),
         ",\"stddev_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[24] AS STRING),'null'),
         ",\"percentile_mean\" : ", COALESCE(CAST(metrics.insight_metrics[25] AS STRING),'null'),
         ",\"percentile_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[26] AS STRING),'null'),
         ",\"percentile_norm_mean\" : ", COALESCE(CAST(metrics.insight_metrics[27] AS STRING),'null'),
         ",\"percentile_norm_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[28] AS STRING),'null'), "}}"
     ) AS insights_json,
     3 AS dmn_ky,
     environment
     FROM
       (SELECT
          clnt_obj_id,
          clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr as int)    AS yr_cd,
          cast(yr_seq_nbr_r as int)    AS yr_cd_r,      
          d_job_cd        AS job_cd,
          d_job_cd_r      AS job_cd_r,
          d_hr_orgn_id    AS hr_orgn_id,
          d_hr_orgn_id_r  AS hr_orgn_id_r,
          d_work_cntry_cd AS work_cntry_cd,
          d_work_cntry_cd_r AS work_cntry_cd_r,
          d_work_loc_cd   AS work_loc_cd,
          d_work_loc_cd_r AS work_loc_cd_r,
          d_work_state_cd AS work_state_cd,
          d_work_state_cd_r AS work_state_cd_r,
          mngr_pers_obj_id,
          mngr_pers_obj_id_r,
          insight_reason_overtime_earnings,
          insight_score_overtime_earnings,
          insight_type_overtime_earnings,
          overtime_earnings_metrics,
          overtime_earnings_events,
          insight_reason_average_earnings,
          insight_score_average_earnings,
          insight_type_average_earnings,
          average_earnings_metrics,
          average_earnings_events,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          headcount                   AS empl_count,
          headcount_r                 AS empl_count_r,
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r      AS pctg_empl_count_r,
          num_dimensions,
          environment
        FROM ${__BLUE_MAIN_DB__}.emi_ins_pr_pef_manager
        DISTRIBUTE BY clnt_obj_id
       ) pr
      LATERAL VIEW stack(2,
                            '202', insight_reason_overtime_earnings,insight_score_overtime_earnings,insight_type_overtime_earnings,overtime_earnings_metrics,overtime_earnings_events,
                            '201', insight_reason_average_earnings,insight_score_average_earnings,insight_type_average_earnings,average_earnings_metrics,average_earnings_events
                            ) metrics AS metric_ky, insight_reason, insight_score, insight_type, insight_metrics, insight_events
  ) rosie
   INNER JOIN (select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_hr_waf
            --where environment='${environment}'
            )fact_sch
    ON rosie.clnt_obj_id = fact_sch.clnt_obj_id
    LEFT OUTER JOIN
     (SELECT DISTINCT CASE WHEN qtr_cd='' THEN NULL ELSE qtr_cd END AS qtr_cd,
      cast(qtr_seq_nbr as int) as qtr_seq_nbr
      FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
     ) lqtr
    ON rosie.qtr_seq_nbr <=> lqtr.qtr_seq_nbr
    LEFT OUTER JOIN
     (SELECT DISTINCT CASE WHEN qtr_cd='' THEN NULL ELSE qtr_cd END AS qtr_cd,
      cast(qtr_seq_nbr as int) as qtr_seq_nbr
      FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
     ) rqtr
     ON rosie.qtr_seq_nbr_r <=> rqtr.qtr_seq_nbr
    LEFT OUTER JOIN
     (SELECT DISTINCT CASE WHEN mnth_cd='' THEN NULL ELSE mnth_cd END AS mnth_cd,
       cast(mnth_seq_nbr as int) as mnth_seq_nbr
      FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
     ) lmnth
     ON rosie.mnth_seq_nbr <=> lmnth.mnth_seq_nbr
    LEFT OUTER JOIN
     (SELECT DISTINCT CASE WHEN mnth_cd='' THEN NULL ELSE mnth_cd END AS mnth_cd,
      cast(mnth_seq_nbr as int) as mnth_seq_nbr
      FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
     ) rmnth
     ON rosie.mnth_seq_nbr_r <=> rmnth.mnth_seq_nbr
    INNER JOIN ${__RO_GREEN_RAW_DB__}.emi_meta_dw_metrics meta
     ON rosie.metric_ky = meta.metric_ky
    WHERE mngr_pers_obj_id IS NOT NULL
        --apply event count filter
        AND (insight_events >=3 OR insight_events IS NULL)
        --apply zscore filter
        AND (CASE WHEN insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') THEN (min_max_zscore > 2 OR min_max_zscore < -2 OR min_max_zscore_norm > 2 OR min_max_zscore_norm < -2)
            ELSE (zscore_diff > 1 OR zscore_diff < -1 OR normalised_zscore_diff > 1 OR normalised_zscore_diff < -1 OR zscore_percentage_diff > 1 OR zscore_percentage_diff < -1 OR normalised_zscore_percentage_diff > 1 OR normalised_zscore_percentage_diff < -1 ) END)
        -- Filter insights, that do not contain mandatory dimensions - job_cd, work_contry_cd, hr_orgn_id
        --AND (job_cd IS NOT NULL OR work_cntry_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)
        AND insight_reason <> 'NO_INSIGHT'
        AND metric_value <> 0
        AND metric_value IS NOT NULL
        AND (diff <> 0 OR diff is NULL)
        AND meta.is_included = 1
        -- Filter insights that have 'unknown' value in dimension columns
        AND (CASE WHEN job_cd IS NULL THEN lower(coalesce(job_cd, '')) != 'unknown' ELSE (trim(job_cd)!='' AND lower(trim(job_cd)) != 'unknown') END)
        AND (CASE WHEN hr_orgn_id IS NULL THEN lower(coalesce(hr_orgn_id, '')) != 'unknown' ELSE (trim(hr_orgn_id)!='' AND lower(trim(hr_orgn_id)) != 'unknown') END)
        AND (CASE WHEN work_state_cd IS NULL THEN lower(coalesce(work_state_cd, '')) != 'unknown'  ELSE (trim(work_state_cd)!='' AND lower(trim(work_state_cd)) != 'unknown') END)
        AND (CASE WHEN work_loc_cd IS NULL THEN lower(coalesce(work_loc_cd, '')) != 'unknown' ELSE (trim(work_loc_cd)!='' AND lower(trim(work_loc_cd)) != 'unknown') END)
        -- Filter insights at country level. i.e, work_cntry_cd Is Not Null and work_state_cd Is Null
        AND (CASE WHEN work_cntry_cd IS NOT NULL THEN work_state_cd ELSE 'DUMMY' END) IS NOT NULL
        -- Filter insights for manager where all employees under him present in single location/job/department
        AND (CASE WHEN((insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN min_metric_value ELSE "DUMMY" END) <> (CASE WHEN((insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN max_metric_value ELSE "DUMMY1" END)
)mngr_int_ins
LEFT OUTER JOIN (select distinct curr_yr,curr_qtr,curr_mnth,curr_qtr_num,day_cd,environment from ${__BLUE_MAIN_DB__}.t_dim_today) today
    ON mngr_int_ins.environment=today.environment
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.t_fiscal_info clnt_fisc_info
 ON  mngr_int_ins.clnt_obj_id = clnt_fisc_info.clnt_obj_id
 AND  mngr_int_ins.db_schema = clnt_fisc_info.db_schema
 AND  mngr_int_ins.environment = clnt_fisc_info.environment
 AND  mngr_int_ins.dmn_ky = clnt_fisc_info.dmn_ky
WHERE  
  (CASE WHEN (mngr_int_ins.clnt_obj_id_r IS NOT NULL)
   THEN (CASE WHEN mngr_int_ins.metric_ky IN(57,74,63,73) THEN abs(diff)/ins_empl_cnt>=0.1
  WHEN mngr_int_ins.metric_ky IN(202) THEN abs(diff)/ins_empl_cnt>=1
  WHEN mngr_int_ins.metric_ky IN(76,69,59,201,78,65) THEN ((abs(diff) > (ins_empl_cnt * 0.01)) AND (round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) < 0.9 OR round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) > 1.1))
  WHEN mngr_int_ins.metric_ky=79 THEN abs(diff) >= 1
  ELSE 1=1 END)
  ELSE 1=1 END)
  --For DIFF based insights, time dimensions should not be same
  AND ((clnt_obj_id_r IS NOT NULL AND NOT (mngr_int_ins.yr_cd <=> yr_cd_r AND mngr_int_ins.qtr_cd <=> qtr_cd_r AND mngr_int_ins.mnth_cd <=> mnth_cd_r) AND insight_score <>  0)
            OR (clnt_obj_id_r IS NULL))
  AND ((mngr_int_ins.qtr_seq_nbr IS NULL AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr_num ELSE clnt_fisc_info.qtr_in_yr_nbr END) < 2) AND (CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN
            CASE WHEN (mngr_int_ins.yr_cd_r is not null AND mngr_int_ins.yr_cd != mngr_int_ins.yr_cd_r)  THEN (today.curr_yr - mngr_int_ins.yr_cd_r = 2) ELSE 1=1 END
            ELSE (CASE WHEN (mngr_int_ins.yr_cd_r is not null AND mngr_int_ins.yr_cd != mngr_int_ins.yr_cd_r) THEN (curr_clnt_fisc_yr - mngr_int_ins.yr_cd_r = 2 ) ELSE 1=1 END)END) AND (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - mngr_int_ins.yr_cd = 1) AND (mngr_int_ins.yr_cd_r IS NULL OR (mngr_int_ins.yr_cd - mngr_int_ins.yr_cd_r = 1)))) OR
            ((mngr_int_ins.mnth_seq_nbr IS NULL) AND (CASE WHEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - (CASE WHEN mngr_int_ins.clnt_obj_id_r IS NULL THEN mngr_int_ins.yr_cd ELSE mngr_int_ins.yr_cd_r END) = 0) THEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr ELSE curr_clnt_fisc_qtr  END) - mngr_int_ins.qtr_seq_nbr) = 1
            WHEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - (CASE WHEN mngr_int_ins.clnt_obj_id_r IS NULL THEN mngr_int_ins.yr_cd ELSE mngr_int_ins.yr_cd_r END) = 1) THEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr ELSE curr_clnt_fisc_qtr  END) - mngr_int_ins.qtr_seq_nbr) = 1
            WHEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - mngr_int_ins.yr_cd_r = 1) AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - mngr_int_ins.yr_cd = 0)) THEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr ELSE curr_clnt_fisc_qtr  END) - 1) - mngr_int_ins.qtr_seq_nbr_r) = 4
            WHEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - mngr_int_ins.yr_cd_r = 2) AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - mngr_int_ins.yr_cd = 1)) THEN ((((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr ELSE curr_clnt_fisc_qtr  END) - 1) - mngr_int_ins.qtr_seq_nbr_r) = 4 and (mngr_int_ins.qtr_seq_nbr - mngr_int_ins.qtr_seq_nbr_r) != 1)ELSE 1=0 END)) OR
            (CASE WHEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - (CASE WHEN mngr_int_ins.clnt_obj_id_r IS NULL THEN mngr_int_ins.yr_cd ELSE mngr_int_ins.yr_cd_r END) = 0) THEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_mnth ELSE curr_clnt_fisc_mnth  END) - mngr_int_ins.mnth_seq_nbr) = 1
                  WHEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - (CASE WHEN mngr_int_ins.clnt_obj_id_r IS NULL THEN mngr_int_ins.yr_cd ELSE mngr_int_ins.yr_cd_r END) = 1) THEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_mnth ELSE curr_clnt_fisc_mnth  END) - mngr_int_ins.mnth_seq_nbr) = 1
                  WHEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - mngr_int_ins.yr_cd_r = 1) AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - mngr_int_ins.yr_cd = 0)) THEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_mnth ELSE curr_clnt_fisc_mnth  END) - 1) - mngr_int_ins.mnth_seq_nbr_r)= 12
                  WHEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - mngr_int_ins.yr_cd_r = 2) AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - mngr_int_ins.yr_cd = 1)) THEN ((((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_mnth ELSE curr_clnt_fisc_mnth  END) - 1) - mngr_int_ins.mnth_seq_nbr_r)= 12 and (mngr_int_ins.mnth_seq_nbr - mngr_int_ins.mnth_seq_nbr_r) != 1)
                  ELSE 1 = 0 END))

UNION ALL

--Practitioner insights
SELECT
    /*+ BROADCAST(clnt_fisc_info,today) */
    md5(CONCAT(
    COALESCE(split(prac_int_ins.db_schema,'[|]')[0],'-'),
    COALESCE(prac_int_ins.clnt_obj_id,'-'),
    COALESCE(clnt_obj_id_r,'-'),
    COALESCE(yr_cd,'-'),
    COALESCE(yr_cd_r,'-'),
    COALESCE(qtr_cd,'-'),
    COALESCE(qtr_cd_r,'-'),
    COALESCE(mnth_cd,'-'),
    COALESCE(mnth_cd_r,'-'),
    COALESCE(hr_orgn_id,'-'),
    COALESCE(hr_orgn_id_r,'-'),
    COALESCE(job_cd,'-'),
    COALESCE(job_cd_r,'-'),
    COALESCE(work_cntry_cd,'-'),
    COALESCE(work_cntry_cd_r,'-'),
    COALESCE(work_state_cd,'-'),
    COALESCE(work_state_cd_r,'-'),
    COALESCE(work_loc_cd,'-'),
    COALESCE(work_loc_cd_r,'-'),
    COALESCE(trmnt_rsn,'-'),
    COALESCE(trmnt_rsn_r,'-'),
    COALESCE(metric_ky,'-'),
    COALESCE(insight_type,'-'),
    COALESCE(CASE WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = min_metric_value THEN 'MIN_PERCENTILE_RANKING'
                  WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = max_metric_value THEN 'MAX_PERCENTILE_RANKING'
                  ELSE insight_reason END,'-')
    )) as insight_hash,
    prac_int_ins.clnt_obj_id,
    clnt_obj_id_r,
    CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN yr_cd ELSE CONCAT('FY',yr_cd) END as yr_cd,
    CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN yr_cd_r ELSE CONCAT('FY',yr_cd_r) END as yr_cd_r,
    qtr_cd,
    qtr_cd_r,
    mnth_cd,
    mnth_cd_r,
    NULL AS wk_cd,
    NULL AS wk_cd_r,
    NULL AS flsa_stus_cd,
    NULL AS flsa_stus_cd_r,
    NULL AS full_tm_part_tm_cd,
    NULL AS full_tm_part_tm_cd_r,
    NULL AS gndr_cd,
    NULL AS gndr_cd_r,
    hr_orgn_id,
    hr_orgn_id_r,
    job_cd,
    job_cd_r,
    NULL AS pay_rt_type_cd,
    NULL AS pay_rt_type_cd_r,
    NULL AS reg_temp_cd,
    NULL AS reg_temp_cd_r,
    work_loc_cd,
    work_loc_cd_r,
    NULL AS work_city_cd,
    NULL AS work_city_cd_r,
    work_state_cd,
    work_state_cd_r,
    work_cntry_cd,
    work_cntry_cd_r,
    trmnt_rsn,
    trmnt_rsn_r,
    NULL AS adp_lens_cd,
    NULL AS adp_lens_cd_r,
    NULL AS inds_ky,
    NULL AS inds_ky_r,
    NULL AS sector_cd,
    NULL AS sector_cd_r,
    NULL AS supersector_cd,
    NULL AS supersector_cd_r,
    NULL AS mngr_pers_obj_id,
    NULL AS mngr_pers_obj_id_r,
    CASE WHEN (prac_int_ins.metric_ky=63 AND trmnt_rsn IS NOT NULL) THEN 73 ELSE prac_int_ins.metric_ky END AS metric_ky,
    (insight_score * metric_wgt *
    (CASE WHEN job_cd IS NOT NULL THEN job_cd_wgt ELSE 1 END)*
    (CASE WHEN qtr_seq_nbr IS NOT NULL THEN qtr_seq_nbr_wgt ELSE 1 END)*
    (CASE WHEN mnth_seq_nbr IS NOT NULL THEN mnth_seq_nbr_wgt ELSE 1 END)*
    (CASE WHEN work_state_cd IS NOT NULL THEN work_state_cd_wgt ELSE 1 END)*
    (CASE WHEN work_loc_cd IS NOT NULL THEN work_loc_cd_wgt ELSE 1 END)*
    (CASE WHEN hr_orgn_id IS NOT NULL THEN hr_orgn_id_wgt ELSE 1 END)*
    (CASE WHEN trmnt_rsn IS NOT NULL THEN trmnt_rsn_wgt ELSE 1 END)
    ) AS insight_score,
    percentile_rank,
    CASE WHEN (prac_int_ins.metric_ky<>53 AND insight_reason not in('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') AND  job_cd IS NULL AND hr_orgn_id IS NULL AND work_cntry_cd IS NULL AND trmnt_rsn IS NULL) THEN 'CLIENT_INTERNAL_MYTEAM' ELSE insight_type END as insight_type,
    insight_events,
    CASE WHEN insight_reason IN ('NORM_PCTG_DIFF','PCTG_DIFF') THEN
           CASE WHEN (prac_int_ins.yr_cd <=> prac_int_ins.yr_cd_r AND prac_int_ins.qtr_cd <=> prac_int_ins.qtr_cd_r AND prac_int_ins.mnth_cd <=> prac_int_ins.mnth_cd_r) THEN 'PCTG_DIFF'
           ELSE 'PCTG_DIFF_TIME'  END                   
       WHEN insight_reason  IN ('NORM_ABS_DIFF','ABS_DIFF') THEN
           CASE WHEN (prac_int_ins.yr_cd <=> prac_int_ins.yr_cd_r AND prac_int_ins.qtr_cd <=> prac_int_ins.qtr_cd_r AND prac_int_ins.mnth_cd <=> prac_int_ins.mnth_cd_r) THEN 'ABS_DIFF'
           ELSE 'ABS_DIFF_TIME' END
       WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = min_metric_value THEN 'MIN_PERCENTILE_RANKING'
       WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = max_metric_value THEN 'MAX_PERCENTILE_RANKING'
       ELSE insight_reason
    END AS insight_reason,
    ins_empl_cnt,
    ins_empl_cnt_r,
    empl_count,
    empl_count_r,
    pctg_empl_count,
    pctg_empl_count_r,
    num_dimensions,
    NULL AS retn_period_strt_person_cnt,
    NULL AS retn_period_end_person_cnt,
    NULL AS retn_period_strt_person_cnt_r,
    NULL AS retn_period_end_person_cnt_r,
    metric_value,
    metric_value_r,
    diff,
    percentage_diff,
    normalised_diff,
    normalised_percentage_diff,
    zscore_diff,
    zscore_percentage_diff,
    normalised_zscore_diff,
    normalised_zscore_percentage_diff,
    min_max_zscore,
    min_max_zscore_norm,
    min_metric_value,
    max_metric_value,
    insights_json,
    prac_int_ins.dmn_ky as dmn_ky,
    "prac_ins" AS export_type,
    NULL AS excp_type,
    NULL AS excp_type_r,
    NULL AS supvr_pers_obj_id,
    NULL As supvr_pers_obj_id_r,
    NULL AS rpt_access,
    prac_int_ins.db_schema,
    prac_int_ins.environment
FROM
(SELECT
     /*+ BROADCAST(lqtr,rqtr,lmnth,rmnth,emi_meta_dw_metrics) */
     rosie.clnt_obj_id,
     clnt_obj_id_r,
     yr_cd,
     yr_cd_r,
     rosie.mnth_seq_nbr,
     rosie.mnth_seq_nbr_r,
     lmnth.mnth_cd AS mnth_cd,
     rmnth.mnth_cd AS mnth_cd_r,
     rosie.qtr_seq_nbr,
     rosie.qtr_seq_nbr_r,
     lqtr.qtr_cd AS qtr_cd,
     rqtr.qtr_cd AS qtr_cd_r,
     job_cd,
     job_cd_r,
     hr_orgn_id,
     hr_orgn_id_r,
     work_cntry_cd,
     work_cntry_cd_r,
     work_state_cd,
     work_state_cd_r,
     trmnt_rsn,
     trmnt_rsn_r,
     work_loc_cd,
     work_loc_cd_r,
     rosie.metric_ky,
     insight_reason,
     insight_score,
     insight_type,
     insight_events,
     percentile_rank,
     ins_empl_cnt,
     ins_empl_cnt_r,
     empl_count,
     empl_count_r,
     pctg_empl_count,
     pctg_empl_count_r,
     num_dimensions,
     metric_value,
     metric_value_r,
     diff,
     percentage_diff,
     normalised_diff,
     normalised_percentage_diff,
     zscore_diff,
     zscore_percentage_diff,
     normalised_zscore_diff,
     normalised_zscore_percentage_diff,
     min_max_zscore,
     min_max_zscore_norm,
     min_metric_value,
     max_metric_value,
     insights_json,
     dmn_ky,
     metric_wgt,
     job_cd_wgt ,
     gndr_cd_wgt ,
     qtr_seq_nbr_wgt,
     mnth_seq_nbr_wgt,
     work_state_cd_wgt,
     work_loc_cd_wgt,
     hr_orgn_id_wgt,
     trmnt_rsn_wgt,
     fact_sch.db_schema,
     rosie.environment
FROM
(SELECT
     clnt_obj_id,
     clnt_obj_id_r,
     yr_cd,
     yr_cd_r,
    mnth_seq_nbr,
     mnth_seq_nbr_r,
     qtr_seq_nbr,
     qtr_seq_nbr_r, 
     hr_orgn_id,
     hr_orgn_id_r,
     job_cd,
     job_cd_r, 
     work_cntry_cd,
     work_cntry_cd_r,
     work_loc_cd,
     work_loc_cd_r,
     work_state_cd,
     work_state_cd_r,
     trmnt_rsn,
     trmnt_rsn_r,
     metrics.metric_ky,
     metrics.insight_reason,
     metrics.insight_score,
     metrics.insight_type,
     metrics.insight_events,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS percentile_rank,
     ins_empl_cnt,
     ins_empl_cnt_r,
     CASE WHEN (metrics.metric_ky = '59') THEN (CASE WHEN hr.empl_count=0 THEN hr.ins_empl_cnt ELSE hr.empl_count END)
     ELSE hr.empl_count END AS empl_count,
     CASE WHEN (metrics.metric_ky = '59') THEN (CASE WHEN hr.empl_count_r=0 THEN hr.ins_empl_cnt_r ELSE hr.empl_count_r END)
     ELSE hr.empl_count_r END AS empl_count_r,
     pctg_empl_count,
     pctg_empl_count_r,
     num_dimensions,
     CAST(metrics.insight_metrics[0] AS DOUBLE) AS metric_value,
     CAST(metrics.insight_metrics[1] AS DOUBLE) AS metric_value_r,
     CAST(metrics.insight_metrics[4] AS DOUBLE) AS diff,
     CAST(metrics.insight_metrics[5] AS DOUBLE) AS percentage_diff,
     CAST(metrics.insight_metrics[6] AS DOUBLE) AS normalised_diff,
     CAST(metrics.insight_metrics[7] AS DOUBLE) AS normalised_percentage_diff,
     CAST(metrics.insight_metrics[8] AS DOUBLE) AS zscore_diff,
     CAST(metrics.insight_metrics[9] AS DOUBLE) AS zscore_percentage_diff,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS normalised_zscore_diff,
     CAST(metrics.insight_metrics[11] AS DOUBLE) AS normalised_zscore_percentage_diff,
     CAST(metrics.insight_metrics[13] AS DOUBLE) AS min_metric_value,
     CAST(metrics.insight_metrics[14] AS DOUBLE) AS max_metric_value,
     CAST(metrics.insight_metrics[15] AS DOUBLE) AS min_max_zscore,
     CAST(metrics.insight_metrics[16] AS DOUBLE) AS min_max_zscore_norm,
     CONCAT(
       "{ \"this\": ", COALESCE(CAST(metrics.insight_metrics[0] AS STRING),'null'),
        ",\"that\": ", COALESCE(CAST(metrics.insight_metrics[1] AS STRING),'null'),
         ",\"this_events\": ", COALESCE(CAST(metrics.insight_metrics[2] AS STRING),'null'),
         ",\"that_events\": ", COALESCE(CAST(metrics.insight_metrics[3] AS STRING),'null'),
         ",\"insight\": {\"diff\" : ", COALESCE(CAST(metrics.insight_metrics[4] AS STRING),'null'),
         ",\"percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[5] AS STRING),'null'),
         ",\"normalised_diff\" : ", COALESCE(CAST(metrics.insight_metrics[6] AS STRING),'null'),
         ",\"normalised_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[7] AS STRING),'null'),
         ",\"ins_empl_cnt\" : ", COALESCE(CAST(ins_empl_cnt AS STRING),'null'),
         ",\"ins_empl_cnt_cmpr_with\" : ", COALESCE(CAST(ins_empl_cnt_r AS STRING),'null'),
         ",\"zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[8] AS STRING),'null'),
         ",\"zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[9] AS STRING),'null'),
         ",\"normalised_zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[10] AS STRING),'null'),
         ",\"normalised_zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[11] AS STRING),'null'),
         ",\"percentile_rank\" : ", COALESCE(CAST(metrics.insight_metrics[12] AS STRING),'null'),
         ",\"percentile_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[15] AS STRING),'null'),
         ",\"percentile_normalised_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[16] AS STRING),'null'),
         ",\"mean_diff\" : ", COALESCE(CAST(metrics.insight_metrics[17] AS STRING),'null'),
         ",\"stddev_diff\" : ", COALESCE(CAST(metrics.insight_metrics[18] AS STRING),'null'),
         ",\"mean_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[19] AS STRING),'null'),
         ",\"stddev_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[20] AS STRING),'null'),
         ",\"mean_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[21] AS STRING),'null'),
         ",\"stddev_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[22] AS STRING),'null'),
         ",\"mean_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[23] AS STRING),'null'),
         ",\"stddev_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[24] AS STRING),'null'),
         ",\"percentile_mean\" : ", COALESCE(CAST(metrics.insight_metrics[25] AS STRING),'null'),
         ",\"percentile_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[26] AS STRING),'null'),
         ",\"percentile_norm_mean\" : ", COALESCE(CAST(metrics.insight_metrics[27] AS STRING),'null'),
         ",\"percentile_norm_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[28] AS STRING),'null'), "}}"
     ) AS insights_json,
     2 AS dmn_ky,
     environment
   FROM
     (SELECT clnt_obj_id,
        clnt_obj_id_r,
        mnth_seq_nbr,
        mnth_seq_nbr_r,
        qtr_seq_nbr,
        qtr_seq_nbr_r,
        cast(yr_seq_nbr as int)           AS yr_cd,
        cast(yr_seq_nbr_r as int)         AS yr_cd_r,    
        d_hr_orgn_id           AS hr_orgn_id,
        d_hr_orgn_id_r         AS hr_orgn_id_r,
        d_job_cd               AS job_cd,
        d_job_cd_r             AS job_cd_r,
        NULL                   AS pay_rt_type_cd,
        NULL                   AS pay_rt_type_cd_r,
        d_work_cntry_cd        AS work_cntry_cd,
        d_work_cntry_cd_r      AS work_cntry_cd_r,
        d_work_loc_cd          AS work_loc_cd,
        d_work_loc_cd_r        AS work_loc_cd_r,
        d_work_state_cd        AS work_state_cd,
        d_work_state_cd_r      AS work_state_cd_r,
        d_trmnt_rsn            AS trmnt_rsn,
        d_trmnt_rsn_r          AS trmnt_rsn_r,
        insight_score_turnover_rate,
        insight_type_turnover_rate,
        insight_reason_turnover_rate,
        turnover_rate_metrics,
        turnover_rate_events,
        insight_reason_voln_turnover_rate,
        insight_score_voln_turnover_rate,
        insight_type_voln_turnover_rate,
        voln_turnover_rate_metrics,
        voln_turnover_rate_events,
        insight_reason_internal_mobility_rate,
        insight_score_internal_mobility_rate,
        insight_type_internal_mobility_rate,
        internal_mobility_rate_metrics,
        internal_mobility_rate_events,
        insight_reason_newhire_turnover_rate,
        insight_score_newhire_turnover_rate,
        insight_type_newhire_turnover_rate,
        newhire_turnover_rate_metrics,
        newhire_turnover_rate_events,
        insight_reason_average_tenure,
        insight_score_average_tenure,
        insight_type_average_tenure,
        average_tenure_metrics,
        average_tenure_events,
        insight_reason_avg_time_to_promotion,
        insight_score_avg_time_to_promotion,
        insight_type_avg_time_to_promotion,
        avg_time_to_promotion_metrics,
        avg_time_to_promotion_events,
        insight_reason_retention_rate,
        insight_score_retention_rate,
        insight_type_retention_rate,
        retention_rate_metrics,
        retention_rate_events,
        insight_reason_num_hires,
        insight_score_num_hires,
        insight_type_num_hires,
        num_hires_metrics,
        num_hires_events,
        insight_reason_num_terminations,
        insight_score_num_terminations,
        insight_type_num_terminations,
        num_terminations_metrics,
        num_terminations_events,
        insight_reason_headcount,
        insight_score_headcount,
        insight_type_headcount,
        headcount_metrics,
        headcount_events,
        insight_reason_leave_percentage,
        insight_score_leave_percentage,
        insight_type_leave_percentage,
        leave_percentage_metrics,
        leave_percentage_events,
        num_employees as ins_empl_cnt,
        num_employees_r as ins_empl_cnt_r,
        tot_rpt_headcount                   AS empl_count,
        tot_rpt_headcount_r                 AS empl_count_r,
        percentage_headcount        AS pctg_empl_count,
        percentage_headcount_r      AS pctg_empl_count_r,
        num_dimensions,
        environment
      FROM ${__BLUE_MAIN_DB__}.emi_ins_hr_waf_practitioner
      DISTRIBUTE BY clnt_obj_id
      ) hr
    LATERAL VIEW stack (11,  -- Total number of rows each row is exploded to
                            '76', insight_reason_turnover_rate,insight_score_turnover_rate,insight_type_turnover_rate,turnover_rate_metrics,turnover_rate_events,
                            '53', insight_reason_voln_turnover_rate,insight_score_voln_turnover_rate,insight_type_voln_turnover_rate,voln_turnover_rate_metrics,voln_turnover_rate_events,
                            '65', insight_reason_internal_mobility_rate,insight_score_internal_mobility_rate,insight_type_internal_mobility_rate,internal_mobility_rate_metrics,internal_mobility_rate_events,
                            '69', insight_reason_newhire_turnover_rate,insight_score_newhire_turnover_rate,insight_type_newhire_turnover_rate,newhire_turnover_rate_metrics,newhire_turnover_rate_events,
                            '60', insight_reason_average_tenure,insight_score_average_tenure,insight_type_average_tenure,average_tenure_metrics,average_tenure_events,
                            '79', insight_reason_avg_time_to_promotion,insight_score_avg_time_to_promotion,insight_type_avg_time_to_promotion,avg_time_to_promotion_metrics,avg_time_to_promotion_events,
                            '78', insight_reason_retention_rate,insight_score_retention_rate,insight_type_retention_rate,retention_rate_metrics,retention_rate_events,
                            '74', insight_reason_num_hires,insight_score_num_hires,insight_type_num_hires,num_hires_metrics,num_hires_events,
                            '63', insight_reason_num_terminations,insight_score_num_terminations,insight_type_num_terminations,num_terminations_metrics,num_terminations_events,
                            '57', insight_reason_headcount,insight_score_headcount,insight_type_headcount,headcount_metrics,headcount_events,
                            '59', insight_reason_leave_percentage,insight_score_leave_percentage,insight_type_leave_percentage,leave_percentage_metrics,leave_percentage_events
                          ) metrics AS metric_ky, insight_reason, insight_score, insight_type, insight_metrics, insight_events
   UNION ALL
   SELECT
   clnt_obj_id,
   clnt_obj_id_r,
   yr_cd,
   yr_cd_r,
   mnth_seq_nbr,
   mnth_seq_nbr_r,
   qtr_seq_nbr,
   qtr_seq_nbr_r,
   hr_orgn_id,
   hr_orgn_id_r,
   job_cd,
   job_cd_r,
   work_cntry_cd,
   work_cntry_cd_r,
   work_loc_cd,
   work_loc_cd_r,
   work_state_cd,
   work_state_cd_r,
   trmnt_rsn,
   trmnt_rsn_r,
   metrics.metric_ky,
   metrics.insight_reason,
   metrics.insight_score,
   metrics.insight_type,
   metrics.insight_events,
   CAST(metrics.insight_metrics[10] AS DOUBLE) AS percentile_rank,
   ins_empl_cnt,
   ins_empl_cnt_r,
   empl_count,
   empl_count_r,
   pctg_empl_count,
   pctg_empl_count_r,
   num_dimensions,
   CAST(metrics.insight_metrics[0] AS DOUBLE) AS metric_value,
   CAST(metrics.insight_metrics[1] AS DOUBLE) AS metric_value_r,
   CAST(metrics.insight_metrics[4] AS DOUBLE) AS diff,
   CAST(metrics.insight_metrics[5] AS DOUBLE) AS percentage_diff,
   CAST(metrics.insight_metrics[6] AS DOUBLE) AS normalised_diff,
   CAST(metrics.insight_metrics[7] AS DOUBLE) AS normalised_percentage_diff,
   CAST(metrics.insight_metrics[8] AS DOUBLE) AS zscore_diff,
   CAST(metrics.insight_metrics[9] AS DOUBLE) AS zscore_percentage_diff,
   CAST(metrics.insight_metrics[10] AS DOUBLE) AS normalised_zscore_diff,
   CAST(metrics.insight_metrics[11] AS DOUBLE) AS normalised_zscore_percentage_diff,
   CAST(metrics.insight_metrics[13] AS DOUBLE) AS min_metric_value,
   CAST(metrics.insight_metrics[14] AS DOUBLE) AS max_metric_value,
   CAST(metrics.insight_metrics[15] AS DOUBLE) AS min_max_zscore,
   CAST(metrics.insight_metrics[16] AS DOUBLE) AS min_max_zscore_norm,
   CONCAT(
       "{ \"this\": ", COALESCE(CAST(metrics.insight_metrics[0] AS STRING),'null'),
        ",\"that\": ", COALESCE(CAST(metrics.insight_metrics[1] AS STRING),'null'),
       ",\"this_events\": ", COALESCE(CAST(metrics.insight_metrics[2] AS STRING),'null'),
         ",\"that_events\": ", COALESCE(CAST(metrics.insight_metrics[3] AS STRING),'null'),
         ",\"insight\": {\"diff\" : ", COALESCE(CAST(metrics.insight_metrics[4] AS STRING),'null'),
         ",\"percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[5] AS STRING),'null'),
         ",\"normalised_diff\" : ", COALESCE(CAST(metrics.insight_metrics[6] AS STRING),'null'),
         ",\"normalised_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[7] AS STRING),'null'),
         ",\"ins_empl_cnt\" : ", COALESCE(CAST(ins_empl_cnt AS STRING),'null'),
         ",\"ins_empl_cnt_cmpr_with\" : ", COALESCE(CAST(ins_empl_cnt_r AS STRING),'null'),
         ",\"zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[8] AS STRING),'null'),
         ",\"zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[9] AS STRING),'null'),
         ",\"normalised_zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[10] AS STRING),'null'),
         ",\"normalised_zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[11] AS STRING),'null'),
         ",\"percentile_rank\" : ", COALESCE(CAST(metrics.insight_metrics[12] AS STRING),'null'),
         ",\"percentile_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[15] AS STRING),'null'),
         ",\"percentile_normalised_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[16] AS STRING),'null'),
         ",\"mean_diff\" : ", COALESCE(CAST(metrics.insight_metrics[17] AS STRING),'null'),
         ",\"stddev_diff\" : ", COALESCE(CAST(metrics.insight_metrics[18] AS STRING),'null'),
         ",\"mean_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[19] AS STRING),'null'),
         ",\"stddev_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[20] AS STRING),'null'),
         ",\"mean_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[21] AS STRING),'null'),
         ",\"stddev_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[22] AS STRING),'null'),
         ",\"mean_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[23] AS STRING),'null'),
         ",\"stddev_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[24] AS STRING),'null'),
         ",\"percentile_mean\" : ", COALESCE(CAST(metrics.insight_metrics[25] AS STRING),'null'),
         ",\"percentile_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[26] AS STRING),'null'),
         ",\"percentile_norm_mean\" : ", COALESCE(CAST(metrics.insight_metrics[27] AS STRING),'null'),
         ",\"percentile_norm_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[28] AS STRING),'null'), "}}"
     ) AS insights_json,
     3 AS dmn_ky,
     environment
   FROM
     (SELECT
        clnt_obj_id,
        clnt_obj_id_r,
        mnth_seq_nbr,
        mnth_seq_nbr_r,
        qtr_seq_nbr,
        qtr_seq_nbr_r,
        cast(yr_seq_nbr as int) AS yr_cd,
        cast(yr_seq_nbr_r as int) AS yr_cd_r,    
        d_hr_orgn_id AS hr_orgn_id,
        d_hr_orgn_id_r AS hr_orgn_id_r,
        d_job_cd AS job_cd,
        d_job_cd_r AS job_cd_r,
        d_work_cntry_cd AS work_cntry_cd,
        d_work_cntry_cd_r AS work_cntry_cd_r,
        d_work_loc_cd AS work_loc_cd,
        d_work_loc_cd_r AS work_loc_cd_r,
        d_work_state_cd AS work_state_cd,
        d_work_state_cd_r AS work_state_cd_r,
        NULL AS trmnt_rsn,
        NULL AS trmnt_rsn_r,
        insight_reason_overtime_earnings,
        insight_score_overtime_earnings,
        insight_type_overtime_earnings,
        overtime_earnings_metrics,
        overtime_earnings_events,
        insight_reason_average_earnings,
        insight_score_average_earnings,
        insight_type_average_earnings,
        average_earnings_metrics,
        average_earnings_events,
        num_employees as ins_empl_cnt,
        num_employees_r as ins_empl_cnt_r,
        headcount                   AS empl_count,
        headcount_r                 AS empl_count_r,
        percentage_headcount        AS pctg_empl_count,
        percentage_headcount_r      AS pctg_empl_count_r,
        num_dimensions,
        environment
      FROM ${__BLUE_MAIN_DB__}.emi_ins_pr_pef_practitioner
      DISTRIBUTE BY clnt_obj_id
      ) pr
      LATERAL VIEW stack (2,
                            '202', insight_reason_overtime_earnings,insight_score_overtime_earnings,insight_type_overtime_earnings,overtime_earnings_metrics,overtime_earnings_events,
                            '201', insight_reason_average_earnings,insight_score_average_earnings,insight_type_average_earnings,average_earnings_metrics,average_earnings_events
                            ) metrics AS metric_ky, insight_reason, insight_score, insight_type, insight_metrics, insight_events
  ) rosie
  INNER JOIN (select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_hr_waf
            )fact_sch
    ON rosie.clnt_obj_id = fact_sch.clnt_obj_id
  LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN qtr_cd='' THEN NULL ELSE qtr_cd END AS qtr_cd,
      cast(qtr_seq_nbr as int) as qtr_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) lqtr
  ON rosie.qtr_seq_nbr <=> lqtr.qtr_seq_nbr
  LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN qtr_cd='' THEN NULL ELSE qtr_cd END AS qtr_cd,
      cast(qtr_seq_nbr as int) as qtr_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) rqtr
  ON rosie.qtr_seq_nbr_r <=> rqtr.qtr_seq_nbr
  LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN mnth_cd='' THEN NULL ELSE mnth_cd END AS mnth_cd,
      cast(mnth_seq_nbr as int) as mnth_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) lmnth
  ON rosie.mnth_seq_nbr <=> lmnth.mnth_seq_nbr
  LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN mnth_cd='' THEN NULL ELSE mnth_cd END AS mnth_cd,
      cast(mnth_seq_nbr as int) as mnth_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) rmnth
  ON rosie.mnth_seq_nbr_r <=> rmnth.mnth_seq_nbr
  INNER JOIN ${__RO_GREEN_RAW_DB__}.emi_meta_dw_metrics meta
  ON rosie.metric_ky = meta.metric_ky
  WHERE
      --apply event count filter
     (insight_events >=3 OR insight_events IS NULL)
    --apply zscore filter
    AND (CASE WHEN insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') THEN (min_max_zscore > 2 OR min_max_zscore < -2 OR min_max_zscore_norm > 2 OR min_max_zscore_norm < -2)
         ELSE (zscore_diff > 1 OR zscore_diff < -1 OR normalised_zscore_diff > 1 OR normalised_zscore_diff < -1 OR zscore_percentage_diff > 1 OR zscore_percentage_diff < -1 OR normalised_zscore_percentage_diff > 1 OR normalised_zscore_percentage_diff < -1 ) END)
    AND insight_reason <> 'NO_INSIGHT'
    AND metric_value IS NOT NULL
    AND metric_value <> 0
    AND meta.is_included = 1
    AND (diff <> 0 OR diff is NULL)
    -- Filter insights that have 'unknown' value in dimension columns
    AND (CASE WHEN job_cd IS NULL THEN lower(coalesce(job_cd, '')) != 'unknown' ELSE (trim(job_cd)!='' AND lower(trim(job_cd)) != 'unknown') END)
    AND (CASE WHEN hr_orgn_id IS NULL THEN lower(coalesce(hr_orgn_id, '')) != 'unknown' ELSE (trim(hr_orgn_id)!='' AND lower(trim(hr_orgn_id)) != 'unknown') END)
    AND (CASE WHEN work_state_cd IS NULL THEN lower(coalesce(work_state_cd, '')) != 'unknown'  ELSE (trim(work_state_cd)!='' AND lower(trim(work_state_cd)) != 'unknown') END)
    AND (CASE WHEN work_loc_cd IS NULL THEN lower(coalesce(work_loc_cd, '')) != 'unknown' ELSE (trim(work_loc_cd)!='' AND lower(trim(work_loc_cd)) != 'unknown') END)
    AND (CASE WHEN trmnt_rsn IS NULL THEN lower(coalesce(trmnt_rsn, '')) != 'unknown' ELSE (trim(trmnt_rsn)!='' AND lower(trim(trmnt_rsn)) != 'unknown') END)
    -- Filter insights that have a non null termination reason where mtrc_ky is not 63 (Num Terminations)
    AND (CASE WHEN rosie.metric_ky <> 63 THEN trmnt_rsn ELSE NULL END) IS NULL
    -- Filter min percentile ranks for metric 73
    AND (CASE WHEN (rosie.metric_ky = 63 AND trmnt_rsn IS NOT NULL) THEN insight_reason ELSE 'DUMMY' END) <> "MIN_PERCENTILE_RANKING"
    -- Filter insights at country level. i.e, work_cntry_cd Is Not Null and work_state_cd Is Null
    AND (CASE WHEN work_cntry_cd IS NOT NULL THEN work_state_cd ELSE 'DUMMY' END) IS NOT NULL
    -- Filter insights where all employees under him present in single location/job/department
    AND (CASE WHEN((insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN min_metric_value ELSE "DUMMY" END) <> (CASE WHEN((insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN max_metric_value ELSE "DUMMY1" END)
)prac_int_ins
LEFT OUTER JOIN (select distinct curr_yr,curr_qtr,curr_mnth,curr_qtr_num,day_cd,environment from ${__BLUE_MAIN_DB__}.t_dim_today) today
    ON prac_int_ins.environment=today.environment
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.t_fiscal_info clnt_fisc_info
 ON  prac_int_ins.clnt_obj_id = clnt_fisc_info.clnt_obj_id
 AND  prac_int_ins.db_schema = clnt_fisc_info.db_schema
 AND  prac_int_ins.environment = clnt_fisc_info.environment
 AND  prac_int_ins.dmn_ky = clnt_fisc_info.dmn_ky
 WHERE 
 (CASE WHEN (prac_int_ins.clnt_obj_id_r IS NOT NULL)
  THEN (CASE WHEN prac_int_ins.metric_ky IN(57,74,63,73) THEN abs(diff)/ins_empl_cnt>=0.1
    WHEN prac_int_ins.metric_ky IN(202) THEN abs(diff)/ins_empl_cnt>=1
    WHEN prac_int_ins.metric_ky IN(76,69,59,201,78,65) THEN ((abs(diff) > (ins_empl_cnt * 0.01)) AND (round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) < 0.9 OR round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) > 1.1))
    WHEN prac_int_ins.metric_ky=79 THEN abs(diff) >= 1
    ELSE 1=1 END)
  ELSE 1=1 END)
 AND ((clnt_obj_id_r IS NOT NULL AND NOT (prac_int_ins.yr_cd <=> yr_cd_r AND prac_int_ins.qtr_cd <=> qtr_cd_r AND prac_int_ins.mnth_cd <=> mnth_cd_r) AND insight_score <>  0)
    OR clnt_obj_id_r IS NULL)
 AND ((prac_int_ins.qtr_seq_nbr IS NULL AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr_num ELSE clnt_fisc_info.qtr_in_yr_nbr END) < 2) AND (CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN
            CASE WHEN (yr_cd_r is not null AND yr_cd != yr_cd_r)  THEN (today.curr_yr - yr_cd_r = 2) ELSE 1=1 END
            ELSE (CASE WHEN (yr_cd_r is not null AND yr_cd != yr_cd_r) THEN (curr_clnt_fisc_yr - yr_cd_r = 2 ) ELSE 1=1 END)END) AND (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - prac_int_ins.yr_cd = 1) AND (prac_int_ins.yr_cd_r IS NULL OR (prac_int_ins.yr_cd - prac_int_ins.yr_cd_r = 1)))) OR
            ((prac_int_ins.mnth_seq_nbr IS NULL) AND (CASE WHEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - (CASE WHEN prac_int_ins.clnt_obj_id_r IS NULL THEN prac_int_ins.yr_cd ELSE prac_int_ins.yr_cd_r END) = 0) THEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr ELSE curr_clnt_fisc_qtr  END) - prac_int_ins.qtr_seq_nbr) = 1
            WHEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - (CASE WHEN prac_int_ins.clnt_obj_id_r IS NULL THEN prac_int_ins.yr_cd ELSE prac_int_ins.yr_cd_r END) = 1) THEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr ELSE curr_clnt_fisc_qtr  END) - prac_int_ins.qtr_seq_nbr) = 1
            WHEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - prac_int_ins.yr_cd_r = 1) AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - prac_int_ins.yr_cd = 0)) THEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr ELSE curr_clnt_fisc_qtr  END) - 1) - prac_int_ins.qtr_seq_nbr_r) = 4
            WHEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - prac_int_ins.yr_cd_r = 2) AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - prac_int_ins.yr_cd = 1)) THEN ((((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_qtr ELSE curr_clnt_fisc_qtr  END) - 1) - prac_int_ins.qtr_seq_nbr_r) = 4 and (prac_int_ins.qtr_seq_nbr - prac_int_ins.qtr_seq_nbr_r) != 1)
                                                       ELSE 1=0 END)) OR
            (CASE WHEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - (CASE WHEN prac_int_ins.clnt_obj_id_r IS NULL THEN prac_int_ins.yr_cd ELSE prac_int_ins.yr_cd_r END) = 0) THEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_mnth ELSE curr_clnt_fisc_mnth  END) - prac_int_ins.mnth_seq_nbr) = 1
                  WHEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr  END) - (CASE WHEN prac_int_ins.clnt_obj_id_r IS NULL THEN prac_int_ins.yr_cd ELSE prac_int_ins.yr_cd_r END) = 1) THEN ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_mnth ELSE curr_clnt_fisc_mnth  END) - prac_int_ins.mnth_seq_nbr) = 1
                  WHEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - prac_int_ins.yr_cd_r = 1) AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - prac_int_ins.yr_cd = 0)) THEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_mnth ELSE curr_clnt_fisc_mnth  END) - 1) - prac_int_ins.mnth_seq_nbr_r)= 12
                  WHEN (((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - prac_int_ins.yr_cd_r = 2) AND ((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_yr ELSE curr_clnt_fisc_yr END) - prac_int_ins.yr_cd = 1)) THEN ((((CASE WHEN clnt_fisc_info.clnt_obj_id IS NULL THEN today.curr_mnth ELSE curr_clnt_fisc_mnth  END) - 1) - prac_int_ins.mnth_seq_nbr_r)= 12 and (prac_int_ins.mnth_seq_nbr - prac_int_ins.mnth_seq_nbr_r) != 1)
                  ELSE 1 = 0 END)
    )

UNION ALL

--Time Manager insights
SELECT
    /*+ BROADCAST(today,dim_clnt) */
    md5(concat(
    COALESCE(split(tm_mngr_ins.db_schema,'[|]')[0],'-'),
    COALESCE(tm_mngr_ins.clnt_obj_id,'-'),
    COALESCE(clnt_obj_id_r,'-'),
    COALESCE(yr_cd,'-'),
    COALESCE(yr_cd_r,'-'),
    COALESCE(qtr_cd,'-'),
    COALESCE(qtr_cd_r,'-'),
    COALESCE(mnth_cd,'-'),
    COALESCE(mnth_cd_r,'-'),
    COALESCE(job_cd,'-'),
    COALESCE(job_cd_r,'-'),
    COALESCE(hr_orgn_id,'-'),
    COALESCE(hr_orgn_id_r,'-'),
    COALESCE(excp_type,'-'),
    COALESCE(excp_type_r,'-'),
    COALESCE(work_cntry_cd,'-'),
    COALESCE(work_cntry_cd_r,'-'),
    COALESCE(work_state_cd,'-'),
    COALESCE(work_state_cd_r,'-'),
    COALESCE(work_loc_cd,'-'),
    COALESCE(work_loc_cd_r,'-'),
    COALESCE(mngr_pers_obj_id,'-'),
    COALESCE(mngr_pers_obj_id_r,'-'),
    COALESCE(supvr_pers_obj_id,'-'),
    COALESCE(supvr_pers_obj_id_r,'-'),
    COALESCE(metric_ky,'-'),
    COALESCE(insight_type,'-'),
    COALESCE(CASE WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = min_metric_value THEN 'MIN_PERCENTILE_RANKING'
                  WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = max_metric_value THEN 'MAX_PERCENTILE_RANKING'
                  ELSE insight_reason END,'-')
    )) as insight_hash,
    tm_mngr_ins.clnt_obj_id,
    clnt_obj_id_r,
    yr_cd,
    yr_cd_r,
    qtr_cd,
    qtr_cd_r,
    mnth_cd,
    mnth_cd_r,
    NULL AS wk_cd,
    NULL AS wk_cd_r,
    NULL AS flsa_stus_cd,
    NULL AS flsa_stus_cd_r,
    NULL AS full_tm_part_tm_cd,
    NULL AS full_tm_part_tm_cd_r,
    NULL AS gndr_cd,
    NULL AS gndr_cd_r,
    hr_orgn_id,
    hr_orgn_id_r,
    job_cd,
    job_cd_r,
    NULL AS pay_rt_type_cd,
    NULL AS pay_rt_type_cd_r,
    NULL AS reg_temp_cd,
    NULL AS reg_temp_cd_r,
    work_loc_cd,
    work_loc_cd_r,
    NULL AS work_city_cd,
    NULL AS work_city_cd_r,
    work_state_cd,
    work_state_cd_r,
    work_cntry_cd,
    work_cntry_cd_r,
    NULL AS trmnt_rsn,
    NULL AS trmnt_rsn_r,
    NULL AS adp_lens_cd,
    NULL AS adp_lens_cd_r,
    NULL AS inds_ky,
    NULL AS inds_ky_r,
    NULL AS sector_cd,
    NULL AS sector_cd_r,
    NULL AS supersector_cd,
    NULL AS supersector_cd_r,
    mngr_pers_obj_id,
    mngr_pers_obj_id_r,
    CASE WHEN(dim_clnt.clnt_obj_id IS NOT NULL AND metric_ky = 2 AND time_product = 'ezlm') THEN 112
       WHEN(dim_clnt.clnt_obj_id IS NOT NULL AND metric_ky = 5 AND time_product = 'ezlm') THEN 105
       WHEN(dim_clnt.clnt_obj_id IS NOT NULL AND metric_ky = 21 AND time_product = 'ezlm') THEN 121
       ELSE metric_ky END as metric_ky,
    (insight_score * metric_wgt *
    (CASE WHEN job_cd IS NOT NULL THEN job_cd_wgt ELSE 1 END)*
    (CASE WHEN hr_orgn_id IS NOT NULL THEN hr_orgn_id_wgt ELSE 1 END)*
    (CASE WHEN excp_type IS NOT NULL THEN excp_type_wgt ELSE 1 END)*
    (CASE WHEN qtr_seq_nbr IS NOT NULL THEN qtr_seq_nbr_wgt ELSE 1 END)*
    (CASE WHEN mnth_seq_nbr IS NOT NULL THEN mnth_seq_nbr_wgt ELSE 1 END)*
    (CASE WHEN work_state_cd IS NOT NULL THEN work_state_cd_wgt ELSE 1 END)*
    (CASE WHEN work_loc_cd IS NOT NULL THEN work_loc_cd_wgt ELSE 1 END)
    ) AS insight_score,
    percentile_rank,
    CASE WHEN (metric_ky<>21 AND insight_reason not in('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') AND  job_cd IS NULL AND hr_orgn_id IS NULL AND work_cntry_cd IS NULL) THEN 'CLIENT_INTERNAL_MYTEAM' ELSE insight_type END as insight_type,
    insight_events,
    CASE WHEN insight_reason IN ('NORM_PCTG_DIFF','PCTG_DIFF') THEN
           CASE WHEN (tm_mngr_ins.yr_cd <=> tm_mngr_ins.yr_cd_r AND tm_mngr_ins.qtr_cd <=> tm_mngr_ins.qtr_cd_r AND tm_mngr_ins.mnth_cd <=> tm_mngr_ins.mnth_cd_r) THEN 'PCTG_DIFF'
           ELSE 'PCTG_DIFF_TIME'  END                   
        WHEN insight_reason  IN ('NORM_ABS_DIFF','ABS_DIFF') THEN
           CASE WHEN (tm_mngr_ins.yr_cd <=> tm_mngr_ins.yr_cd_r AND tm_mngr_ins.qtr_cd <=> tm_mngr_ins.qtr_cd_r AND tm_mngr_ins.mnth_cd <=> tm_mngr_ins.mnth_cd_r) THEN 'ABS_DIFF'
           ELSE 'ABS_DIFF_TIME' END
        WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = min_metric_value THEN 'MIN_PERCENTILE_RANKING'
        WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = max_metric_value THEN 'MAX_PERCENTILE_RANKING' 
        ELSE insight_reason
    END AS insight_reason,
    ins_empl_cnt,
    ins_empl_cnt_r,
    tot_rpt_headcount AS empl_count,
    tot_rpt_headcount_r AS empl_count_r,
    pctg_empl_count,
    pctg_empl_count_r,
    num_dimensions,
    NULL AS retn_period_strt_person_cnt,
    NULL AS retn_period_end_person_cnt,
    NULL AS retn_period_strt_person_cnt_r,
    NULL AS retn_period_end_person_cnt_r,
    metric_value,
    metric_value_r,
    diff,
    percentage_diff,
    normalised_diff,
    normalised_percentage_diff,
    zscore_diff,
    zscore_percentage_diff,
    normalised_zscore_diff,
    normalised_zscore_percentage_diff,
    min_max_zscore,
    min_max_zscore_norm,
    min_metric_value,
    max_metric_value,
    insights_json,
    dmn_ky,
    'tm_mngr_ins' AS export_type,
    excp_type,
    excp_type_r,
    supvr_pers_obj_id,
    supvr_pers_obj_id_r,
    NULL AS rpt_access,
    tm_mngr_ins.db_schema,
    tm_mngr_ins.environment
FROM
(SELECT
     /*+ BROADCAST(lqtr,rqtr,lmnth,rmnth,emi_meta_dw_metrics) */
     tm_mngr_ins.clnt_obj_id,
     clnt_obj_id_r,
     yr_cd,
     yr_cd_r,
     tm_mngr_ins.mnth_seq_nbr,
     tm_mngr_ins.mnth_seq_nbr_r,
     lmnth.mnth_cd AS mnth_cd,
     rmnth.mnth_cd AS mnth_cd_r,
     tm_mngr_ins.qtr_seq_nbr,
     tm_mngr_ins.qtr_seq_nbr_r,
     lqtr.qtr_cd AS qtr_cd,
     rqtr.qtr_cd AS qtr_cd_r,
     job_cd,
     job_cd_r,
     hr_orgn_id,
     hr_orgn_id_r,
     excp_type,
     excp_type_r,
     work_cntry_cd,
     work_cntry_cd_r,
     work_state_cd,
     work_state_cd_r,
     work_loc_cd,
     work_loc_cd_r,
     mngr_pers_obj_id,
     mngr_pers_obj_id_r,
     supvr_pers_obj_id,
     supvr_pers_obj_id_r,
     tm_mngr_ins.metric_ky,
     insight_reason,
     insight_score,
     insight_type,
     insight_events,
     percentile_rank,
     ins_empl_cnt,
     ins_empl_cnt_r,
     tot_rpt_headcount,
     tot_rpt_headcount_r,
     pctg_empl_count,
     pctg_empl_count_r,
     num_dimensions,
     metric_value,
     metric_value_r,
     diff,
     percentage_diff,
     normalised_diff,
     normalised_percentage_diff,
     zscore_diff,
     zscore_percentage_diff,
     normalised_zscore_diff,
     normalised_zscore_percentage_diff,
     min_max_zscore,
     min_max_zscore_norm,
     min_metric_value,
     max_metric_value,
     insights_json,
     dmn_ky,
     metric_wgt ,
     job_cd_wgt ,
     gndr_cd_wgt ,
     qtr_seq_nbr_wgt,
     mnth_seq_nbr_wgt,
     work_state_cd_wgt,
     work_loc_cd_wgt,
     hr_orgn_id_wgt,
     excp_type_wgt,
     tm_mngr_ins.db_schema,
     tm_mngr_ins.environment
FROM
(SELECT
    clnt_obj_id,
     clnt_obj_id_r,
     yr_cd,
     yr_cd_r,
     mnth_seq_nbr,
     mnth_seq_nbr_r,
     qtr_seq_nbr,
     qtr_seq_nbr_r, 
     job_cd,
     job_cd_r,
     hr_orgn_id,
     hr_orgn_id_r,
     excp_type,
     excp_type_r,
     work_cntry_cd,
     work_cntry_cd_r,
     work_state_cd,
     work_state_cd_r,
     work_loc_cd,
     work_loc_cd_r,
     mngr_pers_obj_id,
     mngr_pers_obj_id_r,
     supvr_pers_obj_id,
     supvr_pers_obj_id_r,
     pctg_empl_count,
     pctg_empl_count_r,
     ins_empl_cnt,
     ins_empl_cnt_r,
     tot_rpt_headcount,
     tot_rpt_headcount_r,
     metrics.metric_ky,
     metrics.insight_reason,
     metrics.insight_score,
     metrics.insight_type,
     metrics.insight_events,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS percentile_rank,
     num_dimensions,
     CAST(metrics.insight_metrics[0] AS DOUBLE) AS metric_value,
     CAST(metrics.insight_metrics[1] AS DOUBLE) AS metric_value_r,
     CAST(metrics.insight_metrics[4] AS DOUBLE) AS diff,
     CAST(metrics.insight_metrics[5] AS DOUBLE) AS percentage_diff,
     CAST(metrics.insight_metrics[6] AS DOUBLE) AS normalised_diff,
     CAST(metrics.insight_metrics[7] AS DOUBLE) AS normalised_percentage_diff,
     CAST(metrics.insight_metrics[8] AS DOUBLE) AS zscore_diff,
     CAST(metrics.insight_metrics[9] AS DOUBLE) AS zscore_percentage_diff,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS normalised_zscore_diff,
     CAST(metrics.insight_metrics[11] AS DOUBLE) AS normalised_zscore_percentage_diff,
     CAST(metrics.insight_metrics[13] AS DOUBLE) AS min_metric_value,
     CAST(metrics.insight_metrics[14] AS DOUBLE) AS max_metric_value,
     CAST(metrics.insight_metrics[15] AS DOUBLE) AS min_max_zscore,
     CAST(metrics.insight_metrics[16] AS DOUBLE) AS min_max_zscore_norm,
     CONCAT(
        "{ \"this\": ", COALESCE(CAST(metrics.insight_metrics[0] AS STRING),'null'),
         ",\"that\": ", COALESCE(CAST(metrics.insight_metrics[1] AS STRING),'null'),
          ",\"this_events\": ", COALESCE(CAST(metrics.insight_metrics[2] AS STRING),'null'),
         ",\"that_events\": ", COALESCE(CAST(metrics.insight_metrics[3] AS STRING),'null'),
         ",\"insight\": {\"diff\" : ", COALESCE(CAST(metrics.insight_metrics[4] AS STRING),'null'),
         ",\"percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[5] AS STRING),'null'),
         ",\"normalised_diff\" : ", COALESCE(CAST(metrics.insight_metrics[6] AS STRING),'null'),
         ",\"normalised_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[7] AS STRING),'null'),
         ",\"ins_empl_cnt\" : ", COALESCE(CAST(ins_empl_cnt AS STRING),'null'),
         ",\"ins_empl_cnt_cmpr_with\" : ", COALESCE(CAST(ins_empl_cnt_r AS STRING),'null'),
         ",\"zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[8] AS STRING),'null'),
         ",\"zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[9] AS STRING),'null'),
         ",\"normalised_zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[10] AS STRING),'null'),
         ",\"normalised_zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[11] AS STRING),'null'),
         ",\"percentile_rank\" : ", COALESCE(CAST(metrics.insight_metrics[12] AS STRING),'null'),
         ",\"percentile_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[15] AS STRING),'null'),
         ",\"percentile_normalised_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[16] AS STRING),'null'),
         ",\"mean_diff\" : ", COALESCE(CAST(metrics.insight_metrics[17] AS STRING),'null'),
         ",\"stddev_diff\" : ", COALESCE(CAST(metrics.insight_metrics[18] AS STRING),'null'),
         ",\"mean_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[19] AS STRING),'null'),
         ",\"stddev_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[20] AS STRING),'null'),
         ",\"mean_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[21] AS STRING),'null'),
         ",\"stddev_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[22] AS STRING),'null'),
         ",\"mean_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[23] AS STRING),'null'),
         ",\"stddev_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[24] AS STRING),'null'),
         ",\"percentile_mean\" : ", COALESCE(CAST(metrics.insight_metrics[25] AS STRING),'null'),
         ",\"percentile_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[26] AS STRING),'null'),
         ",\"percentile_norm_mean\" : ", COALESCE(CAST(metrics.insight_metrics[27] AS STRING),'null'),
         ",\"percentile_norm_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[28] AS STRING),'null'), "}}"
     ) AS insights_json,
     5 AS dmn_ky,
     environment,
     db_schema
     FROM
       (SELECT
          mngr.clnt_obj_id,
          mngr.clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr  as int)     AS yr_cd,
          cast(yr_seq_nbr_r  as int)   AS yr_cd_r,      
          job_cd,
          job_cd_r,
          hr_orgn_id,
          hr_orgn_id_r,
          NULL AS excp_type,
          NULL AS excp_type_r,
          work_cntry_cd,
          work_cntry_cd_r,
          work_state_cd,
          work_state_cd_r,
          work_loc_cd,
          work_loc_cd_r,
          mngr_pers_obj_id,
          mngr_pers_obj_id_r,
          NULL AS supvr_pers_obj_id,
          NULL As supvr_pers_obj_id_r,
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r      AS pctg_empl_count_r,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount,
          tot_rpt_headcount_r,
          insight_reason_absence_ratio,
          insight_reason_overtime_ratio,
          NULL AS insight_reason_exception_count,
          NULL AS insight_reason_edits_count,      
          insight_score_absence_ratio,
          insight_score_overtime_ratio,
          NULL AS insight_score_exception_count,
          NULL AS insight_score_edits_count,    
          insight_type_absence_ratio,
          insight_type_overtime_ratio,
          NULL AS insight_type_exception_count,
          NULL AS insight_type_edits_count,    
          num_dimensions,
          absence_ratio_metrics,
          overtime_ratio_metrics,
          NULL AS exception_count_metrics,
          NULL AS edits_count_metrics,  
          absence_ratio_events,
          overtime_ratio_events,
          cast(NULL as double) AS exception_count_events,
          cast(NULL as double) AS edits_count_events,  
          environment,
          db_schema
        FROM ${__BLUE_MAIN_DB__}.emi_ins_tm_tf_manager mngr
        INNER JOIN(select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_tm_tf
                    )fct_sch
        ON mngr.clnt_obj_id = fct_sch.clnt_obj_id

        UNION ALL

        SELECT
          mngr.clnt_obj_id,
          mngr.clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr as int)     AS yr_cd,
          cast(yr_seq_nbr_r  as int)   AS yr_cd_r,      
          job_cd,
          job_cd_r,
          hr_orgn_id,
          hr_orgn_id_r,
          excp_type,
          excp_type_r,
          work_cntry_cd,
          work_cntry_cd_r,
          work_state_cd,
          work_state_cd_r,
          work_loc_cd,
          work_loc_cd_r,
          mngr_pers_obj_id,
          mngr_pers_obj_id_r,
          NULL AS supvr_pers_obj_id,
          NULL As supvr_pers_obj_id_r,
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r      AS pctg_empl_count_r,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount,
          tot_rpt_headcount_r,
          NULL AS insight_reason_absence_ratio,
          NULL AS insight_reason_overtime_ratio,
          insight_reason_exception_count,
          NULL AS insight_reason_edits_count,     
          NULL AS insight_score_absence_ratio,
          NULL AS insight_score_overtime_ratio,
          insight_score_exception_count,
          NULL AS insight_score_edits_count,   
          NULL AS insight_type_absence_ratio,
          NULL AS insight_type_overtime_ratio,
          insight_type_exception_count,
          NULL AS insight_type_edits_count,    
          num_dimensions,
          NULL AS absence_ratio_metrics,
          NULL AS overtime_ratio_metrics,
          exception_count_metrics,
          NULL AS edits_count_metrics,
          cast(NULL as double) AS absence_ratio_events,
          cast(NULL as double) AS overtime_ratio_events,
          exception_count_events,
          cast(NULL as double) AS edits_count_events,     
          environment,
          db_schema
        FROM ${__BLUE_MAIN_DB__}.emi_ins_tm_tf_excp_manager mngr
        INNER JOIN(select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_tm_tf_excp
                    )fct_sch
        ON mngr.clnt_obj_id = fct_sch.clnt_obj_id

        UNION ALL

        SELECT
          mngr.clnt_obj_id,
          mngr.clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr  as int)    AS yr_cd,
          cast(yr_seq_nbr_r  as int)  AS yr_cd_r,      
          job_cd,
          job_cd_r,
          hr_orgn_id,
          hr_orgn_id_r,
          NULL AS excp_type,
          NULL AS excp_type_r,
          work_cntry_cd,
          work_cntry_cd_r,
          work_state_cd,
          work_state_cd_r,
          work_loc_cd,
          work_loc_cd_r,
          mngr_pers_obj_id,
          mngr_pers_obj_id_r,
          NULL AS supvr_pers_obj_id,
          NULL As supvr_pers_obj_id_r,
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r      AS pctg_empl_count_r,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount,
          tot_rpt_headcount_r,
          NULL AS insight_reason_absence_ratio,
          NULL AS insight_reason_overtime_ratio,
          NULL AS insight_reason_exception_count,
          insight_reason_edits_count,     
          NULL AS insight_score_absence_ratio,
          NULL AS insight_score_overtime_ratio,
          NULL AS insight_score_exception_count,
          insight_score_edits_count,   
          NULL AS insight_type_absence_ratio,
          NULL AS insight_type_overtime_ratio,
          NULL AS insight_type_exception_count,
          insight_type_edits_count,    
          num_dimensions,
          NULL AS absence_ratio_metrics,
          NULL AS overtime_ratio_metrics,
          NULL AS exception_count_metrics,
          edits_count_metrics,
          cast(NULL as double) AS absence_ratio_evets,
          cast(NULL as double) AS overtime_ratio_events,
          cast(NULL as double) AS exception_count_events,
          edits_count_events,    
          environment,
          db_schema
        FROM ${__BLUE_MAIN_DB__}.emi_ins_tm_tf_edits_manager mngr
        INNER JOIN(select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_tm_tf_edits
                    )fct_sch
        ON mngr.clnt_obj_id = fct_sch.clnt_obj_id
    
        UNION ALL
    
        SELECT
          supvr.clnt_obj_id,
          supvr.clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr as int)     AS yr_cd,
          cast(yr_seq_nbr_r as int)   AS yr_cd_r,      
          job_cd,
          job_cd_r,
          hr_orgn_id,
          hr_orgn_id_r,
          NULL AS excp_type,
          NULL AS excp_type_r,
          work_cntry_cd,
          work_cntry_cd_r,
          work_state_cd,
          work_state_cd_r,
          work_loc_cd,
          work_loc_cd_r,
          NULL As mngr_pers_obj_id,
          NULL As mngr_pers_obj_id_r,
          supvr_pers_obj_id,
          supvr_pers_obj_id_r,
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r      AS pctg_empl_count_r,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount,
          tot_rpt_headcount_r,
          insight_reason_absence_ratio,
          insight_reason_overtime_ratio,
          NULL AS insight_reason_exception_count,
          NULL AS insight_reason_edits_count,    
          insight_score_absence_ratio,
          insight_score_overtime_ratio,
          NULL AS insight_score_exception_count,
          NULL AS insight_score_edits_count,   
          insight_type_absence_ratio,
          insight_type_overtime_ratio,
          NULL AS insight_type_exception_count,
          NULL AS insight_type_edits_count,      
          num_dimensions,
          absence_ratio_metrics,
          overtime_ratio_metrics,
          NULL AS exception_count_metrics,
          NULL AS edits_count_metrics,
          absence_ratio_events,
          overtime_ratio_events,
          cast(NULL as double) AS exception_count_events,
          cast(NULL as double) AS edits_count_events,     
          environment,
          db_schema
        FROM ${__BLUE_MAIN_DB__}.emi_ins_tm_tf_supervisor supvr
        INNER JOIN(select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_tm_tf
                    )fct_sch
        ON supvr.clnt_obj_id = fct_sch.clnt_obj_id

        UNION ALL
    
        SELECT
          supvr.clnt_obj_id,
          supvr.clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr  as int)    AS yr_cd,
          cast(yr_seq_nbr_r as int)   AS yr_cd_r,      
          job_cd,
          job_cd_r,
          hr_orgn_id,
          hr_orgn_id_r,
          excp_type,
          excp_type_r,
          work_cntry_cd,
          work_cntry_cd_r,
          work_state_cd,
          work_state_cd_r,
          work_loc_cd,
          work_loc_cd_r,
          NULL As mngr_pers_obj_id,
          NULL As mngr_pers_obj_id_r,
          supvr_pers_obj_id,
          supvr_pers_obj_id_r,
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r      AS pctg_empl_count_r,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount,
          tot_rpt_headcount_r,
          NULL AS insight_reason_absence_ratio,
          NULL AS insight_reason_overtime_ratio,
          insight_reason_exception_count,
          NULL AS insight_reason_edits_count,    
          NULL AS insight_score_absence_ratio,
          NULL AS insight_score_overtime_ratio,
          insight_score_exception_count,
          NULL AS insight_score_edits_count,    
          NULL AS insight_type_absence_ratio,
          NULL AS insight_type_overtime_ratio,
          insight_type_exception_count,
          NULL AS insight_type_edits_count,      
          num_dimensions,
          NULL AS absence_ratio_metrics,
          NULL AS overtime_ratio_metrics,
          exception_count_metrics,
          NULL AS edits_count_metrics,
          cast(NULL as double) AS absence_ratio_events,
          cast(NULL as double) AS overtime_ratio_events,
          exception_count_events,
          cast(NULL as double) AS edits_count_events,      
          environment,
          db_schema
        FROM ${__BLUE_MAIN_DB__}.emi_ins_tm_tf_excp_supervisor supvr
        INNER JOIN(select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_tm_tf_excp
                    )fct_sch
        ON supvr.clnt_obj_id = fct_sch.clnt_obj_id

        UNION ALL
    
        SELECT
          supvr.clnt_obj_id,
          supvr.clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr as int)    AS yr_cd,
          cast(yr_seq_nbr_r as int) AS yr_cd_r,       
          job_cd,
          job_cd_r,
          hr_orgn_id,
          hr_orgn_id_r,
          NULL AS excp_type,
          NULL AS excp_type_r,
          work_cntry_cd,
          work_cntry_cd_r,
          work_state_cd,
          work_state_cd_r,
          work_loc_cd,
          work_loc_cd_r,
          NULL As mngr_pers_obj_id,
          NULL As mngr_pers_obj_id_r,
          supvr_pers_obj_id,
          supvr_pers_obj_id_r,
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r      AS pctg_empl_count_r,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount,
          tot_rpt_headcount_r,
          NULL AS insight_reason_absence_ratio,
          NULL AS insight_reason_overtime_ratio,
          NULL AS insight_reason_exception_count,
          insight_reason_edits_count,    
          NULL AS insight_score_absence_ratio,
          NULL AS insight_score_overtime_ratio,
          NULL AS insight_score_exception_count,
          insight_score_edits_count,   
          NULL AS insight_type_absence_ratio,
          NULL AS insight_type_overtime_ratio,
          NULL AS insight_type_exception_count,
          insight_type_edits_count,      
          num_dimensions,
          NULL AS absence_ratio_metrics,
          NULL AS overtime_ratio_metrics,
          NULL AS exception_count_metrics,
          edits_count_metrics,   
          cast(NULL as double) AS absence_ratio_events,
          cast(NULL as double) AS overtime_ratio_events,
          cast(NULL as double) AS exception_count_events,
          edits_count_events,  
          environment,
          db_schema
        FROM ${__BLUE_MAIN_DB__}.emi_ins_tm_tf_edits_supervisor supvr
        INNER JOIN(select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_tm_tf_edits
                    )fct_sch
        ON supvr.clnt_obj_id = fct_sch.clnt_obj_id
        ) tm_mgr
      LATERAL VIEW stack(4,
                          '2', insight_reason_absence_ratio,insight_score_absence_ratio,insight_type_absence_ratio,absence_ratio_metrics,absence_ratio_events,
                          '5', insight_reason_overtime_ratio,insight_score_overtime_ratio,insight_type_overtime_ratio,overtime_ratio_metrics,overtime_ratio_events,
                          '21', insight_reason_exception_count,insight_score_exception_count,insight_type_exception_count,exception_count_metrics,exception_count_events,
                          '822', insight_reason_edits_count,insight_score_edits_count,insight_type_edits_count,edits_count_metrics,edits_count_events
                        ) metrics AS metric_ky, insight_reason, insight_score, insight_type, insight_metrics, insight_events
) tm_mngr_ins
INNER JOIN ${__RO_GREEN_RAW_DB__}.emi_meta_dw_metrics meta
  ON tm_mngr_ins.metric_ky = meta.metric_ky
LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN qtr_cd='' THEN NULL ELSE qtr_cd END AS qtr_cd,
      cast(qtr_seq_nbr as int) as qtr_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) lqtr
ON tm_mngr_ins.qtr_seq_nbr <=> lqtr.qtr_seq_nbr
LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN qtr_cd='' THEN NULL ELSE qtr_cd END AS qtr_cd,
      cast(qtr_seq_nbr as int) as qtr_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) rqtr
ON tm_mngr_ins.qtr_seq_nbr_r <=> rqtr.qtr_seq_nbr
LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN mnth_cd='' THEN NULL ELSE mnth_cd END AS mnth_cd,
      cast(mnth_seq_nbr as int) as mnth_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) lmnth
ON tm_mngr_ins.mnth_seq_nbr <=> lmnth.mnth_seq_nbr
LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN mnth_cd='' THEN NULL ELSE mnth_cd END AS mnth_cd,
      cast(mnth_seq_nbr as int) as mnth_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) rmnth
ON tm_mngr_ins.mnth_seq_nbr_r <=> rmnth.mnth_seq_nbr
WHERE  
   (mngr_pers_obj_id IS NOT NULL AND supvr_pers_obj_id IS NULL)
    --apply event count filter
    AND ((CASE WHEN tm_mngr_ins.metric_ky IN(21,822) THEN insight_events >=8 ELSE insight_events >=3 END) OR insight_events IS NULL)
    --apply zscore filter
    AND (CASE WHEN insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') THEN (min_max_zscore > 2 OR min_max_zscore < -2 OR min_max_zscore_norm > 2 OR min_max_zscore_norm < -2)
         ELSE (zscore_diff > 1 OR zscore_diff < -1 OR normalised_zscore_diff > 1 OR normalised_zscore_diff < -1 OR zscore_percentage_diff > 1 OR zscore_percentage_diff < -1 OR normalised_zscore_percentage_diff > 1 OR normalised_zscore_percentage_diff < -1 ) END)
    -- excp_type dimension is must for metric 21
    AND (CASE WHEN tm_mngr_ins.metric_ky=21 THEN excp_type  ELSE 'DUMMY' END) IS NOT NULL
    AND tm_mngr_ins.insight_reason <> 'NO_INSIGHT'
    AND metric_value IS NOT NULL
    AND metric_value <> 0
    AND meta.is_included = 1
    AND (diff <> 0 OR diff is NULL)
    -- Filter insights that have 'unknown' value in dimension columns
    AND (CASE WHEN job_cd IS NULL THEN lower(coalesce(job_cd, '')) != 'unknown' ELSE (trim(job_cd)!='' AND lower(trim(job_cd)) != 'unknown') END)
    AND (CASE WHEN hr_orgn_id IS NULL THEN lower(coalesce(hr_orgn_id, '')) != 'unknown' ELSE (trim(hr_orgn_id)!='' AND lower(trim(hr_orgn_id)) != 'unknown') END)
    AND (CASE WHEN work_state_cd IS NULL THEN lower(coalesce(work_state_cd, '')) != 'unknown'  ELSE (trim(work_state_cd)!='' AND lower(trim(work_state_cd)) != 'unknown') END)
    AND (CASE WHEN work_loc_cd IS NULL THEN lower(coalesce(work_loc_cd, '')) != 'unknown' ELSE (trim(work_loc_cd)!='' AND lower(trim(work_loc_cd)) != 'unknown') END)
    -- Filter insights at country level. i.e, work_cntry_cd Is Not Null and work_state_cd Is Null
    AND (CASE WHEN work_cntry_cd IS NOT NULL THEN work_state_cd ELSE 'DUMMY' END) IS NOT NULL
    -- Filter insights for manager where all employees under him present in single location/job/department
    AND (CASE WHEN((insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN min_metric_value ELSE "DUMMY" END) <> (CASE WHEN((insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN max_metric_value ELSE "DUMMY1" END)
) tm_mngr_ins
LEFT OUTER JOIN (select distinct curr_yr,curr_qtr,curr_mnth,curr_qtr_num,day_cd,environment from ${__BLUE_MAIN_DB__}.t_dim_today) today
    ON tm_mngr_ins.environment=today.environment
LEFT OUTER JOIN (select distinct environment,clnt_obj_id,db_schema,time_product from ${__RO_BLUE_RAW_DB__}.dwh_t_dim_clnt where time_product IS NOT NULL) dim_clnt
    ON tm_mngr_ins.environment = dim_clnt.environment
    AND tm_mngr_ins.db_schema = dim_clnt.db_schema
    AND tm_mngr_ins.clnt_obj_id = dim_clnt.clnt_obj_id
WHERE
(CASE WHEN (tm_mngr_ins.clnt_obj_id_r IS NOT NULL)
   THEN (
  CASE WHEN tm_mngr_ins.metric_ky IN(21,121,822) THEN abs(diff)/ins_empl_cnt>=1
  WHEN tm_mngr_ins.metric_ky IN(2,5,112,105) THEN ((abs(diff) > (ins_empl_cnt * 0.01)) AND (round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) < 0.9 OR round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) > 1.1))
  ELSE 1=1 END)
  ELSE 1=1 END)
--For DIFF based insights, time dimensions must be same on both sides
AND ((tm_mngr_ins.clnt_obj_id_r IS NOT NULL AND NOT (tm_mngr_ins.yr_cd <=> tm_mngr_ins.yr_cd_r AND tm_mngr_ins.qtr_cd <=> tm_mngr_ins.qtr_cd_r AND tm_mngr_ins.mnth_cd <=> tm_mngr_ins.mnth_cd_r) AND tm_mngr_ins.insight_score <>  0)
        OR tm_mngr_ins.clnt_obj_id_r IS NULL)
AND (
            (tm_mngr_ins.qtr_seq_nbr IS NULL AND today.curr_qtr_num < 2 AND (CASE WHEN (yr_cd_r is not null AND yr_cd != yr_cd_r) THEN (today.curr_yr - yr_cd_r = 2) ELSE 1=1 END) AND ((today.curr_yr - tm_mngr_ins.yr_cd = 1) AND (tm_mngr_ins.yr_cd_r IS NULL OR (tm_mngr_ins.yr_cd - tm_mngr_ins.yr_cd_r = 1)))) OR
            ((tm_mngr_ins.mnth_seq_nbr IS NULL) AND (CASE WHEN (today.curr_yr - (CASE WHEN tm_mngr_ins.clnt_obj_id_r IS NULL THEN tm_mngr_ins.yr_cd ELSE tm_mngr_ins.yr_cd_r END) = 0) THEN (today.curr_qtr - tm_mngr_ins.qtr_seq_nbr) = 1
            WHEN (today.curr_yr - (CASE WHEN tm_mngr_ins.clnt_obj_id_r IS NULL THEN tm_mngr_ins.yr_cd ELSE tm_mngr_ins.yr_cd_r END) = 1) THEN (today.curr_qtr - tm_mngr_ins.qtr_seq_nbr) = 1
            WHEN ((today.curr_yr - tm_mngr_ins.yr_cd_r = 1) AND (today.curr_yr - tm_mngr_ins.yr_cd = 0)) THEN ((today.curr_qtr - 1) - tm_mngr_ins.qtr_seq_nbr_r) = 4
            WHEN ((today.curr_yr - tm_mngr_ins.yr_cd_r = 2) AND (today.curr_yr - tm_mngr_ins.yr_cd = 1)) THEN ((today.curr_qtr - 1) - tm_mngr_ins.qtr_seq_nbr_r) = 4
                                                       ELSE 1=0 END)) OR
            (CASE WHEN (today.curr_yr - (CASE WHEN tm_mngr_ins.clnt_obj_id_r IS NULL THEN tm_mngr_ins.yr_cd ELSE tm_mngr_ins.yr_cd_r END) = 0) THEN (today.curr_mnth - tm_mngr_ins.mnth_seq_nbr) = 1
                  WHEN (today.curr_yr - (CASE WHEN tm_mngr_ins.clnt_obj_id_r IS NULL THEN tm_mngr_ins.yr_cd ELSE tm_mngr_ins.yr_cd_r END) = 1) THEN (today.curr_mnth - tm_mngr_ins.mnth_seq_nbr) = 1
                  WHEN ((today.curr_yr - tm_mngr_ins.yr_cd_r = 1) AND (today.curr_yr - tm_mngr_ins.yr_cd = 0)) THEN ((today.curr_mnth - 1) - tm_mngr_ins.mnth_seq_nbr_r)= 12
                  WHEN ((today.curr_yr - tm_mngr_ins.yr_cd_r = 2) AND (today.curr_yr - tm_mngr_ins.yr_cd = 1)) THEN ((today.curr_mnth - 1) - tm_mngr_ins.mnth_seq_nbr_r)= 12
                  ELSE 1 = 0 END)
    )

UNION ALL

--Time Practitioner insights
SELECT
    /*+ BROADCAST(today,dim_clnt) */
    md5(concat(
    COALESCE(split(tm_prac_ins.db_schema,'[|]')[0],'-'),
    COALESCE(tm_prac_ins.clnt_obj_id,'-'),
    COALESCE(clnt_obj_id_r,'-'),
    COALESCE(yr_cd,'-'),
    COALESCE(yr_cd_r,'-'),
    COALESCE(qtr_cd,'-'),
    COALESCE(qtr_cd_r,'-'),
    COALESCE(mnth_cd,'-'),
    COALESCE(mnth_cd_r,'-'),
    COALESCE(hr_orgn_id,'-'),
    COALESCE(hr_orgn_id_r,'-'),
    COALESCE(excp_type,'-'),
    COALESCE(excp_type_r,'-'),
    COALESCE(job_cd,'-'),
    COALESCE(job_cd_r,'-'),
    COALESCE(work_cntry_cd,'-'),
    COALESCE(work_cntry_cd_r,'-'),
    COALESCE(work_state_cd,'-'),
    COALESCE(work_state_cd_r,'-'),
    COALESCE(work_loc_cd,'-'),
    COALESCE(work_loc_cd_r,'-'),
    COALESCE(tm_prac_ins.metric_ky,'-'),
    COALESCE(insight_type,'-'),
    COALESCE(CASE WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = min_metric_value THEN 'MIN_PERCENTILE_RANKING'
                  WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = max_metric_value THEN 'MAX_PERCENTILE_RANKING'
                  ELSE insight_reason END,'-')
    )) as insight_hash,
    tm_prac_ins.clnt_obj_id,
    clnt_obj_id_r,
    yr_cd,
    yr_cd_r,
    qtr_cd,
    qtr_cd_r,
    mnth_cd,
    mnth_cd_r,
    NULL AS wk_cd,
    NULL AS wk_cd_r,
    NULL AS flsa_stus_cd,
    NULL AS flsa_stus_cd_r,
    NULL AS full_tm_part_tm_cd,
    NULL AS full_tm_part_tm_cd_r,
    NULL AS gndr_cd,
    NULL AS gndr_cd_r,
    hr_orgn_id,
    hr_orgn_id_r,
    job_cd,
    job_cd_r,
    NULL AS pay_rt_type_cd,
    NULL AS pay_rt_type_cd_r,
    NULL AS reg_temp_cd,
    NULL AS reg_temp_cd_r,
    work_loc_cd,
    work_loc_cd_r,
    NULL AS work_city_cd,
    NULL AS work_city_cd_r,
    work_state_cd,
    work_state_cd_r,
    work_cntry_cd,
    work_cntry_cd_r,
    NULL AS trmnt_rsn,
    NULL AS trmnt_rsn_r,
    NULL AS adp_lens_cd,
    NULL AS adp_lens_cd_r,
    NULL AS inds_ky,
    NULL AS inds_ky_r,
    NULL AS sector_cd,
    NULL AS sector_cd_r,
    NULL AS supersector_cd,
    NULL AS supersector_cd_r,
    NULL AS mngr_pers_obj_id,
    NULL AS mngr_pers_obj_id_r,
    CASE WHEN(dim_clnt.clnt_obj_id IS NOT NULL AND tm_prac_ins.metric_ky = 2 AND time_product = 'ezlm') THEN 112
       WHEN(dim_clnt.clnt_obj_id IS NOT NULL AND tm_prac_ins.metric_ky = 5 AND time_product = 'ezlm') THEN 105
       WHEN(dim_clnt.clnt_obj_id IS NOT NULL AND tm_prac_ins.metric_ky = 21 AND time_product = 'ezlm') THEN 121
       ELSE tm_prac_ins.metric_ky END as metric_ky,
    (insight_score * metric_wgt *
    (CASE WHEN job_cd IS NOT NULL THEN job_cd_wgt ELSE 1 END)*
    (CASE WHEN hr_orgn_id IS NOT NULL THEN hr_orgn_id_wgt ELSE 1 END)*
    (CASE WHEN excp_type IS NOT NULL THEN excp_type_wgt ELSE 1 END)*
    (CASE WHEN qtr_seq_nbr IS NOT NULL THEN qtr_seq_nbr_wgt ELSE 1 END)*
    (CASE WHEN mnth_seq_nbr IS NOT NULL THEN mnth_seq_nbr_wgt ELSE 1 END)*
    (CASE WHEN work_state_cd IS NOT NULL THEN work_state_cd_wgt ELSE 1 END) *
    (CASE WHEN work_loc_cd IS NOT NULL THEN work_loc_cd_wgt ELSE 1 END)
    ) AS insight_score,
    percentile_rank,
    CASE WHEN (tm_prac_ins.metric_ky<>21 AND insight_reason not in('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') AND  job_cd IS NULL AND hr_orgn_id IS NULL AND work_cntry_cd IS NULL) THEN 'CLIENT_INTERNAL_MYTEAM' ELSE insight_type END as insight_type,
    insight_events,
     CASE WHEN insight_reason IN ('NORM_PCTG_DIFF','PCTG_DIFF') THEN
           CASE WHEN (tm_prac_ins.yr_cd <=> tm_prac_ins.yr_cd_r AND tm_prac_ins.qtr_cd <=> tm_prac_ins.qtr_cd_r AND tm_prac_ins.mnth_cd <=> tm_prac_ins.mnth_cd_r) THEN 'PCTG_DIFF'
           ELSE 'PCTG_DIFF_TIME'  END                   
       WHEN insight_reason  IN ('NORM_ABS_DIFF','ABS_DIFF') THEN
           CASE WHEN (tm_prac_ins.yr_cd <=> tm_prac_ins.yr_cd_r AND tm_prac_ins.qtr_cd <=> tm_prac_ins.qtr_cd_r AND tm_prac_ins.mnth_cd <=> tm_prac_ins.mnth_cd_r) THEN 'ABS_DIFF'
           ELSE 'ABS_DIFF_TIME' END
       WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = min_metric_value THEN 'MIN_PERCENTILE_RANKING'
       WHEN insight_reason='PERCENTILE_RANKING' AND metric_value = max_metric_value THEN 'MAX_PERCENTILE_RANKING' 
       ELSE insight_reason
    END AS insight_reason,
    ins_empl_cnt,
    ins_empl_cnt_r,
    tot_rpt_headcount AS empl_count,
    tot_rpt_headcount_r AS empl_count_r,
    pctg_empl_count,
    pctg_empl_count_r,
    num_dimensions,
    NULL AS retn_period_strt_person_cnt,
    NULL AS retn_period_end_person_cnt,
    NULL AS retn_period_strt_person_cnt_r,
    NULL AS retn_period_end_person_cnt_r,
    metric_value,
    metric_value_r,
    diff,
    percentage_diff,
    normalised_diff,
    normalised_percentage_diff,
    zscore_diff,
    zscore_percentage_diff,
    normalised_zscore_diff,
    normalised_zscore_percentage_diff,
    min_max_zscore,
    min_max_zscore_norm,
    min_metric_value,
    max_metric_value,
    insights_json,
    tm_prac_ins.dmn_ky as dmn_ky,
    'tm_prac_ins' AS export_type,
    excp_type,
    excp_type_r,
    NULL AS supvr_pers_obj_id,
    NULL AS supvr_pers_obj_id_r,
    NULL AS rpt_access,
    tm_prac_ins.db_schema,
    tm_prac_ins.environment
FROM
(SELECT
    /*+ BROADCAST(lqtr,rqtr,lmnth,rmnth,emi_meta_dw_metrics) */
    time_prac.clnt_obj_id,
    clnt_obj_id_r,
    yr_cd,
    yr_cd_r,
    time_prac.mnth_seq_nbr,
    mnth_seq_nbr_r,
    lmnth.mnth_cd AS mnth_cd,
    rmnth.mnth_cd AS mnth_cd_r,
    time_prac.qtr_seq_nbr,
    qtr_seq_nbr_r,
    lqtr.qtr_cd AS qtr_cd,
    rqtr.qtr_cd AS qtr_cd_r,
    job_cd,
    job_cd_r,
    hr_orgn_id,
    hr_orgn_id_r,
    excp_type,
    excp_type_r,
    work_cntry_cd,
    work_cntry_cd_r,
    work_state_cd,
    work_state_cd_r,
    work_loc_cd,
    work_loc_cd_r,
    time_prac.metric_ky,
    insight_reason,
    insight_score,
    insight_type,
    insight_events,
    percentile_rank,
    ins_empl_cnt,
    ins_empl_cnt_r,
    tot_rpt_headcount,
    tot_rpt_headcount_r,
    pctg_empl_count,
    pctg_empl_count_r,
    num_dimensions,
    metric_value,
    metric_value_r,
    diff,
    percentage_diff,
    normalised_diff,
    normalised_percentage_diff,
    zscore_diff,
    zscore_percentage_diff,
    normalised_zscore_diff,
    normalised_zscore_percentage_diff,
    min_max_zscore,
    min_max_zscore_norm,
    min_metric_value,
    max_metric_value,
    insights_json,
    dmn_ky,
    metric_wgt ,
    job_cd_wgt ,
    gndr_cd_wgt ,
    qtr_seq_nbr_wgt,
    mnth_seq_nbr_wgt,
    work_state_cd_wgt,
    work_loc_cd_wgt,
    hr_orgn_id_wgt,
    excp_type_wgt,
    time_prac.db_schema,
    time_prac.environment
FROM
(SELECT
    clnt_obj_id,
     clnt_obj_id_r,
     yr_cd,
     yr_cd_r,
     mnth_seq_nbr,
     mnth_seq_nbr_r,
     qtr_seq_nbr,
     qtr_seq_nbr_r,
     hr_orgn_id,
     hr_orgn_id_r,
     excp_type,
     excp_type_r,
     job_cd,
     job_cd_r,
     work_cntry_cd,
     work_cntry_cd_r,
     work_state_cd,
     work_state_cd_r,
     work_loc_cd,
     work_loc_cd_r, 
     pctg_empl_count,
     pctg_empl_count_r,
     ins_empl_cnt,
     ins_empl_cnt_r,
     tot_rpt_headcount,
     tot_rpt_headcount_r,
     metrics.metric_ky,
     metrics.insight_reason,
     metrics.insight_score,
     metrics.insight_type,
     metrics.insight_events,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS percentile_rank,
     num_dimensions,
     CAST(metrics.insight_metrics[0] AS DOUBLE) AS metric_value,
     CAST(metrics.insight_metrics[1] AS DOUBLE) AS metric_value_r,
     CAST(metrics.insight_metrics[4] AS DOUBLE) AS diff,
     CAST(metrics.insight_metrics[5] AS DOUBLE) AS percentage_diff,
     CAST(metrics.insight_metrics[6] AS DOUBLE) AS normalised_diff,
     CAST(metrics.insight_metrics[7] AS DOUBLE) AS normalised_percentage_diff,
     CAST(metrics.insight_metrics[8] AS DOUBLE) AS zscore_diff,
     CAST(metrics.insight_metrics[9] AS DOUBLE) AS zscore_percentage_diff,
     CAST(metrics.insight_metrics[10] AS DOUBLE) AS normalised_zscore_diff,
     CAST(metrics.insight_metrics[11] AS DOUBLE) AS normalised_zscore_percentage_diff,
     CAST(metrics.insight_metrics[13] AS DOUBLE) AS min_metric_value,
     CAST(metrics.insight_metrics[14] AS DOUBLE) AS max_metric_value,
     CAST(metrics.insight_metrics[15] AS DOUBLE) AS min_max_zscore,
     CAST(metrics.insight_metrics[16] AS DOUBLE) AS min_max_zscore_norm,
     CONCAT(
        "{ \"this\": ", COALESCE(CAST(metrics.insight_metrics[0] AS STRING),'null'),
         ",\"that\": ", COALESCE(CAST(metrics.insight_metrics[1] AS STRING),'null'),
         ",\"this_events\": ", COALESCE(CAST(metrics.insight_metrics[2] AS STRING),'null'),
         ",\"that_events\": ", COALESCE(CAST(metrics.insight_metrics[3] AS STRING),'null'),
         ",\"insight\": {\"diff\" : ", COALESCE(CAST(metrics.insight_metrics[4] AS STRING),'null'),
         ",\"percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[5] AS STRING),'null'),
         ",\"normalised_diff\" : ", COALESCE(CAST(metrics.insight_metrics[6] AS STRING),'null'),
         ",\"normalised_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[7] AS STRING),'null'),
         ",\"ins_empl_cnt\" : ", COALESCE(CAST(ins_empl_cnt AS STRING),'null'),
         ",\"ins_empl_cnt_cmpr_with\" : ", COALESCE(CAST(ins_empl_cnt_r AS STRING),'null'),
         ",\"zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[8] AS STRING),'null'),
         ",\"zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[9] AS STRING),'null'),
         ",\"normalised_zscore_diff\" : ", COALESCE(CAST(metrics.insight_metrics[10] AS STRING),'null'),
         ",\"normalised_zscore_percentage_diff\" : ", COALESCE(CAST(metrics.insight_metrics[11] AS STRING),'null'),
         ",\"percentile_rank\" : ", COALESCE(CAST(metrics.insight_metrics[12] AS STRING),'null'),
         ",\"percentile_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[15] AS STRING),'null'),
         ",\"percentile_normalised_zscore\" : ", COALESCE(CAST(metrics.insight_metrics[16] AS STRING),'null'),
         ",\"mean_diff\" : ", COALESCE(CAST(metrics.insight_metrics[17] AS STRING),'null'),
         ",\"stddev_diff\" : ", COALESCE(CAST(metrics.insight_metrics[18] AS STRING),'null'),
         ",\"mean_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[19] AS STRING),'null'),
         ",\"stddev_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[20] AS STRING),'null'),
         ",\"mean_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[21] AS STRING),'null'),
         ",\"stddev_norm_diff\" : ", COALESCE(CAST(metrics.insight_metrics[22] AS STRING),'null'),
         ",\"mean_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[23] AS STRING),'null'),
         ",\"stddev_norm_pctg_diff\" : ", COALESCE(CAST(metrics.insight_metrics[24] AS STRING),'null'),
         ",\"percentile_mean\" : ", COALESCE(CAST(metrics.insight_metrics[25] AS STRING),'null'),
         ",\"percentile_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[26] AS STRING),'null'),
         ",\"percentile_norm_mean\" : ", COALESCE(CAST(metrics.insight_metrics[27] AS STRING),'null'),
         ",\"percentile_norm_stddev\" : ", COALESCE(CAST(metrics.insight_metrics[28] AS STRING),'null'), "}}"
     ) AS insights_json,
     5 AS dmn_ky,
     environment,
     db_schema
     FROM
        (SELECT
          prac.clnt_obj_id,
          prac.clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr as int) AS yr_cd,
          cast(yr_seq_nbr_r as int)   AS yr_cd_r,      
          hr_orgn_id,
          hr_orgn_id_r,
          NULL AS excp_type,
          NULL AS excp_type_r,
          job_cd,
          job_cd_r,      
          work_cntry_cd,
          work_cntry_cd_r,
          work_state_cd,
          work_state_cd_r,
          work_loc_cd,
          work_loc_cd_r,      
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r        AS pctg_empl_count_r,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount,
          tot_rpt_headcount_r,
          insight_reason_absence_ratio,
          insight_reason_overtime_ratio,
          NULL AS insight_reason_exception_count,
          NULL AS insight_reason_edits_count,     
          insight_score_absence_ratio,
          insight_score_overtime_ratio,
          NULL AS insight_score_exception_count,
          NULL AS insight_score_edits_count,     
          insight_type_absence_ratio,
          insight_type_overtime_ratio,
          NULL AS insight_type_exception_count,
          NULL AS insight_type_edits_count,      
          num_dimensions,
          absence_ratio_metrics,
          overtime_ratio_metrics,
         NULL AS exception_count_metrics,
          NULL AS edits_count_metrics,
          absence_ratio_events,
         overtime_ratio_events,
         cast(NULL as double) AS exception_count_events,
         cast(NULL as double) AS edits_count_events,
          environment,
          db_schema
      FROM ${__BLUE_MAIN_DB__}.emi_ins_tm_tf_practitioner prac
        INNER JOIN(select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_tm_tf
                    )fct_sch
        ON prac.clnt_obj_id = fct_sch.clnt_obj_id
    
        UNION ALL
    
        SELECT
          prac.clnt_obj_id,
          prac.clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr as int)      AS yr_cd,
          cast(yr_seq_nbr_r as int) AS yr_cd_r,      
          hr_orgn_id,
         hr_orgn_id_r,
          excp_type,
          excp_type_r,
          job_cd,
          job_cd_r,      
          work_cntry_cd,
          work_cntry_cd_r,
          work_state_cd,
          work_state_cd_r,
         work_loc_cd,
          work_loc_cd_r,      
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r        AS pctg_empl_count_r,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount,
          tot_rpt_headcount_r,
          NULL AS insight_reason_absence_ratio,
          NULL AS insight_reason_overtime_ratio,
          insight_reason_exception_count,
          NULL AS insight_reason_edits_count,     
          NULL AS insight_score_absence_ratio,
          NULL AS insight_score_overtime_ratio,
          insight_score_exception_count,
          NULL AS insight_score_edits_count,     
          NULL AS insight_type_absence_ratio,
          NULL AS insight_type_overtime_ratio,
          insight_type_exception_count,
          NULL AS insight_type_edits_count,      
          num_dimensions,
          NULL AS absence_ratio_metrics,
          NULL AS overtime_ratio_metrics,
          exception_count_metrics,
          NULL AS edits_count_metrics,
          cast(NULL as double) AS absence_ratio_events,
          cast(NULL as double) AS overtime_ratio_events,
          exception_count_events,
          cast(NULL as double) AS edits_count_events,
          environment,
          db_schema
        FROM ${__BLUE_MAIN_DB__}.emi_ins_tm_tf_excp_practitioner prac
        INNER JOIN(select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_tm_tf_excp
                    )fct_sch
        ON prac.clnt_obj_id = fct_sch.clnt_obj_id
    
        UNION ALL
    
        SELECT
          prac.clnt_obj_id,
          prac.clnt_obj_id_r,
          mnth_seq_nbr,
          mnth_seq_nbr_r,
          qtr_seq_nbr,
          qtr_seq_nbr_r,
          cast(yr_seq_nbr as int) AS yr_cd,
          cast(yr_seq_nbr_r as int)   AS yr_cd_r,      
          hr_orgn_id,
          hr_orgn_id_r,
          NULL AS excp_type,
          NULL AS excp_type_r,
          job_cd,
          job_cd_r,      
          work_cntry_cd,
          work_cntry_cd_r,
          work_state_cd,
          work_state_cd_r,
          work_loc_cd,
          work_loc_cd_r,      
          percentage_headcount        AS pctg_empl_count,
          percentage_headcount_r        AS pctg_empl_count_r,
          num_employees as ins_empl_cnt,
          num_employees_r as ins_empl_cnt_r,
          tot_rpt_headcount,
          tot_rpt_headcount_r,
          NULL AS insight_reason_absence_ratio,
          NULL AS insight_reason_overtime_ratio,
          NULL AS insight_reason_exception_count,
          insight_reason_edits_count,     
          NULL AS insight_score_absence_ratio,
          NULL AS insight_score_overtime_ratio,
          NULL AS insight_score_exception_count,
          insight_score_edits_count,      
          NULL AS insight_type_absence_ratio,
          NULL AS insight_type_overtime_ratio,
          NULL AS insight_type_exception_count,
          insight_type_edits_count,      
          num_dimensions,
          NULL AS absence_ratio_metrics,
          NULL AS overtime_ratio_metrics,
          NULL AS exception_count_metrics,
          edits_count_metrics,
          cast(NULL as double) AS absence_ratio_events,
          cast(NULL as double) AS overtime_ratio_events,
          cast(NULL as double) AS exception_count_events,
          edits_count_events,
          environment,
          db_schema
        FROM ${__BLUE_MAIN_DB__}.emi_ins_tm_tf_edits_practitioner prac
        INNER JOIN(select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_base_tm_tf_edits
                    )fct_sch
        ON prac.clnt_obj_id = fct_sch.clnt_obj_id ) tm_prac
      LATERAL VIEW stack(4,
                          '2', insight_reason_absence_ratio,insight_score_absence_ratio,insight_type_absence_ratio,absence_ratio_metrics,absence_ratio_events,
                          '5', insight_reason_overtime_ratio,insight_score_overtime_ratio,insight_type_overtime_ratio,overtime_ratio_metrics,overtime_ratio_events,
                           '21', insight_reason_exception_count,insight_score_exception_count,insight_type_exception_count,exception_count_metrics,exception_count_events,
                          '822', insight_reason_edits_count,insight_score_edits_count,insight_type_edits_count,edits_count_metrics,edits_count_events
                       ) metrics AS metric_ky, insight_reason, insight_score, insight_type, insight_metrics, insight_events
) time_prac
LEFT OUTER JOIN
   (SELECT DISTINCT CASE WHEN qtr_cd='' THEN NULL ELSE qtr_cd END AS qtr_cd,
      cast(qtr_seq_nbr as int)as qtr_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) lqtr
ON time_prac.qtr_seq_nbr <=> lqtr.qtr_seq_nbr
LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN qtr_cd='' THEN NULL ELSE qtr_cd END AS qtr_cd,
      cast(qtr_seq_nbr as int)as qtr_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) rqtr
ON time_prac.qtr_seq_nbr_r <=> rqtr.qtr_seq_nbr
LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN mnth_cd='' THEN NULL ELSE mnth_cd END AS mnth_cd,
      cast(mnth_seq_nbr as int) as mnth_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) lmnth
ON time_prac.mnth_seq_nbr <=> lmnth.mnth_seq_nbr
LEFT OUTER JOIN
    (SELECT DISTINCT CASE WHEN mnth_cd='' THEN NULL ELSE mnth_cd END AS mnth_cd,
      cast(mnth_seq_nbr as int) as mnth_seq_nbr
    FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day
    ) rmnth
ON time_prac.mnth_seq_nbr_r <=> rmnth.mnth_seq_nbr
INNER JOIN ${__RO_GREEN_RAW_DB__}.emi_meta_dw_metrics meta
ON time_prac.metric_ky = meta.metric_ky
WHERE ((CASE WHEN time_prac.metric_ky IN(21,822) THEN time_prac.insight_events >=8 ELSE time_prac.insight_events >=3 END) OR time_prac.insight_events IS NULL)
    --apply zscore filter
    AND (CASE WHEN time_prac.insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') THEN (min_max_zscore > 2 OR min_max_zscore < -2 OR min_max_zscore_norm > 2 OR min_max_zscore_norm < -2)
         ELSE (zscore_diff > 1 OR zscore_diff < -1 OR normalised_zscore_diff > 1 OR normalised_zscore_diff < -1 OR zscore_percentage_diff > 1 OR zscore_percentage_diff < -1 OR normalised_zscore_percentage_diff > 1 OR normalised_zscore_percentage_diff < -1 ) END)
    AND time_prac.insight_reason <> 'NO_INSIGHT'
    AND time_prac.metric_value IS NOT NULL
    AND time_prac.metric_value<>0
    AND meta.is_included = 1
    -- Filter insights that have 'unknown' value in dimension columns
    AND (CASE WHEN job_cd IS NULL THEN lower(coalesce(job_cd, '')) != 'unknown' ELSE (trim(job_cd)!='' AND lower(trim(job_cd)) != 'unknown') END)
    AND (CASE WHEN hr_orgn_id IS NULL THEN lower(coalesce(hr_orgn_id, '')) != 'unknown' ELSE (trim(hr_orgn_id)!='' AND lower(trim(hr_orgn_id)) != 'unknown') END)
    AND (CASE WHEN work_state_cd IS NULL THEN lower(coalesce(work_state_cd, '')) != 'unknown'  ELSE (trim(work_state_cd)!='' AND lower(trim(work_state_cd)) != 'unknown') END)
    AND (CASE WHEN work_loc_cd IS NULL THEN lower(coalesce(work_loc_cd, '')) != 'unknown' ELSE (trim(work_loc_cd)!='' AND lower(trim(work_loc_cd)) != 'unknown') END)
    AND (diff <> 0 OR diff is NULL)
    -- Filter insights at country level. i.e, work_cntry_cd Is Not Null and work_state_cd Is Null
    AND (CASE WHEN work_cntry_cd IS NOT NULL THEN work_state_cd ELSE 'DUMMY' END) IS NOT NULL
    -- excp_type dimension is must for metric 21
    AND (CASE WHEN time_prac.metric_ky=21 THEN excp_type  ELSE 'DUMMY' END) IS NOT NULL
    -- Filter insights where all employees under him present in single location/job/department
    AND (CASE WHEN((time_prac.insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN min_metric_value ELSE "DUMMY" END) <> (CASE WHEN((time_prac.insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN max_metric_value ELSE "DUMMY1" END)
)tm_prac_ins
LEFT OUTER JOIN (select distinct curr_yr,curr_qtr,curr_mnth,curr_qtr_num,day_cd,environment from ${__BLUE_MAIN_DB__}.t_dim_today) today
    ON tm_prac_ins.environment=today.environment
LEFT OUTER JOIN (select distinct environment,clnt_obj_id,db_schema,time_product from ${__RO_BLUE_RAW_DB__}.dwh_t_dim_clnt where time_product IS NOT NULL) dim_clnt
    ON tm_prac_ins.environment = dim_clnt.environment
    AND tm_prac_ins.db_schema = dim_clnt.db_schema
    AND tm_prac_ins.clnt_obj_id = dim_clnt.clnt_obj_id
WHERE 
(CASE WHEN (tm_prac_ins.clnt_obj_id_r IS NOT NULL)
   THEN (CASE WHEN tm_prac_ins.metric_ky IN(21,121,822) THEN abs(diff)/ins_empl_cnt>=1
   WHEN tm_prac_ins.metric_ky IN(2,5,112,105) THEN ((abs(diff) > (ins_empl_cnt * 0.01)) AND (round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) < 0.9 OR round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) > 1.1))
   ELSE 1=1 END)
  ELSE 1=1 END)
AND ((tm_prac_ins.clnt_obj_id_r IS NOT NULL AND NOT (tm_prac_ins.yr_cd <=> yr_cd_r AND tm_prac_ins.qtr_cd <=> qtr_cd_r AND tm_prac_ins.mnth_cd <=> mnth_cd_r) AND insight_score <>  0)
    OR tm_prac_ins.clnt_obj_id_r IS NULL)
AND (
            (tm_prac_ins.qtr_seq_nbr IS NULL  AND today.curr_qtr_num < 2 AND (CASE WHEN (yr_cd_r is not null AND yr_cd != yr_cd_r) THEN (today.curr_yr - yr_cd_r = 2) ELSE 1=1 END) AND ((today.curr_yr - tm_prac_ins.yr_cd = 1) AND (tm_prac_ins.yr_cd_r IS NULL OR (tm_prac_ins.yr_cd - tm_prac_ins.yr_cd_r = 1)))) OR
            ((tm_prac_ins.mnth_seq_nbr IS NULL) AND (CASE WHEN (today.curr_yr - (CASE WHEN tm_prac_ins.clnt_obj_id_r IS NULL THEN tm_prac_ins.yr_cd ELSE tm_prac_ins.yr_cd_r END) = 0) THEN (today.curr_qtr - tm_prac_ins.qtr_seq_nbr) = 1
            WHEN (today.curr_yr - (CASE WHEN tm_prac_ins.clnt_obj_id_r IS NULL THEN tm_prac_ins.yr_cd ELSE tm_prac_ins.yr_cd_r END) = 1) THEN (today.curr_qtr - tm_prac_ins.qtr_seq_nbr) = 1
            WHEN ((today.curr_yr - tm_prac_ins.yr_cd_r = 1) AND (today.curr_yr - tm_prac_ins.yr_cd = 0)) THEN ((today.curr_qtr - 1) - tm_prac_ins.qtr_seq_nbr_r) = 4
            WHEN ((today.curr_yr - tm_prac_ins.yr_cd_r = 2) AND (today.curr_yr - tm_prac_ins.yr_cd = 1)) THEN ((today.curr_qtr - 1) - tm_prac_ins.qtr_seq_nbr_r) = 4
                                                       ELSE 1=0 END)) OR
            (CASE WHEN (today.curr_yr - (CASE WHEN tm_prac_ins.clnt_obj_id_r IS NULL THEN tm_prac_ins.yr_cd ELSE tm_prac_ins.yr_cd_r END) = 0) THEN (today.curr_mnth - tm_prac_ins.mnth_seq_nbr) = 1
                  WHEN (today.curr_yr - (CASE WHEN tm_prac_ins.clnt_obj_id_r IS NULL THEN tm_prac_ins.yr_cd ELSE tm_prac_ins.yr_cd_r END) = 1) THEN (today.curr_mnth - tm_prac_ins.mnth_seq_nbr) = 1
                  WHEN ((today.curr_yr - tm_prac_ins.yr_cd_r = 1) AND (today.curr_yr - tm_prac_ins.yr_cd = 0)) THEN ((today.curr_mnth - 1) - tm_prac_ins.mnth_seq_nbr_r)= 12
                  WHEN ((today.curr_yr - tm_prac_ins.yr_cd_r = 2) AND (today.curr_yr - tm_prac_ins.yr_cd = 1)) THEN ((today.curr_mnth - 1) - tm_prac_ins.mnth_seq_nbr_r)= 12
                  ELSE 1 = 0 END)
    )
)final
--applying conditions again
WHERE 
(CASE WHEN (clnt_obj_id_r IS NOT NULL)
   THEN (CASE WHEN metric_ky IN(57,74,63,73) THEN abs(diff)/ins_empl_cnt>=0.1
  WHEN metric_ky IN(21,121,202,822) THEN abs(diff)/ins_empl_cnt>=1
  WHEN metric_ky IN(76,69,59,201,78,65,2,5,112,105) THEN ((abs(diff) > (ins_empl_cnt * 0.01)) AND (round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) < 0.9 OR round(metric_value/(CASE WHEN metric_value_r=0 THEN 1 ELSE metric_value_r END),2) > 1.1))
  WHEN metric_ky=79 THEN abs(diff) >= 1
  ELSE 1=1 END)
  ELSE 1=1 END)
AND ((CASE WHEN metric_ky IN(21,121,822) THEN insight_events >=8 ELSE insight_events >=3 END) OR insight_events IS NULL)
--apply zscore filter
AND (CASE WHEN insight_reason IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') THEN (min_max_zscore > 2 OR min_max_zscore < -2 OR min_max_zscore_norm > 2 OR min_max_zscore_norm < -2)
    ELSE (zscore_diff > 1 OR zscore_diff < -1 OR normalised_zscore_diff > 1 OR normalised_zscore_diff < -1 OR zscore_percentage_diff > 1 OR zscore_percentage_diff < -1 OR normalised_zscore_percentage_diff > 1 OR normalised_zscore_percentage_diff < -1 ) END)
-- excp_type dimension is must for metric 21
AND (CASE WHEN (metric_ky=21 OR metric_ky=121) THEN excp_type  ELSE 'DUMMY' END) IS NOT NULL
AND trim(insight_reason) <> 'NO_INSIGHT'
AND metric_value <> 0
AND metric_value IS NOT NULL
AND (diff <> 0 OR diff is NULL)
-- Filter insights that have 'unknown' value in dimension columns
AND (CASE WHEN job_cd IS NULL THEN lower(coalesce(job_cd, '')) != 'unknown' ELSE (trim(job_cd)!='' AND lower(trim(job_cd)) != 'unknown') END)
AND (CASE WHEN hr_orgn_id IS NULL THEN lower(coalesce(hr_orgn_id, '')) != 'unknown' ELSE (trim(hr_orgn_id)!='' AND lower(trim(hr_orgn_id)) != 'unknown') END)
AND (CASE WHEN work_state_cd IS NULL THEN lower(coalesce(work_state_cd, '')) != 'unknown'  ELSE (trim(work_state_cd)!='' AND lower(trim(work_state_cd)) != 'unknown') END)
AND (CASE WHEN work_loc_cd IS NULL THEN lower(coalesce(work_loc_cd, '')) != 'unknown' ELSE (trim(work_loc_cd)!='' AND lower(trim(work_loc_cd)) != 'unknown') END)
AND (CASE WHEN trmnt_rsn IS NULL THEN lower(coalesce(trmnt_rsn, '')) != 'unknown' ELSE (trim(trmnt_rsn)!='' AND lower(trim(trmnt_rsn)) != 'unknown') END)
-- Filter insights that have a non null termination reason where mtrc_ky is not 63 (Num Terminations)
AND (CASE WHEN metric_ky <> 73 THEN trmnt_rsn ELSE NULL END) IS NULL
-- Filter min percentile ranks for metric 73
AND (CASE WHEN metric_ky = 73 THEN insight_reason ELSE 'DUMMY' END) <> "MIN_PERCENTILE_RANKING"
-- Filter insights at country level. i.e, work_cntry_cd Is Not Null and work_state_cd Is Null
AND (CASE WHEN work_cntry_cd IS NOT NULL THEN work_state_cd ELSE 'DUMMY' END) IS NOT NULL
-- Filter insights for manager where all employees under him present in single location/job/department
AND (CASE WHEN((trim(insight_reason) IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN min_metric_value ELSE "DUMMY" END) <> (CASE WHEN((trim(insight_reason) IN ('PERCENTILE_RANKING','MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING')) AND (job_cd IS NOT NULL OR work_state_cd IS NOT NULL OR hr_orgn_id IS NOT NULL)) THEN max_metric_value ELSE "DUMMY1" END)
-- For DIFF based insights, time dimensions must be same on both sides
AND ((clnt_obj_id_r IS NOT NULL AND NOT (yr_cd <=> yr_cd_r AND qtr_cd <=> qtr_cd_r AND mnth_cd <=> mnth_cd_r) AND insight_score <>  0)
        OR clnt_obj_id_r IS NULL)
DISTRIBUTE BY final.db_schema;