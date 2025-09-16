-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_fact_insights_supvr;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_fact_insights_supvr (
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
    min_metric_value double,
    max_metric_value double,
    ins_json string,
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

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_fact_insights_supvr PARTITION(environment)
SELECT
  /*+ BROADCAST(t_dim_today), COALESCE(800) */
  insight_hash,
  tm_supvr_ins.clnt_obj_id,
  NULL AS clnt_obj_id_r,
  yr_cd,
  NULL AS yr_cd_r,
  cast(qtr_cd as int) as qtr_cd,
  NULL AS qtr_cd_r,
  cast(mnth_cd as int) as mnth_cd,
  NULL AS mnth_cd_r,
  NULL AS wk_cd,
  NULL AS wk_cd_r,
  NULL AS flsa_stus_cd,
  NULL AS flsa_stus_cd_r,
  NULL AS full_tm_part_tm_cd,
  NULL AS full_tm_part_tm_cd_r,
  NULL AS gndr_cd,
  NULL AS gndr_cd_r,
  NULL AS hr_orgn_id,
  NULL AS hr_orgn_id_r,
  NULL AS job_cd,
  NULL AS job_cd_r,
  NULL AS pay_rt_type_cd,
  NULL AS pay_rt_type_cd_r,
  NULL AS reg_temp_cd,
  NULL AS reg_temp_cd_r,
  NULL AS work_loc_cd,
  NULL AS work_loc_cd_r,
  NULL AS work_city_cd,
  NULL AS work_city_cd_r,
  NULL AS work_state_cd,
  NULL AS work_state_cd_r,
  NULL AS work_cntry_cd,
  NULL AS work_cntry_cd_r,
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
  metric_ky,
  NULL AS insight_score,
  NULL AS percentile_rank,
  'CLIENT_INTERNAL' as insight_type,
  'PEOPLE_INSIGHTS' AS insight_reason,
  empl_count,
  NULL AS empl_count_r,
  NULL AS pctg_empl_count,
  NULL AS pctg_empl_count_r,
  NULL AS num_dimensions,
  NULL AS retn_period_strt_person_cnt,
  NULL AS retn_period_end_person_cnt,
  NULL AS retn_period_strt_person_cnt_r,
  NULL AS retn_period_end_person_cnt_r,
  1 as metric_value,
  NULL AS metric_value_r,
  NULL AS diff,
  NULL AS percentage_diff,
  NULL AS normalised_diff,
  NULL AS normalised_percentage_diff,
  NULL AS zscore_diff,
  NULL AS zscore_percentage_diff,
  NULL AS normalised_zscore_diff,
  NULL AS normalised_zscore_percentage_diff,
  NULL AS min_metric_value,
  NULL AS max_metric_value,
  insights_json,
  'supvr_wfm_ins' AS export_type,
  excp_type,
  NULL AS excp_type_r,
  supvr_pers_obj_id,
  NULL AS supvr_pers_obj_id_r,
  rpt_access,
  --tm_supvr_ins.excp_pers_cnt,
  tm_supvr_ins.db_schema,
  tm_supvr_ins.environment
FROM
  (SELECT
     md5(concat(
       COALESCE(split(db_schema,'[|]')[0],'-'),
       COALESCE(clnt_obj_id,'-'),
       COALESCE(yr_cd,'-'),
       COALESCE(qtr_cd,'-'),
       COALESCE(mnth_cd,'-'),
       COALESCE(supvr_pers_obj_id,'-'),
       COALESCE(excp_type,'-'),
       COALESCE(rpt_access,'-'),
       '121',
       'CLIENT_INTERNAL',
       'PEOPLE_INSIGHTS'
     )) as insight_hash,
    clnt_obj_id,
    supvr_pers_obj_id,
    count(pers_obj_id) as empl_count,
    outlier_value,
    concat("{ \"pers_excp_count_map\": ","[",concat_ws(',',collect_list( case when seqnum <=2 then
        concat("{","\"",srce_sys_pers_id,"\":",cast(excep_count_pers as string),"}")  end
        )),"]",",\"outlier_val\": ",cast(round(outlier_value,2) as string),"}") as insights_json,
    excp_type,
    case when lower(excp_type) in ('missing in punch, missing out punch') then 850 else 121 end as metric_ky,
    yr_cd,
    qtr_cd,
    mnth_cd,
    mnth_seq_nbr,
    qtr_seq_nbr,
    yr_seq_nbr,
    5 AS dmn_ky,
    rpt_access,
    environment,
    db_schema
  from
  (select clnt_obj_id,
    supvr_pers_obj_id,
    pers_obj_id,
    srce_sys_pers_id,
    excp_type,
    --case when iqr_outlier > 0 then iqr_outlier else z_outlier end as outlier_value,
    case
    when thrsh_outlier > 0 then thrsh_outlier
    when avg_outlier > 0 then avg_outlier
    when iqr_outlier > 0 then iqr_outlier
    when z_iqr_outlier > 0 then z_iqr_outlier
    when z_outlier > 0 then z_outlier end as outlier_value,
    excep_count_pers,
    yr_cd,
    qtr_cd,
    mnth_cd,
    mnth_seq_nbr,
    qtr_seq_nbr,
    yr_seq_nbr,
    rpt_access,
    row_number() over (partition by clnt_obj_id, excp_type, supvr_pers_obj_id, rpt_access,
            yr_cd, qtr_cd, mnth_cd, environment, db_schema order by excep_count_pers desc, srce_sys_pers_id desc) as seqnum,
    environment,
    db_schema from
        (SELECT
        /*+ BROADCAST(mnth,qtr,yr,dwh_t_dim_pers) */
        rcm.clnt_obj_id,
        rcm.supvr_pers_obj_id,
        rcm.pers_cnt,
       CASE WHEN (
        (round(hc.supvr_headcount,2) >= 4.55 and pers_cnt >= 1 and pers_cnt<= 2  and excep_count_pers > 8)
        ) THEN 8
        ELSE 0 END AS thrsh_outlier,
        CASE WHEN (
        (round(hc.supvr_headcount,2) >= 4.55 and pers_cnt >= 3 and pers_cnt<= 4  and excep_count_pers > excp_cnt_avg)
        ) THEN excp_cnt_avg
        ELSE 0 END AS avg_outlier,
        CASE WHEN (
        (round(hc.supvr_headcount,2) >= 4.55 and pers_cnt >= 5 and pers_cnt <= 20 and iqr_diff > 0 and excep_count_pers >= (iqr_outlier_val))
        ) THEN iqr_outlier_val
        ELSE 0 END AS iqr_outlier,
        CASE WHEN (
        (round(hc.supvr_headcount,2) >= 4.55 and pers_cnt >= 5 and pers_cnt <= 20 and iqr_diff = 0 and norm_excp_cnt_stddev > 0 and (((norm_excep_count_pers - norm_excp_cnt_avg)/norm_excp_cnt_stddev) > 1.5 )
        )) THEN (exp(norm_excp_cnt_avg + 1.5*(norm_excp_cnt_stddev)))
        ELSE 0 END AS z_iqr_outlier,
        CASE WHEN (
        (round(hc.supvr_headcount,2) >= 4.55 and pers_cnt > 20 and norm_excp_cnt_stddev > 0 and (((norm_excep_count_pers - norm_excp_cnt_avg)/norm_excp_cnt_stddev) > 1.5 )
        )) THEN (exp(norm_excp_cnt_avg + 1.5*(norm_excp_cnt_stddev)))
        ELSE 0 END AS z_outlier,
        rpm.pers_obj_id,
        pers.srce_sys_pers_id,
        excep_count_pers,
        rcm.excp_type,
        rcm.mnth_cd,
        rcm.qtr_cd,
        rcm.yr_cd,
        mnth.mnth_seq_nbr,
        qtr.qtr_seq_nbr,
        yr.yr_seq_nbr,
      rpm.rpt_access,
        fact_sch.db_schema,
        rcm.environment
    FROM (
    select
        aggregation_depth,
        excp_cnt_avg,
        clnt_obj_id,
        excp_cnt_stddev,
        excp_type,
        iqr_outlier_val,
        iqr_25th_percentile,
        norm_excp_cnt_avg,
        iqr_75th_percentile,
        norm_excp_cnt_stddev,
        iqr_diff,
        pers_cnt,
        supvr_pers_obj_id,
        NULL as mnth_cd,
        qtr_cd,
        rpt_access,
        environment,
        yr_cd
     from ${__GREEN_MAIN_DB__}.emi_cube_tm_tf_excp_supervisor_qtr_wfm

     UNION ALL

     select
        aggregation_depth,
        excp_cnt_avg,
        clnt_obj_id,
        excp_cnt_stddev,
       excp_type,
        iqr_outlier_val,
        iqr_25th_percentile,
        norm_excp_cnt_avg,
        iqr_75th_percentile,
        norm_excp_cnt_stddev,
        iqr_diff,
        pers_cnt,
        supvr_pers_obj_id,
        mnth_cd,
        qtr_cd,
        rpt_access,
        environment,
        yr_cd
     from ${__GREEN_MAIN_DB__}.emi_cube_tm_tf_excp_supervisor_mnth_wfm
    ) rcm
   INNER JOIN (select distinct clnt_obj_id,db_schema from ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_wfm)fact_sch
    ON rcm.clnt_obj_id = fact_sch.clnt_obj_id
    inner JOIN ${__BLUE_MAIN_DB__}.emi_prep_tm_tf_excp_wfm rpm
    ON rcm.clnt_obj_id       = rpm.clnt_obj_id
   AND rcm.supvr_pers_obj_id = rpm.supvr_pers_obj_id
    AND rcm.excp_type        =rpm.excp_type
    AND rcm.yr_cd            = rpm.yr_cd
    AND rcm.qtr_cd           <=> rpm.qtr_cd
    AND rcm.mnth_cd <=> rpm.mnth_cd
    AND rcm.rpt_access = rpm.rpt_access
    --AND rcm.db_schema        = rpm.db_schema
    AND rcm.environment      =rpm.environment
    INNER JOIN ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pers pers on
    rpm.clnt_obj_id = pers.clnt_obj_id
    AND rpm.pers_obj_id = pers.pers_obj_id
    --AND rpm.db_schema = pers.db_schema
    AND rpm.environment = pers.environment
    LEFT OUTER JOIN (select distinct mnth_cd, cast(mnth_seq_nbr as int) as mnth_seq_nbr FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day) mnth
      ON rcm.mnth_cd = mnth.mnth_cd
      LEFT OUTER JOIN (select distinct qtr_cd, cast(qtr_seq_nbr as int) as qtr_seq_nbr FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day) qtr
      ON rcm.qtr_cd = qtr.qtr_cd
      LEFT OUTER JOIN (select distinct yr_cd, cast(yr_nbr as int) as yr_seq_nbr FROM ${__RO_BLUE_RAW_DB__}.dwh_t_dim_day) yr
     ON rcm.yr_cd = yr.yr_cd
    LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.emi_prep_hr_waf_supvr_headcnt hc
   --ON rcm.db_schema = hc.db_schema
    ON rcm.environment = hc.environment
    AND rcm.clnt_obj_id =hc.clnt_obj_id
    AND rcm.supvr_pers_obj_id = hc.supvr_pers_obj_id
    AND rcm.yr_cd = hc.yr_cd
    AND rcm.qtr_cd <=> hc.qtr_cd
    AND rcm.mnth_cd <=> hc.mnth_cd
    AND rcm.rpt_access <=> hc.rpt_access
    )supvr
    WHERE (thrsh_outlier > 0 or avg_outlier > 0 or z_outlier > 0 or iqr_outlier > 0 or z_iqr_outlier > 0)
)supvr_excp
  GROUP BY
  clnt_obj_id,
  supvr_pers_obj_id,
  excp_type,
  outlier_value,
  yr_cd,
  qtr_cd,
  mnth_cd,
  mnth_seq_nbr,
  qtr_seq_nbr,
  yr_seq_nbr,
  rpt_access,
  environment,
  db_schema
) tm_supvr_ins
LEFT OUTER JOIN ${__BLUE_MAIN_DB__}.t_dim_today today
    ON tm_supvr_ins.environment=today.environment
WHERE
    (
             ((tm_supvr_ins.mnth_seq_nbr IS NULL) AND (CASE WHEN ((today.curr_yr) - (tm_supvr_ins.yr_cd) = 0) THEN ((today.curr_qtr) - tm_supvr_ins.qtr_seq_nbr) = 1
            WHEN ((today.curr_yr) - (tm_supvr_ins.yr_cd) = 1) THEN ((today.curr_qtr) - tm_supvr_ins.qtr_seq_nbr) = 1
             ELSE 1=0 END)) OR
            (CASE WHEN ((today.curr_yr) - (tm_supvr_ins.yr_cd) = 0) THEN ((today.curr_mnth) - tm_supvr_ins.mnth_seq_nbr) = 1
            WHEN ((today.curr_yr) - (tm_supvr_ins.yr_cd) = 1) THEN ((today.curr_mnth) - tm_supvr_ins.mnth_seq_nbr) = 1
                 ELSE 1 = 0 END)
    )
    AND rpt_access IS NOT NULL;