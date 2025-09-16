SELECT DISTINCT
  t1.clnt_obj_id,
  t1.pers_obj_id,
  t1.supersector_cd AS supersector_cd,
  t1.sector_cd AS sector_cd,
  t1.subsector_cd AS subsector_cd,
  NULL AS combo_cd,
  SUBSTR(t1.l6_code,7,6) AS naics_cd,
  t2.onet_code,
  t2.adp_lens_cd,
  t1.work_state_cd,
  t1.qtr_cd,
  t1.experience,
  t1.peer_cmpn_ratio,
  t1.terminated_in_1_quarters,
  t1.terminated_in_2_quarters,
  t1.terminated_in_3_quarters,
  t1.terminated_in_4_quarters,
  t1.vol_trmnt_in_1_quarters,
  t1.vol_trmnt_in_2_quarters,
  t1.vol_trmnt_in_3_quarters,
  t1.vol_trmnt_in_4_quarters,
  t1.annual_comp_by_tenure,
  t2.overtime_dbltime_hours,
  LN(t2.overtime_dbltime_hours) as log_overtime_dbltime_hours,
  t2.overtime_dbltime_earnings,
  t2.travel_distance,
  t2.travel_duration,
  t2.is_manager,
  t2.qtrs_since_last_promotion,
  LN(t2.qtrs_since_last_promotion) as log_qtrs_since_last_promotion,
  t2.salary_hike,
  LN(t2.salary_hike) as log_salary_hike,
  t2.tenure_in_job_months,
  CASE WHEN t1.full_tm_part_tm_dsc = 'FULL_TIME' THEN 'F' ELSE 'P' END AS full_tm_part_tm_cd, -- Used as dimension 
  t1.full_tm_part_tm_dsc, -- Used as fact
  CASE WHEN t1.reg_temp_dsc = 'REGULAR' THEN 'R' ELSE 'T' END AS reg_temp_cd, -- Used as dimension 
  t1.reg_temp_dsc, -- Used as fact
  t2.flsa_stus_dsc,
  t2.compa_rt,
  t2.pay_rt_type_dsc,
  t2.bm_trnovr_rt,
  t2.hr_cmpn_freq_dsc,
  t2.layer,
  t2.total_reports,
  t2.annl_cmpn_amt,
  LN(t2.annl_cmpn_amt) as log_annl_cmpn_amt,
  t2.is_terminated,
  t2.is_voluntary_termination,
  t2.tenure_months,
  '${hiveconf:qtr}' AS qtr
FROM ${hiveconf:__BLUE_MAIN_DB__}.top_processed_dataset t1
    INNER JOIN (SELECT concat('QTR_',a.qtr_cd) AS qtr_cd FROM ${hiveconf:__GREEN_MAIN_DB__}.top_tmp_t_dim_qtr a 
                INNER JOIN 
                (SELECT qtr_cd, row_number() over(ORDER BY qtr_cd DESC) rk FROM ${hiveconf:__GREEN_MAIN_DB__}.top_tmp_t_dim_qtr) b 
                WHERE a.qtr_cd=b.qtr_cd
                AND rk BETWEEN 2 AND 6
                ) qtrs
    ON
    t1.qtr_cd = qtrs.qtr_cd
    INNER JOIN (SELECT DISTINCT a.OOID FROM ${hiveconf:__RO_BLUE_LANDING_BASE_DB__}.client_master a 
                INNER JOIN (SELECT max(yyyymm) AS yyyymm FROM ${hiveconf:__RO_BLUE_LANDING_BASE_DB__}.client_master) b
                ON a.yyyymm = b.yyyymm
                WHERE a.optout_flag = 'N'  ) t3
    ON 
    t3.ooid = t1.clnt_obj_id
    INNER JOIN ${hiveconf:__BLUE_MAIN_DB__}.all_top_data_extract t2
    ON t1.clnt_obj_id = t2.clnt_obj_id
    AND t1.source_system = t2.source_system 
    AND t1.l2_code  = t2.l2_code
    AND t1.pers_obj_id = t2.pers_obj_id
    AND t1.qtr_cd = t2.qtr_cd