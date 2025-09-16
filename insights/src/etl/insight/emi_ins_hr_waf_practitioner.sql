SELECT 
       -- dimensions
       rs_prac.clnt_obj_id,
       rs_prac.yr_cd,
       rs_prac.qtr_cd,
       rs_prac.mnth_cd,
       --rs_prac.d_flsa_stus_cd,
       --rs_prac.d_gndr_cd,
       rs_prac.d_job_cd,
       --rs_prac.d_reg_temp_cd,
       rs_prac.d_work_loc_cd,
       --rs_prac.d_work_city_cd,
       rs_prac.d_work_cntry_cd,
       rs_prac.d_hr_orgn_id,
       --rs_prac.d_pay_rt_type_cd,
       rs_prac.d_work_state_cd,
       --rs_prac.d_full_tm_part_tm_cd,
       rs_hr_wef.d_trmnt_rsn,
       mnth.mnth_seq_nbr,
       qtr.qtr_seq_nbr,
       yr.yr_seq_nbr,
       rs_prac.environment,
       --rs_prac.db_schema,

       -- facts
       --rs_prac.female_percentage,
       rs_prac.period_end_active_pers_cnt AS headcount,
       cast(rs_prac.period_end_active_pers_cnt as double) as headcount_events,
       maxhc.tot_rpt_headcount,
       ROUND((rs_prac.num_employees / maxhc.tot_rpt_headcount)*100,1) as percentage_headcount,
       --rs_prac.inactive_headcount,
       --rs_prac.parttime_headcount,
       --rs_prac.temp_headcount,
       rs_hr_wef.internal_mobility_rate,
       cast(rs_hr_wef.internal_mobility_count as double) as internal_mobility_rate_events,
       rs_hr_wef.num_hires,
       cast(rs_hr_wef.num_hires as double) as num_hires_events,
       rs_hr_wef.num_terminations,
       cast(rs_hr_wef.num_terminations as double) as num_terminations_events,
       100 * rs_hr_wef.num_terminations / rs_prac.headcount AS turnover_rate,
       cast(rs_hr_wef.num_terminations as double) as turnover_rate_events,
       100 * rs_hr_wef.num_voluntary_terminations / rs_prac.headcount AS voln_turnover_rate,
       cast(rs_hr_wef.num_voluntary_terminations as double) as voln_turnover_rate_events,
       100 * rs_hr_wef.num_newhire_turnovers /  rs_prac.headcount AS newhire_turnover_rate,
       cast(rs_hr_wef.num_newhire_turnovers as double) as newhire_turnover_rate_events,
       ROUND((100 * rs_prac.leave_person_days / rs_prac.period_end_active_pers_cnt),2) AS leave_percentage,
       cast(rs_prac.leave_person_count as double) as leave_percentage_events,
       --rs_prac.span_of_control,
       --rs_prac.compa_ratio,
       ROUND(rs_prac.average_tenure, 2) AS average_tenure,
       cast(NULL as double) AS average_tenure_events,
       (rs_hr_wef.avg_time_to_promotion / 12) AS avg_time_to_promotion,
       cast(rs_hr_wef.num_promotions as double) as avg_time_to_promotion_events,
       ROUND((100 * rs_retn.is_retained_in_end_cnt / rs_retn.is_active_in_strt_cnt),2) AS retention_rate,
       cast(NULL as double) AS retention_rate_events,
       --rs_prac.period_strt_person_cnt AS retn_period_strt_person_cnt,
       --rs_prac.period_end_person_cnt AS retn_period_end_person_cnt,

       --column to be used for scoring calculation
       rs_prac.num_employees,
       case when maxhc.tot_rpt_headcount < 5 then '<5' 
            when (maxhc.tot_rpt_headcount >= 5 and maxhc.tot_rpt_headcount < 10)  then '5_TO_10'
            when (maxhc.tot_rpt_headcount >= 10 and maxhc.tot_rpt_headcount < 20)  then '10_TO_20'
            when (maxhc.tot_rpt_headcount >= 20 and maxhc.tot_rpt_headcount < 30)  then '20_TO_30'
            when (maxhc.tot_rpt_headcount >= 30 and maxhc.tot_rpt_headcount < 50)  then '30_TO_50'
            when (maxhc.tot_rpt_headcount >= 50 and maxhc.tot_rpt_headcount < 100)  then '50_TO_100'
            when (maxhc.tot_rpt_headcount >= 100 and maxhc.tot_rpt_headcount < 200)  then '100_TO_200'
            when (maxhc.tot_rpt_headcount >= 200 and maxhc.tot_rpt_headcount < 500)  then '200_TO_500'
            when (maxhc.tot_rpt_headcount >= 500 and maxhc.tot_rpt_headcount < 1000)  then '500_TO_1000'
            else '>1000' end as zscore_type,

       -- Addon Column for Distribution Spec
       COALESCE(rs_prac.mnth_cd, rs_prac.qtr_cd, rs_prac.yr_cd) as period_cd

  FROM ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_hr_waf_practitioner rs_prac
 INNER JOIN (SELECT clnt_obj_id, yr_cd, qtr_cd, mnth_cd,environment, num_employees AS tot_rpt_headcount
               FROM ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_hr_waf_practitioner 
              WHERE d_job_cd IS NULL AND d_hr_orgn_id IS NULL AND d_work_cntry_cd IS NULL) maxhc
    ON rs_prac.clnt_obj_id = maxhc.clnt_obj_id 
   AND rs_prac.yr_cd <=> maxhc.yr_cd
   AND rs_prac.qtr_cd <=> maxhc.qtr_cd
   AND rs_prac.mnth_cd <=> maxhc.mnth_cd
   AND rs_prac.environment = maxhc.environment
   --AND rs_prac.db_schema = maxhc.db_schema
 LEFT OUTER JOIN ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_hr_wef_practitioner rs_hr_wef
    ON rs_prac.clnt_obj_id <=> rs_hr_wef.clnt_obj_id
   AND rs_prac.yr_cd <=> rs_hr_wef.yr_cd
   AND rs_prac.qtr_cd <=> rs_hr_wef.qtr_cd
   AND rs_prac.mnth_cd <=> rs_hr_wef.mnth_cd
   AND rs_prac.environment <=> rs_hr_wef.environment
   --AND rs_prac.db_schema <=> rs_hr_wef.db_schema
   --AND rs_prac.d_flsa_stus_cd <=> rs_hr_wef.d_flsa_stus_cd
   --AND rs_prac.d_gndr_cd <=> rs_hr_wef.d_gndr_cd
   AND rs_prac.d_job_cd <=> rs_hr_wef.d_job_cd
   --AND rs_prac.d_reg_temp_cd <=> rs_hr_wef.d_reg_temp_cd
   AND rs_prac.d_work_loc_cd <=> rs_hr_wef.d_work_loc_cd
   AND rs_prac.d_hr_orgn_id <=> rs_hr_wef.d_hr_orgn_id
   --AND rs_prac.d_pay_rt_type_cd <=> rs_hr_wef.d_pay_rt_type_cd
   AND rs_prac.d_work_state_cd <=> rs_hr_wef.d_work_state_cd
   --AND rs_prac.d_work_city_cd <=> rs_hr_wef.d_work_city_cd
   AND rs_prac.d_work_cntry_cd <=> rs_hr_wef.d_work_cntry_cd
   --AND rs_prac.d_full_tm_part_tm_cd <=> rs_hr_wef.d_full_tm_part_tm_cd
 LEFT OUTER JOIN ${hiveconf:__BLUE_MAIN_DB__}.emi_prep_retn_prac rs_retn
    ON rs_prac.clnt_obj_id <=> rs_retn.clnt_obj_id
   AND rs_prac.yr_cd <=> rs_retn.yr_cd
   AND rs_prac.qtr_cd <=> rs_retn.qtr_cd
   AND rs_prac.mnth_cd <=> rs_retn.mnth_cd
   AND rs_prac.environment <=> rs_retn.environment
   --AND rs_prac.db_schema <=> rs_retn.db_schema   
   AND rs_prac.d_job_cd <=> rs_retn.d_job_cd   
   AND rs_prac.d_work_loc_cd <=> rs_retn.d_work_loc_cd
   AND rs_prac.d_hr_orgn_id <=> rs_retn.d_hr_orgn_id   
   AND rs_prac.d_work_state_cd <=> rs_retn.d_work_state_cd   
   AND rs_prac.d_work_cntry_cd <=> rs_retn.d_work_cntry_cd   
  LEFT OUTER JOIN (SELECT DISTINCT environment, mnth_cd, qtr_cd, yr_cd, cast(qtr_seq_nbr as int) as qtr_seq_nbr, cast(mnth_seq_nbr as int) as mnth_seq_nbr, cast(yr_nbr as int) AS yr_seq_nbr 
                     FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) mnth
    ON rs_prac.environment = mnth.environment
    --AND rs_prac.db_schema = mnth.db_schema
    AND rs_prac.mnth_cd = mnth.mnth_cd
  LEFT OUTER JOIN (SELECT DISTINCT environment,qtr_cd, yr_cd, cast(qtr_seq_nbr as int) as qtr_seq_nbr, cast(yr_nbr as int) AS yr_seq_nbr 
                     FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) qtr
    ON rs_prac.environment = qtr.environment
    --AND rs_prac.db_schema = qtr.db_schema 
    AND rs_prac.qtr_cd = qtr.qtr_cd
  LEFT OUTER JOIN (SELECT DISTINCT environment,yr_cd, cast(yr_nbr as int) AS yr_seq_nbr 
                     FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) yr
    ON rs_prac.environment = yr.environment
    --AND rs_prac.db_schema = yr.db_schema 
    AND rs_prac.yr_cd = yr.yr_cd
  LEFT OUTER JOIN ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_fiscal_clnts fscl
    ON rs_prac.clnt_obj_id = fscl.clnt_obj_id
    AND rs_prac.environment = fscl.environment
    --AND rs_prac.db_schema = fscl.db_schema
    AND fscl.dmn_ky = 2
WHERE rs_prac.yr_cd IS NOT NULL
    -- Filter unnecessary cells from the cube to reduce processing (right now only for non-fiscal clients)
  AND (
    fscl.clnt_obj_id IS NOT NULL OR 
    (
        COALESCE(rs_prac.mnth_cd, date_format(add_months(current_date, -1), 'yyyyMM')) 
        IN (date_format(add_months(current_date, -1), 'yyyyMM'),
            date_format(add_months(current_date, -2), 'yyyyMM'),
            date_format(add_months(current_date, -13), 'yyyyMM')
        )
        -- This is previous quarter = cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)
        AND COALESCE(CASE WHEN rs_prac.mnth_cd IS NULL THEN rs_prac.qtr_cd ELSE NULL END, cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)) 
        IN (
            cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string),
            cast(year(add_months(current_date, -6)) *10 + ceil(month(add_months(current_date, -6))/3) as string),
            cast(year(add_months(current_date, -15)) *10 + ceil(month(add_months(current_date, -15))/3) as string)
        )
        AND rs_prac.yr_cd >= date_format(add_months(current_date, -24), 'yyyy')
    )
    
  )