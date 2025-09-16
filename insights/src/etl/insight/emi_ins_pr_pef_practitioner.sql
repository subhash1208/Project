SELECT 
       -- dimensions
       rs_pr_prac.clnt_obj_id,
       rs_pr_prac.yr_cd,
       rs_pr_prac.qtr_cd,
       rs_pr_prac.mnth_cd,
       --rs_pr_prac.d_flsa_stus_cd,
       --rs_pr_prac.d_gndr_cd,
       rs_pr_prac.d_job_cd,
       --rs_pr_prac.d_reg_temp_cd,
       rs_pr_prac.d_work_loc_cd,
       --rs_pr_prac.d_work_city_cd,
       rs_pr_prac.d_work_cntry_cd,
       rs_pr_prac.d_hr_orgn_id,
       --rs_pr_prac.d_pay_rt_type_cd,
       rs_pr_prac.d_work_state_cd,
       --rs_pr_prac.d_full_tm_part_tm_cd,
       mnth.mnth_seq_nbr,
       qtr.qtr_seq_nbr,
       yr.yr_seq_nbr,
       rs_pr_prac.environment,
       --rs_pr_prac.db_schema,

       -- facts
       tot_rpt_headcount AS headcount,
      
       ROUND((rs_pr_prac.num_employees / maxhc.tot_rpt_headcount)*100,1) as percentage_headcount,
       rs_pr_prac.overtime_earnings,
       cast(rs_pr_prac.overtime_earnings_evnt_src_cnt as double) AS overtime_earnings_events,
       --rs_pr_prac.total_earnings,
       --rs_pr_prac.overtime_earnings/rs_prac.headcount AS overtime_earnings_per_fte,
       round(rs_pr_prac.total_earnings/rs_pr_prac.count_distinct_person,2) AS average_earnings,
       cast(NULL as double) AS average_earnings_events,
       --rs_pr_prac.total_earnings/rs_pr_prac.total_hours AS earnings_per_fte,

       --column to be used for scoring calculation
       rs_pr_prac.num_employees,
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
       COALESCE(rs_pr_prac.mnth_cd, rs_pr_prac.qtr_cd, rs_pr_prac.yr_cd) as period_cd

  FROM ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_pr_pef_practitioner rs_pr_prac
  INNER JOIN (SELECT clnt_obj_id, d_work_cntry_cd, yr_cd, qtr_cd, mnth_cd,environment, num_employees AS tot_rpt_headcount
               FROM ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_pr_pef_practitioner 
              WHERE d_job_cd IS NULL AND d_hr_orgn_id IS NULL AND d_work_cntry_cd IS NULL) maxhc
    ON rs_pr_prac.clnt_obj_id = maxhc.clnt_obj_id
   --AND rs_pr_prac.d_work_cntry_cd = maxhc.d_work_cntry_cd 
   AND rs_pr_prac.yr_cd <=> maxhc.yr_cd
   AND rs_pr_prac.qtr_cd <=> maxhc.qtr_cd
   AND rs_pr_prac.mnth_cd <=> maxhc.mnth_cd
   AND rs_pr_prac.environment = maxhc.environment
   --AND rs_pr_prac.db_schema = maxhc.db_schema
  LEFT OUTER JOIN (SELECT DISTINCT environment, mnth_cd, qtr_cd, yr_cd, cast(qtr_seq_nbr as int) as qtr_seq_nbr, cast(mnth_seq_nbr as int) as mnth_seq_nbr, cast(yr_nbr as int) AS yr_seq_nbr 
                     FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) mnth
    ON rs_pr_prac.environment = mnth.environment
    --AND rs_prac.db_schema = mnth.db_schema
    AND rs_pr_prac.mnth_cd = mnth.mnth_cd
  LEFT OUTER JOIN (SELECT DISTINCT environment,qtr_cd, yr_cd, cast(qtr_seq_nbr as int) as qtr_seq_nbr, cast(yr_nbr as int) AS yr_seq_nbr 
                     FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) qtr
    ON rs_pr_prac.environment = qtr.environment
    --AND rs_prac.db_schema = qtr.db_schema 
    AND rs_pr_prac.qtr_cd = qtr.qtr_cd
  LEFT OUTER JOIN (SELECT DISTINCT environment,yr_cd, cast(yr_nbr as int) AS yr_seq_nbr 
                     FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) yr
    ON rs_pr_prac.environment = yr.environment
    --AND rs_prac.db_schema = yr.db_schema 
    AND rs_pr_prac.yr_cd = yr.yr_cd
  LEFT OUTER JOIN ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_fiscal_clnts fscl
    ON rs_pr_prac.clnt_obj_id = fscl.clnt_obj_id
    AND rs_pr_prac.environment = fscl.environment
    --AND rs_pr_prac.db_schema = fscl.db_schema
    AND fscl.dmn_ky = 3
WHERE rs_pr_prac.yr_cd IS NOT NULL
  AND rs_pr_prac.count_distinct_person != 0
  -- Filter unnecessary cells from the cube to reduce processing (right now only for non-fiscal clients)
  AND (
    fscl.clnt_obj_id IS NOT NULL OR 
    (
        COALESCE(rs_pr_prac.mnth_cd, date_format(add_months(current_date, -1), 'yyyyMM')) 
        IN (date_format(add_months(current_date, -1), 'yyyyMM'),
            date_format(add_months(current_date, -2), 'yyyyMM'),
            date_format(add_months(current_date, -13), 'yyyyMM')
        )
        -- This is previous quarter = cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)
        AND COALESCE(CASE WHEN rs_pr_prac.mnth_cd IS NULL THEN rs_pr_prac.qtr_cd ELSE NULL END, cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)) 
        IN (
            cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string),
            cast(year(add_months(current_date, -6)) *10 + ceil(month(add_months(current_date, -6))/3) as string),
            cast(year(add_months(current_date, -15)) *10 + ceil(month(add_months(current_date, -15))/3) as string)
        )
        AND rs_pr_prac.yr_cd >= date_format(add_months(current_date, -24), 'yyyy')
    )
    
  )