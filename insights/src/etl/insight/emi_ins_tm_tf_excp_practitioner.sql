SELECT 
    -- dimensions
    tm_prac.clnt_obj_id,
    mnth.mnth_seq_nbr,
    qtr.qtr_seq_nbr,
    yr.yr_seq_nbr,  
    tm_prac.job_cd,
    tm_prac.hr_orgn_id,
    tm_prac.excp_type,
    tm_prac.work_state_cd,
    tm_prac.work_loc_cd,
    tm_prac.work_cntry_cd,
    tm_prac.environment,
    --tm_prac.db_schema,
    tm_prac.yr_cd,
    
    -- facts
    tm_prac.exception_hours,
    tm_prac.exception_count,
    cast(tm_prac.exception_count as double) as exception_count_events,
    
    --column to be used for scoring calculation
    tm_prac.num_employees,
    tot_rpt_headcount,
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
    ROUND((tm_prac.num_employees / maxhc.tot_rpt_headcount)*100,1) AS percentage_headcount,

    -- Addon Column for Distribution Spec
    COALESCE(tm_prac.mnth_cd, tm_prac.qtr_cd, tm_prac.yr_cd) as period_cd

FROM
    ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_tm_tf_excp_practitioner tm_prac
LEFT OUTER JOIN (SELECT clnt_obj_id, yr_cd, qtr_cd, mnth_cd,environment, num_employees AS tot_rpt_headcount
               FROM ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_hr_waf_practitioner 
              WHERE d_job_cd IS NULL AND d_hr_orgn_id IS NULL AND d_work_cntry_cd IS NULL) maxhc
ON tm_prac.clnt_obj_id = maxhc.clnt_obj_id 
AND tm_prac.yr_cd <=> maxhc.yr_cd
AND tm_prac.qtr_cd <=> maxhc.qtr_cd
AND tm_prac.mnth_cd <=> maxhc.mnth_cd
--AND tm_prac.yr_wk_cd <=> maxhc.yr_wk_cd
AND tm_prac.environment = maxhc.environment
--AND tm_prac.db_schema = maxhc.db_schema
--LEFT OUTER JOIN (select distinct wk_cd, wk_seq_nbr FROM ${hiveconf:__GREEN_MAIN_DB__}.t_dim_day) wk
--ON tm_prac.yr_wk_cd = wk.wk_cd
LEFT OUTER JOIN (select distinct environment,mnth_cd, cast(mnth_seq_nbr as int) as mnth_seq_nbr FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) mnth
    ON tm_prac.environment = mnth.environment
    --AND tm_mngr.db_schema = tm_mngr.db_schema 
    AND tm_prac.mnth_cd = mnth.mnth_cd
    LEFT OUTER JOIN (select distinct environment,qtr_cd, cast(qtr_seq_nbr as int) as qtr_seq_nbr FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) qtr
    ON tm_prac.environment = qtr.environment
    --AND tm_mngr.db_schema = tm_mngr.db_schema 
    AND tm_prac.qtr_cd = qtr.qtr_cd
    LEFT OUTER JOIN (select distinct environment,yr_cd, cast(yr_nbr as int) as yr_seq_nbr FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) yr
    ON tm_prac.environment = yr.environment
    --AND tm_mngr.db_schema = yr.db_schema 
    AND tm_prac.yr_cd = yr.yr_cd
-- LEFT OUTER JOIN ${hiveconf:__GREEN_MAIN_DB__}.rosie_xtrct_t_fiscal_clnts fscl
-- ON tm_prac.clnt_obj_id = fscl.clnt_obj_id
-- AND tm_prac.environment = fscl.environment
-- AND tm_prac.db_schema = fscl.db_schema
-- AND fscl.dmn_ky = 5
WHERE 
    tm_prac.yr_cd IS NOT NULL AND tm_prac.clndr_wk_cd IS NULL
    -- Filter unnecessary cells from the cube to reduce processing (right now only for non-fiscal clients)
    AND (
        -- fscl.clnt_obj_id IS NOT NULL OR 
        -- (
            COALESCE(tm_prac.mnth_cd, date_format(add_months(current_date, -1), 'yyyyMM')) 
            IN (date_format(add_months(current_date, -1), 'yyyyMM'),
                date_format(add_months(current_date, -2), 'yyyyMM'),
                date_format(add_months(current_date, -13), 'yyyyMM')
            )
            -- This is previous quarter = cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)
            AND COALESCE(CASE WHEN tm_prac.mnth_cd IS NULL THEN tm_prac.qtr_cd ELSE NULL END, cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)) 
            IN (
                cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string),
                cast(year(add_months(current_date, -6)) *10 + ceil(month(add_months(current_date, -6))/3) as string),
                cast(year(add_months(current_date, -15)) *10 + ceil(month(add_months(current_date, -15))/3) as string)
            )
            AND tm_prac.yr_cd >= date_format(add_months(current_date, -24), 'yyyy')
        -- )
    )