SELECT 
    -- dimensions
    clnt_obj_id,
    mnth.mnth_seq_nbr,
    qtr.qtr_seq_nbr,
    yr.yr_seq_nbr,
    job_cd,
    hr_orgn_id,
    mngr_pers_obj_id,
    work_state_cd,
    work_loc_cd,
    work_cntry_cd,
    tm_mngr.environment,
    tm_mngr.yr_cd,
    -- facts    
    edits_count,
    edits_count_events,
    num_employees,
    tot_rpt_headcount,
    zscore_type,
    percentage_headcount,
    period_cd
FROM
(SELECT    
    clnt_obj_id,
    mnth_cd,
    qtr_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    mngr_pers_obj_id,
    work_state_cd,
    work_loc_cd,
    work_cntry_cd,
    environment,
    yr_cd,   
    edits_count,
    edits_count_events,   
    num_employees,
    tot_rpt_headcount,
    zscore_type,
    percentage_headcount,    
    period_cd
FROM
    (SELECT 
    -- dimensions
    tm_mngr.clnt_obj_id,
    tm_mngr.mnth_cd,
    tm_mngr.qtr_cd,
    tm_mngr.clndr_wk_cd,
    tm_mngr.job_cd,
    tm_mngr.hr_orgn_id,
    CASE WHEN tm_mngr.mngr_pers_obj_id = 'DEFAULT' THEN NULL 
         ELSE tm_mngr.mngr_pers_obj_id 
    END AS mngr_pers_obj_id,
    tm_mngr.work_state_cd,
    tm_mngr.work_loc_cd,
    tm_mngr.work_cntry_cd,
    tm_mngr.environment,
    --tm_mngr.db_schema,
    tm_mngr.yr_cd,
    
    -- facts    
    tm_mngr.edits_count,
    cast(tm_mngr.edits_count as double) as edits_count_events,

    --column to be used for scoring calculation
    tm_mngr.num_employees,
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
    ROUND((tm_mngr.num_employees / maxhc.tot_rpt_headcount)*100,1) AS percentage_headcount,

    -- Addon Column for Distribution Spec
    COALESCE(tm_mngr.mnth_cd, tm_mngr.qtr_cd, tm_mngr.yr_cd) as period_cd

FROM
    ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_tm_tf_edits_manager tm_mngr
LEFT OUTER JOIN (SELECT clnt_obj_id, mngr_pers_obj_id, yr_cd, qtr_cd, mnth_cd,environment, num_employees AS tot_rpt_headcount 
               FROM ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_hr_waf_manager 
              WHERE d_job_cd IS NULL AND d_hr_orgn_id IS NULL AND d_work_cntry_cd IS NULL) maxhc
ON tm_mngr.clnt_obj_id = maxhc.clnt_obj_id
AND tm_mngr.mngr_pers_obj_id <=> maxhc.mngr_pers_obj_id
AND tm_mngr.yr_cd <=> maxhc.yr_cd
AND tm_mngr.qtr_cd <=> maxhc.qtr_cd
AND tm_mngr.mnth_cd <=> maxhc.mnth_cd
AND tm_mngr.environment = maxhc.environment)a

UNION ALL

SELECT 
    -- dimensions
    clnt_obj_id,
    mnth_cd,
    qtr_cd,
    clndr_wk_cd,
    job_cd,
    hr_orgn_id,
    mngr_pers_obj_id,
    work_state_cd,
    work_loc_cd,
    work_cntry_cd,
    environment,    
    yr_cd,    
    edits_count,
    edits_count_events,    
    num_employees,
    tot_rpt_headcount,
    zscore_type,
    percentage_headcount,    
    period_cd
FROM
(SELECT 
    -- dimensions
    tm_mngr.clnt_obj_id,
    tm_mngr.mnth_cd,
    tm_mngr.qtr_cd,
    tm_mngr.clndr_wk_cd,
    tm_mngr.job_cd,
    tm_mngr.hr_orgn_id,
    CASE WHEN tm_mngr.mngr_pers_obj_id = 'DEFAULT' THEN NULL 
         ELSE tm_mngr.mngr_pers_obj_id 
    END AS mngr_pers_obj_id,
    tm_mngr.work_state_cd,
    tm_mngr.work_loc_cd,
    tm_mngr.work_cntry_cd,
    tm_mngr.environment,
    --tm_mngr.db_schema,
    tm_mngr.yr_cd,
    
    -- facts    
    tm_mngr.edits_count,
    cast(tm_mngr.edits_count as double) as edits_count_events,

    --column to be used for scoring calculation
    tm_mngr.num_employees,
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
    ROUND((tm_mngr.num_employees / maxhc.tot_rpt_headcount)*100,1) AS percentage_headcount,

    -- Addon Column for Distribution Spec
    COALESCE(tm_mngr.mnth_cd, tm_mngr.qtr_cd, tm_mngr.yr_cd) as period_cd

FROM
    ${hiveconf:__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_etime_mngr_agg tm_mngr
INNER JOIN (SELECT clnt_obj_id, mngr_pers_obj_id, yr_cd, qtr_cd, mnth_cd,environment, num_employees AS tot_rpt_headcount 
               FROM ${hiveconf:__BLUE_MAIN_DB__}.emi_prep_tm_tf_edits_etime_mngr_agg  
              WHERE job_cd is null and hr_orgn_id is null and work_cntry_cd is null and work_state_cd is null and work_loc_cd is null and clndr_wk_cd is null) maxhc
ON tm_mngr.clnt_obj_id = maxhc.clnt_obj_id
AND tm_mngr.mngr_pers_obj_id <=> maxhc.mngr_pers_obj_id
AND tm_mngr.yr_cd <=> maxhc.yr_cd
AND tm_mngr.qtr_cd <=> maxhc.qtr_cd
AND tm_mngr.mnth_cd <=> maxhc.mnth_cd
--AND tm_mngr.yr_wk_cd <=> maxhc.yr_wk_cd
AND tm_mngr.environment = maxhc.environment)b)tm_mngr
LEFT OUTER JOIN (select distinct environment,mnth_cd, cast(mnth_seq_nbr as int) as mnth_seq_nbr FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) mnth
ON tm_mngr.environment = mnth.environment
--AND tm_mngr.db_schema = tm_mngr.db_schema 
AND tm_mngr.mnth_cd = mnth.mnth_cd
LEFT OUTER JOIN (select distinct environment,qtr_cd, cast(qtr_seq_nbr as int) as qtr_seq_nbr FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) qtr
ON tm_mngr.environment = qtr.environment
--AND tm_mngr.db_schema = tm_mngr.db_schema 
AND tm_mngr.qtr_cd = qtr.qtr_cd
LEFT OUTER JOIN (select distinct environment,yr_cd, cast(yr_nbr as int) as yr_seq_nbr FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) yr
ON tm_mngr.environment = yr.environment
--AND tm_mngr.db_schema = yr.db_schema 
AND tm_mngr.yr_cd = yr.yr_cd
-- LEFT OUTER JOIN ${hiveconf:__GREEN_MAIN_DB__}.rosie_xtrct_t_fiscal_clnts fscl
-- ON tm_mngr.clnt_obj_id = fscl.clnt_obj_id
-- AND tm_mngr.environment = fscl.environment
-- AND tm_mngr.db_schema = fscl.db_schema
-- AND fscl.dmn_ky = 5
WHERE 
  tm_mngr.yr_cd IS NOT NULL AND tm_mngr.clndr_wk_cd IS NULL
  -- Filter unnecessary cells from the cube to reduce processing (right now only for non-fiscal clients)
  AND (
    -- fscl.clnt_obj_id IS NOT NULL OR 
    -- (
        COALESCE(tm_mngr.mnth_cd, date_format(add_months(current_date, -1), 'yyyyMM')) 
        IN (date_format(add_months(current_date, -1), 'yyyyMM'),
            date_format(add_months(current_date, -2), 'yyyyMM'),
            date_format(add_months(current_date, -13), 'yyyyMM')
        )
        -- This is previous quarter = cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)
        AND COALESCE(CASE WHEN tm_mngr.mnth_cd IS NULL THEN tm_mngr.qtr_cd ELSE NULL END, cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)) 
        IN (
            cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string),
            cast(year(add_months(current_date, -6)) *10 + ceil(month(add_months(current_date, -6))/3) as string),
            cast(year(add_months(current_date, -15)) *10 + ceil(month(add_months(current_date, -15))/3) as string)
        )
        AND tm_mngr.yr_cd >= date_format(add_months(current_date, -24), 'yyyy')
    -- )
  )