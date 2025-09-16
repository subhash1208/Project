SELECT 
    -- dimensions
    tm_supvr.clnt_obj_id,
    mnth.mnth_seq_nbr,
    qtr.qtr_seq_nbr,
    yr.yr_seq_nbr,
    tm_supvr.job_cd,
    tm_supvr.hr_orgn_id,
    CASE WHEN tm_supvr.supvr_pers_obj_id = 'DEFAULT' THEN NULL 
         ELSE tm_supvr.supvr_pers_obj_id 
    END AS supvr_pers_obj_id,
    tm_supvr.work_state_cd,
    tm_supvr.work_loc_cd,
    tm_supvr.work_cntry_cd,
    tm_supvr.environment,
    --tm_supvr.db_schema,
    tm_supvr.yr_cd,    

    -- facts
    tm_supvr.edits_count,
    cast(tm_supvr.edits_count as double) as edits_count_events,

    --column to be used for scoring calculation
    tm_supvr.num_employees,
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
    tot_rpt_headcount,
    ROUND((tm_supvr.num_employees / maxhc.tot_rpt_headcount)*100,1) AS percentage_headcount,

    -- Addon Column for Distribution Spec
    COALESCE(tm_supvr.mnth_cd, tm_supvr.qtr_cd, tm_supvr.yr_cd) as period_cd

FROM
    ${hiveconf:__GREEN_MAIN_DB__}.emi_cube_tm_tf_edits_supervisor tm_supvr
    LEFT OUTER JOIN 
    (SELECT hd_cnt.clnt_obj_id,
            hd_cnt.supvr_pers_obj_id, 
            hd_cnt.yr_cd,
            hd_cnt.qtr_cd,
            hd_cnt.mnth_cd,
            hd_cnt.supvr_headcount as tot_rpt_headcount,
            hd_cnt.environment FROM ${hiveconf:__BLUE_MAIN_DB__}.emi_prep_hr_waf_supvr_headcnt hd_cnt 
            INNER JOIN ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_pers pers
            ON hd_cnt.clnt_obj_id = pers.clnt_obj_id
            AND hd_cnt.supvr_pers_obj_id = pers.pers_obj_id
            AND hd_cnt.environment = pers.environment
            AND hd_cnt.db_schema = pers.db_schema
            AND (case when hd_cnt.rpt_access = 'DI' then 1 else 0 end) = pers.DIR_AND_INDIR_SUP_ACCS_IND) maxhc
   ON tm_supvr.clnt_obj_id = maxhc.clnt_obj_id
   AND tm_supvr.supvr_pers_obj_id <=> maxhc.supvr_pers_obj_id
   AND tm_supvr.yr_cd <=> maxhc.yr_cd
   AND tm_supvr.qtr_cd <=> maxhc.qtr_cd
   AND tm_supvr.mnth_cd <=> maxhc.mnth_cd
   --AND tm_mngr.yr_wk_cd <=> maxhc.yr_wk_cd
   AND tm_supvr.environment = maxhc.environment
   --AND tm_supvr.db_schema = maxhc.db_schema
   --LEFT OUTER JOIN (select distinct wk_cd, wk_seq_nbr FROM ${hiveconf:__GREEN_MAIN_DB__}.t_dim_day) wk
   --ON tm_supvr.yr_wk_cd = wk.wk_cd
   LEFT OUTER JOIN (select distinct environment,mnth_cd, cast(mnth_seq_nbr as int) as mnth_seq_nbr FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) mnth
    ON tm_supvr.environment = mnth.environment
    --AND tm_mngr.db_schema = tm_mngr.db_schema 
    AND tm_supvr.mnth_cd = mnth.mnth_cd
    LEFT OUTER JOIN (select distinct environment,qtr_cd, cast(qtr_seq_nbr as int) as qtr_seq_nbr FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) qtr
    ON tm_supvr.environment = qtr.environment
    --AND tm_mngr.db_schema = tm_mngr.db_schema 
    AND tm_supvr.qtr_cd = qtr.qtr_cd
    LEFT OUTER JOIN (select distinct environment,yr_cd, cast(yr_nbr as int) as yr_seq_nbr FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_dim_day) yr
    ON tm_supvr.environment = yr.environment
    --AND tm_mngr.db_schema = yr.db_schema 
    AND tm_supvr.yr_cd = yr.yr_cd
    -- LEFT OUTER JOIN ${hiveconf:__GREEN_MAIN_DB__}.rosie_xtrct_t_fiscal_clnts fscl
    -- ON tm_supvr.clnt_obj_id = fscl.clnt_obj_id
    -- AND tm_supvr.environment = fscl.environment
    -- AND tm_supvr.db_schema = fscl.db_schema
    -- AND fscl.dmn_ky = 5
WHERE 
  tm_supvr.yr_cd IS NOT NULL AND tm_supvr.clndr_wk_cd IS NULL
  -- Filter unnecessary cells from the cube to reduce processing (right now only for non-fiscal clients)
  AND (
    -- fscl.clnt_obj_id IS NOT NULL OR 
    -- (
        COALESCE(tm_supvr.mnth_cd, date_format(add_months(current_date, -1), 'yyyyMM')) 
        IN (date_format(add_months(current_date, -1), 'yyyyMM'),
            date_format(add_months(current_date, -2), 'yyyyMM'),
            date_format(add_months(current_date, -13), 'yyyyMM')
        )
        -- This is previous quarter = cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)
        AND COALESCE(CASE WHEN tm_supvr.mnth_cd IS NULL THEN tm_supvr.qtr_cd ELSE NULL END, cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string)) 
        IN (
            cast(year(add_months(current_date, -3)) *10 + ceil(month(add_months(current_date, -3))/3) as string),
            cast(year(add_months(current_date, -6)) *10 + ceil(month(add_months(current_date, -6))/3) as string),
            cast(year(add_months(current_date, -15)) *10 + ceil(month(add_months(current_date, -15))/3) as string)
        )
        AND tm_supvr.yr_cd >= date_format(add_months(current_date, -24), 'yyyy')
    -- )
  )