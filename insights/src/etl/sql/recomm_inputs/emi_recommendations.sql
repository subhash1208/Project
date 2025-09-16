SELECT
  meta.*,
  usr.*,  
  fd_bk.is_useful_flag,
  fd_bk.feed_back_txt,
  fd_bk.headline_val_type,
  CASE WHEN meta.mtrc_ky = 2 THEN 1 ELSE 0 END AS  metric_2,
  CASE WHEN meta.mtrc_ky = 5 THEN 1 ELSE 0 END AS  metric_5,
  CASE WHEN meta.mtrc_ky = 57 THEN 1 ELSE 0 END AS metric_57,
  CASE WHEN meta.mtrc_ky = 59 THEN 1 ELSE 0 END AS metric_59,
  CASE WHEN meta.mtrc_ky = 60 THEN 1 ELSE 0 END AS metric_60,
  CASE WHEN meta.mtrc_ky = 63 THEN 1 ELSE 0 END AS metric_63,
  CASE WHEN meta.mtrc_ky = 65 THEN 1 ELSE 0 END AS metric_65,
  CASE WHEN meta.mtrc_ky = 69 THEN 1 ELSE 0 END AS metric_69,
  CASE WHEN meta.mtrc_ky = 73 THEN 1 ELSE 0 END AS metric_73,
  CASE WHEN meta.mtrc_ky = 74 THEN 1 ELSE 0 END AS metric_74,
  CASE WHEN meta.mtrc_ky = 76 THEN 1 ELSE 0 END AS metric_76,
  CASE WHEN meta.mtrc_ky = 78 THEN 1 ELSE 0 END AS metric_78,
  CASE WHEN meta.mtrc_ky = 79 THEN 1 ELSE 0 END AS metric_79,
  CASE WHEN meta.mtrc_ky = 201 THEN 1 ELSE 0 END AS metric_201,
  CASE WHEN meta.mtrc_ky = 202 THEN 1 ELSE 0 END AS metric_202,
  CASE WHEN meta.ins_type = 'CLIENT_INTERNAL' THEN 1 ELSE 0 END AS ins_type_client_internal,
  CASE WHEN meta.ins_type = 'CLIENT_INTERNAL_BM' THEN 1 ELSE 0 END AS ins_type_client_internal_bm,
  CASE WHEN meta.ins_type = 'CLIENT_VS_BM' THEN 1 ELSE 0 END AS ins_type_client_vs_bm,
  CASE WHEN meta.ins_rsn = 'MIN_PERCENTILE_RANKING' THEN 1 ELSE 0 END AS ins_rsn_min_percentile_ranking,
  CASE WHEN meta.ins_rsn = 'MAX_PERCENTILE_RANKING' THEN 1 ELSE 0 END AS ins_rsn_max_percentile_ranking,
  CASE WHEN meta.ins_rsn IN ('PCTG_DIFF','PCTG_DIFF_TIME') THEN 1 ELSE 0 END AS ins_rsn_pct_diff,
  CASE WHEN meta.ins_rsn IN ('ABS_DIFF','ABS_DIFF_TIME') THEN 1 ELSE 0 END AS ins_rsn_abs_diff,
  CASE WHEN meta.mngr_pers_obj_id IS NOT NULL THEN 1 ELSE 0 END AS is_manager_1,
  CASE WHEN meta.mngr_pers_obj_id IS NULL THEN 1 ELSE 0 END AS is_manager_0,
  case WHEN meta.mngr_pers_obj_id IS NOT NULL THEN meta.mngr_pers_obj_id ELSE meta.clnt_obj_id END AS usr_id,
  intr_h.hist_metric_2,
  intr_h.hist_metric_5,
  intr_h.hist_metric_57,
  intr_h.hist_metric_59,
  intr_h.hist_metric_60,
  intr_h.hist_metric_63,
  intr_h.hist_metric_65,
  intr_h.hist_metric_69,
  intr_h.hist_metric_73,
  intr_h.hist_metric_74,
  intr_h.hist_metric_76,
  intr_h.hist_metric_78,
  intr_h.hist_metric_79,
  intr_h.hist_metric_201,
  intr_h.hist_metric_202,
  hist.ins_serv_dt,
  hist.usr_view_ind,  
  hist.delvy_chanl,
  get_json_object(ins_json,'$.insight.diff') as diff,
  get_json_object(ins_json,'$.insight.normalised_diff') as normalised_diff,
  get_json_object(ins_json,'$.insight.zscore_diff') as zscore_diff,
  get_json_object(ins_json,'$.insight.zscore_percentage_diff') as zscore_percentage_diff,
  get_json_object(ins_json,'$.insight.percentile_rank') as percentile_rank,
  get_json_object(ins_json,'$.insight.normalised_zscore_diff') as normalised_zscore_diff,
  get_json_object(ins_json,'$.insight.normalised_zscore_percentage_diff') as normalised_zscore_percentage_diff,
  COALESCE(CASE WHEN meta.mnth_cd IS NOT NULL THEN "Month" ELSE NULL END,
           CASE WHEN meta.qtr_cd  IS NOT NULL THEN "Quarter"  ELSE NULL END,
           CASE WHEN meta.yr_cd   IS NOT NULL THEN "Year"   ELSE NULL END) as time_grain,
  CASE WHEN COALESCE(
           CASE WHEN meta.mnth_cd IS NOT NULL THEN "Month" ELSE NULL END,
           CASE WHEN meta.qtr_cd  IS NOT NULL THEN "Quarter"  ELSE NULL END,
           CASE WHEN meta.yr_cd   IS NOT NULL THEN "Year"   ELSE NULL END
                    ) = "Month"        THEN 1
       WHEN COALESCE(
           CASE WHEN meta.mnth_cd IS NOT NULL THEN "Month" ELSE NULL END,
           CASE WHEN meta.qtr_cd  IS NOT NULL THEN "Quarter"  ELSE NULL END,
           CASE WHEN meta.yr_cd   IS NOT NULL THEN "Year"   ELSE NULL END
                    ) = "Year"          THEN 1/month(to_date(meta.rec_crt_ts))
       ELSE  CASE WHEN month(to_date(meta.rec_crt_ts)) % 3 = 1 THEN 3/3
                  WHEN month(to_date(meta.rec_crt_ts)) % 3 = 2 THEN 2/3
                  WHEN month(to_date(meta.rec_crt_ts)) % 3 = 0 THEN 1/3 
              END 
  END as time_grain_weight,
  CASE WHEN fd_bk.is_useful_flag = 'y' THEN 'VSY'
       WHEN fd_bk.is_useful_flag = 'n' THEN 'VSN'
       WHEN hist.usr_view_ind > 0  THEN 'SY'
       WHEN mv.metric_ky IS NOT NULL AND (mv.ins_hash_val IS NULL OR mv.ins_hash_val = '') THEN 'MY'
       WHEN mv.ins_hash_val IS NOT NULL THEN 'SY'
       WHEN (mv.ins_hash_val IS NULL OR mv.ins_hash_val = '') AND hist.delvy_chanl = 'mobile-notification' THEN 'SN'
       WHEN hist.usr_view_ind = 0 THEN 'SN'
       ELSE 'MN' END as clicks_dist,
  CASE WHEN fd_bk.is_useful_flag = 'y' THEN 1
       WHEN fd_bk.is_useful_flag = 'n' THEN 0
       WHEN hist.usr_view_ind > 0 THEN 1
     --WHEN hist.usr_view_ind = 0 THEN 0 # Issue with usr_view_ind in history table 
       WHEN mv.metric_ky IS NOT NULL AND (mv.ins_hash_val IS NULL OR mv.ins_hash_val = '') THEN 1
       WHEN mv.ins_hash_val IS NOT NULL THEN 1
       WHEN (mv.ins_hash_val IS NULL OR mv.ins_hash_val = '') AND hist.delvy_chanl = 'mobile-notification' THEN 0       
       ELSE 0 END as clicks
FROM 
(
  SELECT
   clnt_obj_id,
   pers_obj_id,
   ins_hash_val,
   ins_serv_dt,
   usr_view_ind,
   delvy_chanl,
   environment,
   db_schema
    FROM
    (
      SELECT 
          clnt_obj_id,
          pers_obj_id,
          ins_hash_val,
          cast(to_date(ins_serv_dt) as string) as ins_serv_dt,
          usr_view_ind,
          delvy_chanl,
          environment,
          db_schema,          
          ROW_NUMBER() OVER (PARTITION BY clnt_obj_id,ins_hash_val ORDER BY  rec_crt_ts desc) as rank
          FROM ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_fact_ins_hist
          WHERE  delvy_chanl in ('mobile-insight','mobile-notification')        
    ) sub
    WHERE rank = 1 
) hist
INNER JOIN 
(
  SELECT * 
  FROM    
  (
    SELECT  *,
     ROW_NUMBER() OVER(PARTITION BY ins_hash_val ORDER BY rec_crt_ts DESC ) AS rank
     FROM
   ${hiveconf:__BLUE_MAIN_DB__}.emi_recommend_ins_meta 
  ) mt 
  WHERE rank = 1
) meta
   ON hist.ins_hash_val = meta.ins_hash_val
   AND hist.clnt_obj_id = meta.clnt_obj_id
   AND hist.environment = meta.environment
LEFT OUTER JOIN 
  (
   SELECT 
        clnt_obj_id,
        pers_obj_id,
        ins_hash_val,
        is_useful_flag,
        feed_back_txt,
        headline_val_type,
        environment
   FROM 
     (
       SELECT 
        clnt_obj_id,
        pers_obj_id,
        ins_hash_val,
        is_useful_flag,
        feed_back_txt,
        headline_val_type,
        environment,
        ROW_NUMBER() OVER (PARTITION BY clnt_obj_id,pers_obj_id,ins_hash_val ORDER BY  rec_crt_ts desc) as rank
      FROM 
      ${hiveconf:__RO_BLUE_RAW_DB__}.dwh_t_headline_notfn_feed_back 
     ) fd
  WHERE rank = 1 
   ) fd_bk
   ON hist.ins_hash_val = fd_bk.ins_hash_val
   AND hist.clnt_obj_id = fd_bk.clnt_obj_id
   AND hist.environment = fd_bk.environment
LEFT OUTER JOIN 
( 
  SELECT 
  clnt_obj_id,
  pers_obj_id,
  CASE WHEN sum(mv[2]) IS NULL THEN 0 ELSE sum(mv[2]) END AS hist_metric_2,
  CASE WHEN sum(mv[5]) IS NULL THEN 0 ELSE sum(mv[5]) END AS hist_metric_5,
  CASE WHEN sum(mv[57]) IS NULL THEN 0 ELSE sum(mv[57]) END AS hist_metric_57,
  CASE WHEN sum(mv[59]) IS NULL THEN 0 ELSE sum(mv[59]) END AS hist_metric_59,
  CASE WHEN sum(mv[60]) IS NULL THEN 0 ELSE sum(mv[60]) END AS hist_metric_60,
  CASE WHEN sum(mv[63]) IS NULL THEN 0 ELSE sum(mv[63]) END AS hist_metric_63,
  CASE WHEN sum(mv[65]) IS NULL THEN 0 ELSE sum(mv[65]) END AS hist_metric_65,
  CASE WHEN sum(mv[69]) IS NULL THEN 0 ELSE sum(mv[69]) END AS hist_metric_69,
  CASE WHEN sum(mv[73]) IS NULL THEN 0 ELSE sum(mv[73]) END AS hist_metric_73,
  CASE WHEN sum(mv[74]) IS NULL THEN 0 ELSE sum(mv[74]) END AS hist_metric_74,
  CASE WHEN sum(mv[76]) IS NULL THEN 0 ELSE sum(mv[76]) END AS hist_metric_76,
  CASE WHEN sum(mv[78]) IS NULL THEN 0 ELSE sum(mv[78]) END AS hist_metric_78,
  CASE WHEN sum(mv[79]) IS NULL THEN 0 ELSE sum(mv[79]) END AS hist_metric_79,
  CASE WHEN sum(mv[201]) IS NULL THEN 0  ELSE sum(mv[201]) END AS hist_metric_201,
  CASE WHEN sum(mv[202]) IS NULL THEN 0 ELSE sum(mv[202])  END AS hist_metric_202
  FROM 
    (
      SELECT 
           ful.orgid as clnt_obj_id,
           ful.aoid as pers_obj_id,
           ful.ins_hash_val,
           map(cast(metric_ky as int),1) as mv 
      FROM  ${hiveconf:__BLUE_MAIN_DB__}.emi_prep_recom_metric_view ful
      INNER JOIN 
          (  SELECT  
                orgid,
                aoid,
                event_date
              FROM  
                  ( SELECT 
                     orgid, 
                     aoid, 
                     to_date(EVENT_TIME) as event_date,
                     ROW_NUMBER() OVER (PARTITION BY orgid,aoid ORDER BY to_date(EVENT_TIME) desc) as rank 
                     FROM ${hiveconf:__BLUE_MAIN_DB__}.emi_prep_recom_metric_view
                   ) sub
                    WHERE rank = 1 
            ) recent_event
       ON  ful.orgid = recent_event.orgid
       AND ful.aoid = recent_event.aoid
       WHERE  to_date(ful.EVENT_TIME) > to_date(DATE_SUB(recent_event.event_date,90))
       AND cast(ful.metric_ky as int) in (2,5,57,59,60,63,65,69,73,74,76,78,79,201,202)
    ) int_hist_mtrc_cnt
  GROUP BY clnt_obj_id,pers_obj_id
 ) intr_h 
  ON meta.clnt_obj_id = intr_h.clnt_obj_id
  AND meta.mngr_pers_obj_id = intr_h.pers_obj_id
LEFT OUTER JOIN 
(
    SELECT 
         clnt_obj_id,
         pers_obj_id,
         metric_ky,
         ins_hash_val
    FROM 
        ( 
             SELECT orgid as clnt_obj_id, 
                    aoid as pers_obj_id, 
                    cast(metric_ky as int) as metric_ky, 
                    ins_hash_val, 
                    mobile_cnt, 
                    is_executive, 
                    ROW_NUMBER() OVER (PARTITION BY orgid,aoid,cast(metric_ky as int) ORDER BY  cast(metric_ky as int) desc) as rank 
                FROM ${hiveconf:__BLUE_MAIN_DB__}.emi_prep_recom_metric_view
        ) mviews  
    WHERE rank = 1
 and cast(metric_ky as int) in (2,5,57,59,60,63,65,69,73,74,76,78,79,201,202)
) mv 
  ON meta.clnt_obj_id = mv.clnt_obj_id
  AND meta.mngr_pers_obj_id = mv.pers_obj_id
  AND meta.mtrc_ky = mv.metric_ky
LEFT OUTER JOIN 
(
  SELECT 
    employee.*,
    0 as indirect_children,
    0 as direct_children,
    0 as total_children
    FROM
    (   
      SELECT 
       ooid,
        aoid,
        ap_company_code,
        ap_product_code,
        employee_type_,
        regular_temporary_,
        full_time_part_time_,
        flsa_status_,
        age_band_ky_,
        is_manager_,
        generation_band_,
        generation_band_ky_,
        job_title_,
        eeo1_,
        tenure_band_ky_,
        tenure_band_,
        rate_type_,
        regular_pay_,
        naics,
        jl_job_level,
        jl_job_level_validity_flag,
        janzz_best_concept,
        janzz_best_concept_cd,
        janzz_best_concept_score,
        janzz_adp_func,
        janzz_adp_func_cd,
        janzz_onet,
        janzz_adp_lens_cd
      FROM 
      (
       SELECT
        ooid,
        aoid,
        ap_company_code,
        ap_product_code,
        regexp_replace(employee_type_,'[|]','') as employee_type_,
        regular_temporary_,
        full_time_part_time_,
        flsa_status_,
        age_band_ky_,
        is_manager_,
        regexp_replace(generation_band_,'[|]','') as generation_band_,
        generation_band_ky_,
        regexp_replace(job_title_,'[|]','') as job_title_,
        regexp_replace(eeo1_,'[|]','') as eeo1_,
        tenure_band_ky_,
        tenure_band_,
        rate_type_,
        regular_pay_,
        naics,
        jl_job_level,
        jl_job_level_validity_flag,
        regexp_replace(janzz_best_concept,'[|]','') as janzz_best_concept,
        janzz_best_concept_cd,
        janzz_best_concept_score,
        janzz_adp_func,
        regexp_replace(janzz_adp_func_cd,'[|]','') as janzz_adp_func_cd,
        regexp_replace(janzz_onet,'[|]','') as janzz_onet,
        janzz_adp_lens_cd,
        ROW_NUMBER() OVER (PARTITION BY aoid ORDER BY yyyymm desc) as rank
       FROM ${hiveconf:__RO_BLUE_LANDING_MAIN_DB__}.employee_monthly em
       INNER JOIN (SELECT MAX(yyyymm) max_yyyymm FROM ${hiveconf:__RO_BLUE_LANDING_MAIN_DB__}.employee_monthly) mx
       ON em.yyyymm = mx.max_yyyymm
      ) em
       WHERE rank = 1 
    ) employee
) usr
on hist.pers_obj_id = usr.aoid