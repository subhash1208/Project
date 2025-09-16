DROP TABLE IF EXISTS ${hiveconf:__GREEN_MAIN_DB__}.emi_exp_insights_summary;

CREATE TABLE IF NOT EXISTS ${hiveconf:__GREEN_MAIN_DB__}.emi_exp_insights_summary 
USING PARQUET AS
SELECT 
  ins.rec_crt_ts,
  environment,
  db_schema,
  clnt_obj_id,
  metric_name,
  CASE
    WHEN qtr_cd IS NULL
    THEN 'YEARLY'
    WHEN mnth_cd IS NULL
    THEN 'QUARTERLY'
    WHEN wk_cd IS NULL
    THEN 'MONTHLY'
    ELSE 'WEEKLY'
  END AS time_period,
  CASE
    WHEN job_cd IS NOT NULL
    THEN 'JOB'
    WHEN hr_orgn_id IS NOT NULL
    THEN 'DEPARTMENT'
    --WHEN work_loc_cd IS NOT NULL
    --THEN 'WORK LOCATION'
    WHEN city_id IS NOT NULL
    THEN 'CITY'
    WHEN state_prov_cd IS NOT NULL
    THEN 'STATE'
    ELSE 'UNKNOWN'
  END AS dimension,
  ins_type,
  REGEXP_REPLACE(ins_rsn,'_PERCENTILE_RANKING','') AS ins_rsn,
  CASE
    WHEN mngr_pers_obj_id IS NULL
    THEN 'EXECUTIVE'
    ELSE 'MANAGER'
  END                              AS persona,
  count(1)                         AS num_insights,
  count(DISTINCT mngr_pers_obj_id) AS num_managers
FROM ${hiveconf:__GREEN_MAIN_DB__}.t_fact_insights ins
INNER JOIN ${hiveconf:__RO_GREEN_RAW_DB__}.emi_meta_dw_metrics m
ON m.metric_ky = ins.mtrc_ky
--WHERE 
--  ins.environment = '${hiveconf:environment}'
GROUP BY rec_crt_ts,
  environment,
  db_schema,
  clnt_obj_id,
  metric_name,
  CASE
    WHEN qtr_cd IS NULL
    THEN 'YEARLY'
    WHEN mnth_cd IS NULL
    THEN 'QUARTERLY'
    WHEN wk_cd IS NULL
    THEN 'MONTHLY'
    ELSE 'WEEKLY'
  END,
  CASE
    WHEN job_cd IS NOT NULL
    THEN 'JOB'
    WHEN hr_orgn_id IS NOT NULL
    THEN 'DEPARTMENT'
    --WHEN work_loc_cd IS NOT NULL
    --THEN 'WORK LOCATION'
    WHEN city_id IS NOT NULL
    THEN 'CITY'
    WHEN state_prov_cd IS NOT NULL
    THEN 'STATE'
    ELSE 'UNKNOWN'
  END,
  ins_type,
  REGEXP_REPLACE(ins_rsn,'_PERCENTILE_RANKING',''),
  CASE
    WHEN mngr_pers_obj_id IS NULL
    THEN 'EXECUTIVE'
    ELSE 'MANAGER'
  END;

DROP TABLE IF EXISTS ${hiveconf:__GREEN_MAIN_DB__}.emi_exp_mngrs_with_less_insights;

CREATE TABLE IF NOT EXISTS ${hiveconf:__GREEN_MAIN_DB__}.emi_exp_mngrs_with_less_insights 
USING PARQUET AS
SELECT 
    ins.rec_crt_ts,
    mngrs.environment,
    mngrs.db_schema,
    mngrs.clnt_obj_id,
    mngrs.mngr_pers_obj_id,
    mngrs.approx_reportees,
    count(1) as num_insights
FROM 
    (SELECT
      environment, db_schema, clnt_obj_id, mngr_pers_obj_id, count(1) as approx_reportees
    FROM
        (SELECT
            DISTINCT mngr.environment, mngr.db_schema, mngr.clnt_obj_id, mngr.mngr_pers_obj_id, mngr.pers_obj_id
        FROM ${hiveconf:__GREEN_MAIN_DB__}.emi_prep_hr_waf_mngr mngr
        INNER JOIN (
          SELECT clnt_obj_id, max(mnth_cd) as mnth_cd FROM ${hiveconf:__GREEN_MAIN_DB__}.emi_prep_hr_waf_mngr
          --WHERE environment = '${hiveconf:environment}'
          GROUP BY clnt_obj_id
        ) mnth 
        ON mnth.clnt_obj_id = mngr.clnt_obj_id
        AND mnth.mnth_cd = mngr.mnth_cd
        --WHERE mngr.environment = '${hiveconf:environment}'
        ) ignored_alias
    GROUP BY environment, db_schema, clnt_obj_id, mngr_pers_obj_id
    ) mngrs
LEFT OUTER JOIN ${hiveconf:__GREEN_MAIN_DB__}.t_fact_insights ins
    ON mngrs.environment = ins.environment
    AND mngrs.db_schema = ins.db_schema
    AND mngrs.clnt_obj_id = ins.clnt_obj_id
    AND mngrs.mngr_pers_obj_id = ins.mngr_pers_obj_id
    AND ins.mngr_pers_obj_id IS NOT NULL
--WHERE 
--    mngrs.environment = '${hiveconf:environment}'
GROUP BY 
    rec_crt_ts,
    mngrs.environment,
    mngrs.db_schema,
    mngrs.clnt_obj_id,
    mngrs.mngr_pers_obj_id,
    mngrs.approx_reportees
HAVING 
    num_insights < 10;