set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${hiveconf:__GREEN_MAIN_DB__}.clnt_min_max_mtrc_summ;

CREATE TABLE IF NOT EXISTS ${hiveconf:__GREEN_MAIN_DB__}.clnt_min_max_mtrc_summ (
    clnt_obj_id STRING,
    yr_cd STRING,
    qtr_cd STRING,
    mnth_cd STRING,
    mngr_pers_obj_id STRING,
    iso_3_char_cntry_cd STRING,
    state_prov_cd STRING,
    mtrc_ky INT,
    ins_rsn STRING,
    grp STRING,
    cnt INT) PARTITIONED BY (environment string, db_schema string);

--ALTER TABLE ${hiveconf:__GREEN_MAIN_DB__}.clnt_min_max_mtrc_summ DROP IF EXISTS PARTITION (environment = '${hiveconf:environment}');

INSERT OVERWRITE TABLE ${hiveconf:__GREEN_MAIN_DB__}.clnt_min_max_mtrc_summ PARTITION(environment, db_schema)
SELECT
  /*+ COALESCE(800) */
  clnt_obj_id,
  yr_cd,
  qtr_cd,
  mnth_cd,
  mngr_pers_obj_id,
  iso_3_char_cntry_cd,
  state_prov_cd,
  mtrc_ky,
  ins_rsn,  
  grp,
  cnt,
  environment,
  db_schema
FROM
(SELECT
    ins.clnt_obj_id,
    ins.yr_cd,
    ins.qtr_cd,
    ins.mnth_cd,
    ins.mngr_pers_obj_id,
    NULL AS iso_3_char_cntry_cd,
    NULL AS state_prov_cd,
    ins.mtrc_ky,
    ins.ins_rsn,
    ins.environment,
    ins.db_schema,
    "dept" as grp,
    count(*) as cnt
FROM
    ${hiveconf:__GREEN_MAIN_DB__}.t_fact_insights ins 
WHERE 
--environment='${hiveconf:environment}' 
hr_orgn_id IS NOT NULL AND job_cd IS NULL AND  iso_3_char_cntry_cd IS NULL AND trmnt_rsn IS NULL
AND ins_rsn in('MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') AND metric_value <> 0
GROUP BY 
clnt_obj_id,
yr_cd,
qtr_cd,
mnth_cd,
mngr_pers_obj_id,
mtrc_ky,
ins_rsn,
environment,
db_schema

UNION ALL

SELECT
    ins.clnt_obj_id,
    ins.yr_cd,
    ins.qtr_cd,
    ins.mnth_cd,
    ins.mngr_pers_obj_id,
    ins.iso_3_char_cntry_cd,
    NULL AS state_prov_cd,
    ins.mtrc_ky,
    ins.ins_rsn,
    ins.environment,
    ins.db_schema,
    "state" as grp,
    count(*) as cnt
FROM
    ${hiveconf:__GREEN_MAIN_DB__}.t_fact_insights ins 
WHERE  
--environment='${hiveconf:environment}' 
iso_3_char_cntry_cd IS NOT NULL  AND state_prov_cd IS NOT NULL AND work_loc_cd IS NULL AND hr_orgn_id IS NULL AND job_cd IS NULL AND trmnt_rsn IS NULL
AND ins_rsn in('MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') AND metric_value <> 0
GROUP BY 
clnt_obj_id,
yr_cd,
qtr_cd,
mnth_cd,
mngr_pers_obj_id,
iso_3_char_cntry_cd,
mtrc_ky,
ins_rsn,
environment,
db_schema

UNION ALL

SELECT
    ins.clnt_obj_id,
    ins.yr_cd,
    ins.qtr_cd,
    ins.mnth_cd,
    ins.mngr_pers_obj_id,
    ins.iso_3_char_cntry_cd,
    ins.state_prov_cd,
    ins.mtrc_ky,
    ins.ins_rsn,
    ins.environment,
    ins.db_schema,
    "loc" as grp,
    count(*) as cnt
FROM
    ${hiveconf:__GREEN_MAIN_DB__}.t_fact_insights ins 
WHERE  
--environment='${hiveconf:environment}' 
iso_3_char_cntry_cd IS NOT NULL  AND state_prov_cd IS NOT NULL AND work_loc_cd IS NOT NULL AND hr_orgn_id IS NULL AND job_cd IS NULL AND trmnt_rsn IS NULL
AND ins_rsn in('MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') AND metric_value <> 0
GROUP BY 
clnt_obj_id,
yr_cd,
qtr_cd,
mnth_cd,
mngr_pers_obj_id,
iso_3_char_cntry_cd,
state_prov_cd,
mtrc_ky,
ins_rsn,
environment,
db_schema

UNION ALL

SELECT
    ins.clnt_obj_id,
    ins.yr_cd,
    ins.qtr_cd,
    ins.mnth_cd,
    ins.mngr_pers_obj_id,
    NULL AS iso_3_char_cntry_cd,
    NULL AS state_prov_cd,
    ins.mtrc_ky,
    ins.ins_rsn,
    ins.environment,
    ins.db_schema,
    "job" as grp,
    count(*) as cnt
FROM
    ${hiveconf:__GREEN_MAIN_DB__}.t_fact_insights ins 
WHERE  
--environment='${hiveconf:environment}' 
job_cd IS NOT NULL AND hr_orgn_id IS NULL AND iso_3_char_cntry_cd IS NULL AND trmnt_rsn IS NULL
AND ins_rsn in('MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') AND metric_value <> 0
GROUP BY 
clnt_obj_id,
yr_cd,
qtr_cd,
mnth_cd,
mngr_pers_obj_id,
mtrc_ky,
ins_rsn,
environment,
db_schema

UNION ALL

SELECT
    ins.clnt_obj_id,
    ins.yr_cd,
    ins.qtr_cd,
    ins.mnth_cd,
    ins.mngr_pers_obj_id,
    NULL AS iso_3_char_cntry_cd,
    NULL AS state_prov_cd,
    ins.mtrc_ky,
    ins.ins_rsn,
    ins.environment,
    ins.db_schema,
    "trmn" as grp,
    count(*) as cnt
FROM
    ${hiveconf:__GREEN_MAIN_DB__}.t_fact_insights ins 
WHERE  
--environment='${hiveconf:environment}'
trmnt_rsn IS NOT NULL AND job_cd IS NULL AND hr_orgn_id IS NULL AND iso_3_char_cntry_cd IS NULL AND 
ins_rsn in('MIN_PERCENTILE_RANKING','MAX_PERCENTILE_RANKING') AND metric_value <> 0
GROUP BY 
clnt_obj_id,
yr_cd,
qtr_cd,
mnth_cd,
mngr_pers_obj_id,
mtrc_ky,
ins_rsn,
environment,
db_schema)final