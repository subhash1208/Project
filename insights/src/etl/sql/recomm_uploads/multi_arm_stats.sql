set hive.resultset.use.unique.column.names=false;
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.multi_arm_stats;
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.multi_arm_stats
USING PARQUET
AS
SELECT 
    bandit_arm,
    clicks_cnt,
    non_clicks_cnt,
    learning_rt,
    decay_clicks_rt,
    decay_non_clicks_rt
  FROM 
    ${__BLUE_MAIN_DB__}.t_dim_ins_arm_stats
  --WHERE
  --  environment = '${environment}';