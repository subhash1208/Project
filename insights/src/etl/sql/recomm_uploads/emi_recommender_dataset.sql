set hive.resultset.use.unique.column.names=false;
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_recommender_dataset;
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_recommender_dataset
USING PARQUET
AS
SELECT 
    *
  FROM 
    ${__BLUE_MAIN_DB__}.emi_recommender_base_dataset
  --WHERE
  --  environment = '${environment}';