SELECT 
    ins_hash_val,count(*) 
  FROM 
    ${hiveconf:__GREEN_MAIN_DB__}.t_fact_insights
  --WHERE
  --  environment = '${hiveconf:ENVIRONMENT}'
  GROUP BY ins_hash_val HAVING count(*)>1