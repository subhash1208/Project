SELECT 
    CONCAT( 
        'ts="', SUBSTR(CAST(rec_crt_ts AS STRING),1,10), '"  ', 
        'e="', environment, '"  ', 
        'ds="', db_schema, '"  ', 
        'ooid="', clnt_obj_id, '"  ', 
        'mn="', metric_name, '"  ', 
        'tp="', CASE WHEN time_period = 'YEARLY' THEN 'YRLY' WHEN time_period = 'MONTHLY' THEN 'MNTHLY' WHEN time_period = 'QUARTERLY' THEN 'QTRLY' ELSE 'WKLY' END , '"  ', 
        'd="', dimension, '"  ', 
        'it="', CASE WHEN ins_type = 'CLIENT_INTERNAL' THEN 'INTRNL' WHEN ins_type = 'CLIENT_VS_BM' THEN 'EXT_BM' ELSE 'INT_BM' END , '"  ', 
        'ir="', CASE WHEN (ins_rsn = 'ABS_DIFF' OR ins_rsn = 'PCTG_DIFF') THEN 'DIFF' WHEN (ins_rsn='ABS_DIFF_TIME' OR ins_rsn='PCTG_DIFF_TIME') THEN 'DIFF_TME' ELSE ins_rsn END , '"  ', 
        'p="', CASE WHEN persona = 'MANAGER' THEN 'MNG' ELSE 'EXEC' END , '"  ', 
        'n="', CAST(num_insights AS STRING), '"  ', 
        'm="', CAST(num_managers AS STRING), '"  '  
    ) as log_message 
  FROM 
    ${hiveconf:__GREEN_MAIN_DB__}.emi_exp_insights_summary
  WHERE
    --environment = '${hiveconf:ENVIRONMENT}' 
    ins_rsn != 'PERCENTILE_RANKING'