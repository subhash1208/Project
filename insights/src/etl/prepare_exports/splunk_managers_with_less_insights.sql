SELECT
    CONCAT(
        'ts=\"', SUBSTR(CAST(rec_crt_ts AS STRING),1,10), '\"  ',
        'e=\"', environment, '\"  ',
        'ds=\"', db_schema, '\"  ',
        'ooid=\"', clnt_obj_id, '\"  ',
        'mgr=\"', mngr_pers_obj_id, '\"  ',
        'app_rept=\"', approx_reportees, '\"  ',
        'num_ins=\"', CAST(num_insights AS STRING), '\"  '
    ) as log_message
FROM
    ${hiveconf:__GREEN_MAIN_DB__}.emi_exp_mngrs_with_less_insights;
--WHERE
--    environment = '${hiveconf:ENVIRONMENT}'