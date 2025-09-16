SELECT
    ooid AS clnt_obj_id,
    aoid AS pers_obj_id,
    COUNT(1) AS empl_cnt,
    current_date() AS rec_lst_updt_ts,
    current_date() AS rpt_dt,
    date_format(current_date(),'yyyyMM') AS yyyymm,
    '[DBSCHEMA]' AS db_schema
FROM
   {noSqlTable:userAccount}
GROUP BY
    aoid,
    ooid