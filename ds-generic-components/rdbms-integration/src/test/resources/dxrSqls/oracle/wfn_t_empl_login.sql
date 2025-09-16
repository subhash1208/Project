SELECT
    clnt_obj_id as clnt_obj_id_hash,
    trunc(rpt_dt,'MM') rpt_dt,
    pers_obj_id as pers_obj_id_hash,
    lst_login_ts,
    rec_crt_ts,
    rec_lst_updt_ts,
    to_char(rpt_dt,'yyyyMM') AS yyyymm,
    '[DBSCHEMA]' AS db_schema
FROM
    [OWNERID].v_empl_login
WHERE
    rpt_dt > to_timestamp('[INCREMENTALSTARTDATE]','YYYY-MM-DD HH24:MI:SS.ff')
    AND   rpt_dt <= to_timestamp('[INCREMENTALENDDATE]','YYYY-MM-DD HH24:MI:SS.ff')