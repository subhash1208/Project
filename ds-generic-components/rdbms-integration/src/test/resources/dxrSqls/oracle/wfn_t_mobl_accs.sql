SELECT
    clnt_obj_id AS clnt_obj_id_hash,
    pers_obj_id AS pers_obj_id_hash,
    empl_cnt,
    rec_crt_ts,
    trunc(rpt_dt,'MM') rpt_dt,
    TO_CHAR(rpt_dt,'yyyyMM') AS yyyymm,
    '[DBSCHEMA]' AS db_schema
FROM
   [OWNERID].v_mobl_accs
WHERE
    rpt_dt > to_timestamp('[INCREMENTALSTARTDATE]','YYYY-MM-DD HH24:MI:SS.ff')
    AND   rpt_dt <= to_timestamp('[INCREMENTALENDDATE]','YYYY-MM-DD HH24:MI:SS.ff')