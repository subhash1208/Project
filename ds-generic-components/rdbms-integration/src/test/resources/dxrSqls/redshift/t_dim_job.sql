SELECT
    clnt_obj_id,
    job_cd,
    job_dsc,
    '[DBSCHEMA]' AS db_schema
FROM
    [OWNERID].t_dim_job