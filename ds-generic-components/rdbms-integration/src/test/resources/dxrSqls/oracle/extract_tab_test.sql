SELECT
    tname,
    tabtype,
    clusterid,
    'STRICT_HASH_ME' AS test_stricthash,
    'JUST_HASH_ME' AS test_hash,
    '[DBSCHEMA]' AS db_schema,
    SYSDATE AS sys_date
FROM
    tab