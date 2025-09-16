SELECT
    upper(sys_context('USERENV','DB_NAME') ) AS db_name,
    '[DBSCHEMA]' AS db_schema,
    'STRICT_HASH_ME' AS test_stricthash,
    'JUST_HASH_ME' AS test_hash,
    SYSDATE AS sys_date
FROM
    dual