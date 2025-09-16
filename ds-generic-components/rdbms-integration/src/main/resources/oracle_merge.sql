BEGIN
    TRUNCATE TABLE {{tableName}};
    INSERT INTO {{tableName}}
    SELECT * FROM {{tableName}}_UPSERTS;
	COMMIT;
END;
/ 