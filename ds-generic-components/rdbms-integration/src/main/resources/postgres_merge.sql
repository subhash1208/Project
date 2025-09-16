do
LANGUAGE plpgsql
$$
BEGIN
    -- Process deletions
    DELETE FROM {{tableName}}
    WHERE ({{pkColumnsList}}) IN (SELECT {{pkColumnsList}} FROM {{tableName}}_DELETES); 

    -- Process Merges
    INSERT INTO {{tableName}}
    SELECT * FROM {{tableName}}_UPSERTS
    ON CONFLICT ({{pkColumnsList}})
    DO UPDATE 
        SET 
        {{columnAssignments}};

    COMMIT;

    -- Gather Stats
    EXECUTE 'ANALYZE {{tableName}}';

    -- Drop the temporary tables
    DROP TABLE {{tableName}}_DELETES;
    DROP TABLE {{tableName}}_UPSERTS;

    COMMIT;
END;
$$ 