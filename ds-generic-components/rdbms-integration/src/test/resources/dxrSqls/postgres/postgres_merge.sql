do
LANGUAGE plpgsql
$$
BEGIN
	-- Process deletions
    DELETE {{tableName}}
    WHERE ({{pkColumnsList}}) IN (SELECT {{pkColumnsList}} FROM {{tableName}}_DELETES); 

    -- Process Merges
    INSERT INTO {{tableName}}
    SELECT * FROM {{tableName}}_UPSERTS
    ON CONFLICT ON ({{pkColumnsList}})
    DO UPDATE 
        SET 
        {{columnAssignments}};

    COMMIT;

    DROP TABLE {{tableName}}_DELETES;
    DROP TABLE {{tableName}}_UPSERTS;

    COMMIT;
END;
$$ 