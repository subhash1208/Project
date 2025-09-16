-- This SQL contains the query to be fired to ensure both the source and target tables are in sync with each other
-- The "sync state hash" is computed only using the primary keys of the table and doesnt capture state of all fields. This may change in future  
SELECT
	'[DBSCHEMA]' AS db_schema,
	md5(
		CAST(
			array_agg(f.* order by {{pkColumnsList}}) AS text
		)
	) as sync_state_hash
FROM
	(SELECT {{pkColumnsList}} FROM {{tableName}}) f