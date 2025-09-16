
# Incremental Export Dataset Generation

Example using T_DIM_ORGN as the table

----------------------------------------------
| Lifespan | Database | Table Name | Purpose |
|----------|----------|------------|---------|
| Persistent | Postgre  | T_DIM_ORGN | Warehouse Main table |
| Temporary | Postgre  | T_DIM_ORGN_DELETES | Staging table for deletes before loading to main |
| Temporary | Postgre  | T_DIM_ORGN_UPSERTS | Staging table for upserts before loading to main |
| Persistent | Lake | T_DIM_ORGN | Same table as Postgre. Always holds the same data as target | 
| Persistent | Lake | T_DIM_ORGN_FULL | Regenerated every workflow execution. Contains full dataset |
| Temporary | Lake | T_DIM_ORGN_OLD | Temporary table to maintain last known copy of T_DIM_ORGN |
| Temporary | Lake | T_DIM_ORGN_DELETES | Computed using records present in T_DIM_ORGN_OLD, but no longer present in T_DIM_ORGN_FULL, after comparison using PK columns |
| Temporary | Lake | T_DIM_ORGN_UPSERTS | Computed using records which have changed in T_DIM_ORGN_FULL as compared to T_DIM_ORGN_OLD, based on all columns.


- XML Config
    - Contains list of Configurations
    - Hive Table Name + Target RDBMS Table Name + Unique Key + (optional exclusions)
        - Hive Table Name               = T_DIM_ORGN_FULL  (full dataset)
        - Target Postgres Table Name    = T_DIM_ORGN

- Driver
    - For each configuration, in the XML Config above
        - Check if T_DIM_ORGN table exists in the lake. If not, create the table with same columns as T_DIM_ORGN_FULL, but no data. (First time load)
        - Check if columns in T_DIM_ORGN table match exactly with columns in T_DIM_ORGN_FULL.
            - If columns are not matching, then drop T_DIM_ORGN and recreate it with same columns as T_DIM_ORGN_FULL. Effectively forces a full load.
        - Generate the list of columns to use for join, using the below logic
            - (All columns of T_DIM_ORGN_FULL) MINUS (exclusions specified in config)
        - Generate the join condition using a foldLeft operation
            - equality of column in old vs new aliases.
        - Compute Delta
          - DROP IF EXISTS T_DIM_ORGN_OLD, T_DIM_ORGN_UPSERTS, T_DIM_ORGN_DELETES.
          - Create a table T_DIM_ORGN_OLD with same data as T_DIM_ORGN.
          - Create 2 tables T_DIM_ORGN_UPSERTS and T_DIM_ORGN_DELETES containing differences between T_DIM_ORGN_OLD and T_DIM_ORGN_FULL.
        - Export deleta datasets UPSERTS and DELETES to Postgres and merge in the target DB. (PL/SQL)
        - INSERT OVERWRITE T_DIM_ORGN using data from T_DIM_ORGN_FULL. (for subsequent runs to work)
        - DROP temporary TABLEs T_DIM_ORGN_OLD, T_DIM_ORGN_UPSERTS, T_DIM_ORGN_DELETES.


val target_columns = set(spark.sql("select * from T_DIM_ORGN").columns)
val lake_columns = set(spark.sql("select * from T_DIM_ORGN").columns)

