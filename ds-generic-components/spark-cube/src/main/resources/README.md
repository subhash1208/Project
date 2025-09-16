# Spark-Cube T\_DIM\_DAY\_ROLLUP


### Parquet file generation steps

This parquet file needs to be generated periodically for the time-being. The current one is updated till 2021. 


```
# Execute the t_dim_day_rollup.sql in a warehouse DB and export the result to a csv.

beeline -e "create table ditdsraw.tmp_t_dim_day_rollup (yr_cd string, qtr_cd string, mnth_cd string, wk_cd string, sys_calendar_days int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'"

hdfs dfs -copyFromLocal t_dim_day_rollup.tsv /dit/ads/ditdsraw.db/tmp_t_dim_day_rollup/

sqlContext.sql("Set spark.sql.shuffle.partitions=1")
sqlContext.sql("create table ditdsraw.t_dim_day_rollup stored as parquet as select * from ditdsraw.tmp_t_dim_day_rollup distribute by 1")

# Copy the parquet file down into src/main/resources folder in spark-cube project
```

**TODO**: Spark-cube functionality to be modified to default the time rollup table to **\_\_GREEN\_RAW\_DB\_\_.dwh\_t\_dim\_day\_rollup** table.
