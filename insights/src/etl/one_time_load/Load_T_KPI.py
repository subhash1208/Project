# Databricks notebook source
# Databricks notebook source
# Import required libraries
import pandas as pd


# COMMAND ----------

BLUE_MAIN_ID = dbutils.widgets.get("__BLUE_MAIN_DB__")
env = dbutils.widgets.get("env")

# COMMAND ----------

file_location = '/dbfs/FileStore/_t_kpi__202211021743.csv'
df_pd = pd.read_csv(file_location)
sparkDF=spark.createDataFrame(df_pd) 
sparkDF.createOrReplaceGlobalTempView('t_kpi')
spark.sql(f"select count(0) from global_temp.t_kpi")
spark.sql(f"drop table if exists {BLUE_MAIN_ID}.insights_t_kpi")
spark.sql(f"create table if not exists {BLUE_MAIN_ID}.insights_t_kpi STORED AS parquet as select * from global_temp.t_kpi where 1=2")
spark.sql(f"insert overwrite table {BLUE_MAIN_ID}.insights_t_kpi select * from global_temp.t_kpi where 1=2")
spark.sql(f"select count(0) from {BLUE_MAIN_ID}.insights_t_kpi")