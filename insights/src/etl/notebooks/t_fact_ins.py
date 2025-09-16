# Databricks notebook source
# Databricks notebook source
GREEN_MAIN_DB = dbutils.widgets.get("GREEN_MAIN_DB")
BLUE_MAIN_DB = dbutils.widgets.get("BLUE_MAIN_DB")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

'''Extracting data from T_FACT_INS_STAGING
For each INS_HASH_VAL with a valid INS_ML_SCOR would replace the INS_SCOR extracted from T_FACT_INS_STAGING
The final result is stored in table T_FACT_INS table.'''

t_fact_ins_staging_df = spark.table(f'{BLUE_MAIN_DB}.T_FACT_INS_STAGING')
emi_ml_inferences_df = spark.table(f'{BLUE_MAIN_DB}.EMI_ML_INFERENCES')
t_fact_ins_join = t_fact_ins_staging_df.join(emi_ml_inferences_df,t_fact_ins_staging_df.ins_hash_val==emi_ml_inferences_df.ins_hash_val,'left') \
                                .select(f"spark_catalog.{BLUE_MAIN_DB}.t_fact_ins_staging.*",f"spark_catalog.{BLUE_MAIN_DB}.emi_ml_inferences.ins_ml_score")

#When INS_ML_SCOR is not null, pass INS_ML_SCOR, else INS_SCOR
t_fact_ins = t_fact_ins_join.withColumn("ins_scor",when(col('ins_ml_score').isNull(),col('ins_scor')).otherwise(col('ins_ml_score'))).drop(col('ins_ml_score'))
t_fact_ins.write.saveAsTable(name=f'{BLUE_MAIN_DB}.t_fact_ins', format='parquet', mode='overwrite')

