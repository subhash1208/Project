# Databricks notebook source
import os

# COMMAND ----------

#recommender basedataset upload to artifactory URL 

# COMMAND ----------

#initialize variables
recommbase_file_path = "/tmp/emi_recommender_dataset.csv"
recommbase_zipfile_path = "/tmp/emi_recommender_dataset.csv.gz"
os.environ['recommbase_file_path'] = recommbase_file_path
os.environ['recommbase_zipfile_path'] = recommbase_zipfile_path
db_schema = dbutils.widgets.get('green-main-db')
print("DB Schema:", db_schema)

# COMMAND ----------

base_databsettbl  = f"{db_schema}.emi_recommender_base_dataset"
base_df = spark.table(base_databsettbl)
filtered_base_df = base_df.select(base_df.colRegex("`(db_schema)?+.+`"))

# COMMAND ----------

filtered_base_pddf = filtered_base_df.toPandas()

# COMMAND ----------

#display couple of records for debug
display(filtered_base_df.limit(2))

# COMMAND ----------

#display couple of records for debug
filtered_base_pddf.head(2)

# COMMAND ----------

#write the pd basedata set to file
filtered_base_pddf.to_csv(recommbase_file_path, sep = "\t", index=False)

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC head -2 $recommbase_file_path

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC #list the file to display if the file has been created from pandas dataframe or not and compress 
# MAGIC 
# MAGIC ls -ltr $recommbase_file_path
# MAGIC 
# MAGIC sed -i 's/[\t]/|/g' $recommbase_file_path

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC # check on the created file contennt - display couple of records for debug 
# MAGIC 
# MAGIC head -2 $recommbase_file_path

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC #upload the zipped file to the artifactory location 
# MAGIC 
# MAGIC ls -ltr /tmp/emi*
# MAGIC 
# MAGIC gzip -f $recommbase_file_path
# MAGIC 
# MAGIC curl -u ds_user1:AP6TMbqHECaqWRjZfXuhroeKYXF -T $recommbase_zipfile_path "https://artifactory.us.caas.oneadp.com/artifactory/datacloud-datascience-generic-local/dc-recommender/7.0/"
# MAGIC 
# MAGIC rm $recommbase_zipfile_path
# MAGIC 
# MAGIC echo "Base dataset upload to artifactory completed"

# COMMAND ----------

# Multi arm data set export 

# COMMAND ----------

multi_file_path = "/tmp/multi_arm_stats.csv"
multi_arm_zipfile = "/tmp/multi_arm_stats.zip"
os.environ['multi_file_path'] = multi_file_path
os.environ['multi_arm_zipfile'] = multi_arm_zipfile
# db_schema = "datacloud_nonprod_dit_main"


# COMMAND ----------

multiarm_sql = f"""
                        SELECT 
                        bandit_arm,
                        clicks_cnt,
                        non_clicks_cnt,
                        learning_rt,
                        decay_clicks_rt,
                        decay_non_clicks_rt
                        FROM {db_schema}.t_dim_ins_arm_stats 
                  """ 

multiarm_df = spark.sql(multiarm_sql)

# COMMAND ----------

display(multiarm_df.limit(10))

# COMMAND ----------

#convert this to spark dataframe to pandas dataframe to write to csv file temporarily 

multiarm_pd_df = multiarm_df.toPandas()
multiarm_pd_df.to_csv(multi_file_path, index=False)

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC head $multi_file_path

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC sed -i 's/[\t]/,/g' $multi_file_path
# MAGIC zip  $multi_arm_zipfile $multi_file_path
# MAGIC 
# MAGIC ls -ltr $multi_arm_zipfile
# MAGIC 
# MAGIC echo "file zipped and ready to upload"
# MAGIC 
# MAGIC curl -k -u ds_user1:AP6TMbqHECaqWRjZfXuhroeKYXF -T $multi_arm_zipfile "https://artifactory.us.caas.oneadp.com/artifactory/datacloud-datascience-generic-local/dc-recommender/7.0/"
# MAGIC 
# MAGIC rm $multi_arm_zipfile $multi_file_path
# MAGIC echo "temporary files deleted from folder"

# COMMAND ----------

# dbutils.widgets.removeAll()
# dbutils.widgets.text("green-main-db", "datacloud_nonprod_dit_main")
