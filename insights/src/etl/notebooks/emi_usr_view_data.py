# Databricks notebook source
# Databricks notebook source
import boto3
import base64
import os
import botocore
from botocore.exceptions import ClientError
import botocore.session 
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

# COMMAND ----------

import botocore 

try:    
    client = botocore.session.get_session().create_client('secretsmanager')
    cache_config = SecretCacheConfig()
    cache = SecretCache( config = cache_config, client = client)
    secret = cache.get_secret_string('SPLUNKDCUserPassword')
except ClientError as e:
    if e.response['Error']['Code'] == 'DecryptionFailureException':
        print("DecryptionFailureException Excetion",str(e))
    elif e.response['Error']['Code'] == 'InternalServiceErrorException':
        print("EInternalServiceErrorException Excetion",str(e))
    elif e.response['Error']['Code'] == 'InvalidParameterException':
        print("InvalidParameterException",str(e))
    elif e.response['Error']['Code'] == 'InvalidRequestException':
        print("InvalidRequestException",str(e))
    elif e.response['Error']['Code'] == 'ResourceNotFoundException':
        print("ResourceNotFoundException",str(e))
    elif e.response['Error']['Code'] == 'NoCredentialsError':
        print("NoCredentialsError",str(e))
    else:
        print("Other Excetion",str(e))

# COMMAND ----------

account_id = boto3.client("sts").get_caller_identity()["Account"]
print(account_id)

# COMMAND ----------

# # get the secret key from aws secret store 
# def get_secret(secret_name, region_name):
#     # Create a Secrets Manager client
#     session = boto3.session.Session()
#     client = session.client(
#                              service_name='secretsmanager',
#                              region_name=region_name
#                             )

#     try:
#         get_secret_value_response = client.get_secret_value(SecretId=secret_name)
#         #print("Test Message", get_secret_value_response['SecretString'])
#     except ClientError as e:
#         if e.response['Error']['Code'] == 'DecryptionFailureException':
#             print("DecryptionFailureException Excetion",str(e))
#         elif e.response['Error']['Code'] == 'InternalServiceErrorException':
#             print("EInternalServiceErrorException Excetion",str(e))
#         elif e.response['Error']['Code'] == 'InvalidParameterException':
#             print("InvalidParameterException",str(e))
#         elif e.response['Error']['Code'] == 'InvalidRequestException':
#             print("InvalidRequestException",str(e))
#         elif e.response['Error']['Code'] == 'ResourceNotFoundException':
#             print("ResourceNotFoundException",str(e))
#         elif e.response['Error']['Code'] == 'NoCredentialsError':
#             print("NoCredentialsError",str(e))
#         else:
#            print("Other Excetion",str(e))
#     else:
#         # Decrypts secret using the associated KMS key.
#         # Depending on whether the secret is a string or binary, one of these fields will be populated.
#         if 'SecretString' in get_secret_value_response:
#             secret = get_secret_value_response['SecretString']
#             return(secret)
#         else:
#             decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
#             return(decoded_binary_secret)

# COMMAND ----------

if __name__ == '__main__':       
    region_name = "us-east-1"
    os.environ['splunk_api_key'] = secret
    blue_main_s3_bucket = dbutils.widgets.get('blue_main_s3_bucket')
    blue_main_db = dbutils.widgets.get('__BLUE_MAIN_DB__')
    s3_source_table_folder="emi_usr_view_data/"
    s3_drop_filename = "emi_usr_view_data.csv"
    s3_abs_filepath = s3_source_table_folder + s3_drop_filename
    temp_splunk_absfilepath = '/tmp/emi_usr_view_data.csv' 
    os.environ['temp_splunk_absfilepath'] = temp_splunk_absfilepath
    print(blue_main_s3_bucket, s3_abs_filepath, os.environ['temp_splunk_absfilepath'])

# COMMAND ----------

# MAGIC %md
# MAGIC Splunk query to fetch the click data(Implicit Feedback) for User metrics for the last 90 days:
# MAGIC 
# MAGIC ```earliest=-90d@d & latest=now index=adpanalytics_main  URI="*/read-by-filter-request*" CLNT_LIVE_IND=Y UAMODE=N | stats dc(TRANSACTIONID) by ORGID,AOID,URI,KPIID,KPINAME | rename dc(TRANSACTIONID) as views_cnt```

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC echo $temp_splunk_absfilepath
# MAGIC 
# MAGIC curl -k -H "Authorization: Bearer $splunk_api_key" https://splunkprodapi.oneadp.com:443/servicesNS/splunkdatacloud/adpanalytics_search/search/jobs/export --data-urlencode search='search earliest=-90d@d & latest=now index=adpanalytics_main  URI="*/read-by-filter-request*" CLNT_LIVE_IND=Y UAMODE=N | stats dc(TRANSACTIONID) by ORGID,AOID,URI,KPIID,KPINAME | rename dc(TRANSACTIONID) as views_cnt' -d output_mode=csv  --connect-timeout 600 -o $temp_splunk_absfilepath
# MAGIC 
# MAGIC status=$?
# MAGIC 
# MAGIC if [ $status -eq 0 ]; then
# MAGIC     #If the credentials wrong still we will have file generated but with ERROR message inside it
# MAGIC     cat $temp_splunk_absfilepath | grep "ERROR"
# MAGIC     status=$?
# MAGIC     if [ $status -ne 0 ]; then
# MAGIC        sed -i 1d $temp_splunk_absfilepath
# MAGIC        echo "Splunk extracted file saved without header in $temp_splunk_absfilepath"
# MAGIC     else
# MAGIC        echo "Splunk file extracted but contains errors"
# MAGIC        exit 1
# MAGIC     fi
# MAGIC else
# MAGIC     echo "Error in extraction of the splunk file"
# MAGIC     exit 1
# MAGIC fi

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -l $temp_splunk_absfilepath

# COMMAND ----------

try:  
    client   = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    if os.path.exists(temp_splunk_absfilepath):
        client.upload_file(temp_splunk_absfilepath, blue_main_s3_bucket, s3_abs_filepath)
    else:
        print(f"Temporary file {temp_splunk_absfilepath}  does not exist to upload! - check the file S3 or re-run the workflow step")

except botocore.exceptions.ClientError as err:
    print(f"Error in S3 Upload , S3 bucket {blue_main_s3_bucket} & file {s3_abs_filepath}")
    raise err
else:
    print(f"File successfully transferred to S3 bucket {blue_main_s3_bucket} & file {s3_abs_filepath} ")
    os.remove(temp_splunk_absfilepath)
    print(f"Temporary file {temp_splunk_absfilepath} has been deleted successfully. Check the status of this in S3")

# COMMAND ----------

# temp_splunk_absfilepath = '/tmp/emi_prep_recom_metric_view.csv' 

# if os.path.exists(temp_splunk_absfilepath):
#   print("temp file exists --- ")
# #     client.upload_file(temp_splunk_absfilepath, blue_main_s3_bucket, s3_abs_filepath)
# else:
#     print(f"Temporary file {temp_splunk_absfilepath}  does not exist to upload! - check the file S3 or re-run the workflow step")

# COMMAND ----------

# Read the click data from s3 as a csv and convert it into pandas dataframe to add required column headers.

splunk_sdf=spark.read.csv("s3://{blue_main_s3_bucket}/emi_usr_view_data/emi_usr_view_data.csv".format(blue_main_s3_bucket=blue_main_s3_bucket))
splunk_df = splunk_sdf.toPandas()
splunk_df.columns = ['ORGID','AOID','URI','KPIID','KPINAME','views_cnt']
splunk_df.head()

# COMMAND ----------

# Save the User click data as as a parquet so that it can be read as a table from blue_main_db.

user_click_data_sdf = spark.createDataFrame(splunk_df)
user_click_data_sdf.write. \
    format('parquet'). \
    mode('overwrite'). \
    option('path', f's3://{blue_main_s3_bucket}/emi_usr_view_data'). \
    saveAsTable(f'{blue_main_db}.emi_usr_view_data')

# COMMAND ----------

# %sql 
# CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_usr_view_data (
#   ORGID string,
#   AOID string,
#   URI string,
#   KPIID int,
#   KPINAME string,
#   views_cnt int
# ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"") LOCATION 's3://${blue_main_s3_bucket}/emi_usr_view_data';
