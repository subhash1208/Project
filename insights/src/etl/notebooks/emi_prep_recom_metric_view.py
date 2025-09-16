# Databricks notebook source
import boto3
import base64
import os
import botocore
from botocore.exceptions import ClientError

# COMMAND ----------

# get the secret key from aws secret store 
def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
                             service_name='secretsmanager',
                             region_name=region_name
                            )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        #print("Test Message", get_secret_value_response['SecretString'])
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
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return(secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return(decoded_binary_secret)

# COMMAND ----------

if __name__ == '__main__':       
    secret_name = "arn:aws:secretsmanager:us-east-1:708035784431:secret:SPLUNKDCUserPassword-E7AcEq"
    region_name = "us-east-1"
    os.environ['splunk_api_key'] = get_secret(secret_name,region_name)
    s3_bucket = dbutils.widgets.get('green-main-s3-bucket')
    s3_source_table_folder="emi_prep_recom_metric_view/"
    s3_drop_filename = "emi_prep_recom_metric_view.csv"
    s3_abs_filepath = s3_source_table_folder + s3_drop_filename
    temp_splunk_absfilepath = '/tmp/emi_prep_recom_metric_view.csv' 
    os.environ['temp_splunk_absfilepath'] = temp_splunk_absfilepath
    print(s3_bucket, s3_abs_filepath, os.environ['temp_splunk_absfilepath'])

# COMMAND ----------

# MAGIC %sh 
# MAGIC 
# MAGIC echo $temp_splunk_absfilepath
# MAGIC 
# MAGIC curl -k -H "Authorization: Bearer $splunk_api_key" https://dc1prsplunkapi.whc.dc01.us.adp/servicesNS/splunkdatacloud/adpanalytics_search/search/jobs/export --data-urlencode search='search index=datacloud_perf sourcetype=adpa_request_perf ("URI=*metrics*meta*itemid*" OR URI="*metrics*meta*itemid*") UAMODE=N
# MAGIC | eval URI=mvindex(split(URI," "),0)
# MAGIC | eval URI=urldecode(URI)
# MAGIC | eval params=split(URI,"/")
# MAGIC | eval metric =mvindex(params,5)
# MAGIC | eval metric =split(URI," ")
# MAGIC | eval METRIC_KEY =mvindex(metric,2)
# MAGIC | eval INSIGHT=mvindex(metric,6)
# MAGIC | table ORGID AOID ROLE USERTYPE IS_EXECUTIVE METRIC_KEY INSIGHT MOBILE_CLNT CLNT_LIVE_IND SRCSYSTEM URI _time' -d output_mode=csv -d earliest_time=-90d@d -d latest_time=-0d@d --connect-timeout 600 -o $temp_splunk_absfilepath
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
    client.upload_file(temp_splunk_absfilepath, s3_bucket, s3_abs_filepath)
  else:
    print(f"Temporary file {temp_splunk_absfilepath}  does not exist to upload! - check the file S3 or re-run the workflow step")
  
except botocore.exceptions.ClientError as err:
   print(f"Error in S3 Upload , S3 bucket {s3_bucket} & file {s3_abs_filepath}")
   raise err
else:
  print(f"File successfully transferred to S3 bucket {s3_bucket} & file {s3_abs_filepath} ")
  os.remove(temp_splunk_absfilepath)
  print(f"Temporary file {temp_splunk_absfilepath} has been deleted successfully. Check the status of this in S3")

# COMMAND ----------

# temp_splunk_absfilepath = '/tmp/emi_prep_recom_metric_view.csv' 

# if os.path.exists(temp_splunk_absfilepath):
#   print("temp file exists --- ")
# #     client.upload_file(temp_splunk_absfilepath, s3_bucket, s3_abs_filepath)
# else:
#     print(f"Temporary file {temp_splunk_absfilepath}  does not exist to upload! - check the file S3 or re-run the workflow step")
