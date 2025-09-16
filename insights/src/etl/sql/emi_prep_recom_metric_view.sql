-- Databricks notebook source
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.emi_prep_recom_metric_view
(
 ORGID string,
 AOID  string,
 ROLE  string,
 USERTYPE int,
 IS_EXECUTIVE string,
 METRIC_KY int,
 INS_HASH_VAL string,
 MOBILE_CNT string,
 CLNT_LIVE_IND string,
 SOURCE  string,
 URI string,
 EVENT_TIME string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
LOCATION 's3://${__BLUE_MAIN_S3_BUCKET__}/emi_prep_recom_metric_view';