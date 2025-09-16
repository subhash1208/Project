#Basically we may also not get file if there is a connection time out, so in that case also we don't want to fail the script but exit safely.

#Get the splunk user password
splunk_pwd=`aws secretsmanager get-secret-value --secret-id SPLUNKDCUserPassword|jq '.SecretString'|tr -d '"'`

curl -k -u splunkdatacloud:$splunk_pwd https://dc1prsplunkapi.whc.dc01.us.adp/servicesNS/splunkdatacloud/adpanalytics_search/search/jobs/export --data-urlencode search='search index=datacloud_perf sourcetype=adpa_request_perf ("URI=*metrics*meta*itemid*" OR URI="*metrics*meta*itemid*") UAMODE=N
| eval URI=mvindex(split(URI," "),0)
| eval URI=urldecode(URI)
| eval params=split(URI,"/")
| eval metric =mvindex(params,5)
| eval metric =split(URI," ")
| eval METRIC_KEY =mvindex(metric,2)
| eval INSIGHT=mvindex(metric,6)
| table ORGID AOID ROLE USERTYPE IS_EXECUTIVE METRIC_KEY INSIGHT MOBILE_CLNT CLNT_LIVE_IND SRCSYSTEM URI _time' -d output_mode=csv -d earliest_time=-90d@d -d latest_time=-0d@d --connect-timeout 60 -o /tmp/emi_prep_recom_metric_view.csv

status=$?

if [ $status -eq 0 ]; then
    #If the credentials wrong still we will have file generated but with ERROR message inside it
    cat /tmp/emi_prep_recom_metric_view.csv | grep "ERROR"
    status=$?
    if [ $status -ne 0 ]; then
       sed -i 1d /tmp/emi_prep_recom_metric_view.csv
       aws s3 mv /tmp/emi_prep_recom_metric_view.csv s3://${hiveconf:__GREEN_MAIN_S3_BUCKET__}/emi_prep_recom_metric_view/
    else
       exit 0
    fi
else
    exit 0
fi