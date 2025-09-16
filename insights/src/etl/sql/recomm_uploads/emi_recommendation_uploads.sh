#Uploading emi_recommender_dataset dataset
hive -e 'set hive.cli.print.header=true; set hive.resultset.use.unique.column.names=false; SET hive.support.quoted.identifiers=NONE; select `(db_schema)?+.+` from ${hiveconf:__GREEN_MAIN_DB__}.emi_recommender_base_dataset' | sed 's/[\t]/|/g'  > emi_recommender_dataset.csv
gzip -f emi_recommender_dataset.csv
curl -u ds_user:AP9tPQxTrhZcxaRWLjV9BgrwLwV -T emi_recommender_dataset.csv.gz "https://artifactory.us.caas.oneadp.com/artifactory/datacloud-datascience-generic-local/dc-recommender/7.0/"

#Uploading multi_arm_stats dataset
hive -e 'set hive.cli.print.header=true; set hive.resultset.use.unique.column.names=false;
SELECT 
    bandit_arm,
    clicks_cnt,
    non_clicks_cnt,
    learning_rt,
    decay_clicks_rt,
    decay_non_clicks_rt
  FROM 
    ${hiveconf:__GREEN_MAIN_DB__}.t_dim_ins_arm_stats' | sed 's/[\t]/,/g' > multi_arm_stats.csv
zip multi_arm_stats.zip multi_arm_stats.csv
curl -u ds_user:AP9tPQxTrhZcxaRWLjV9BgrwLwV -T multi_arm_stats.zip "https://artifactory.us.caas.oneadp.com/artifactory/datacloud-datascience-generic-local/dc-recommender/7.0/"