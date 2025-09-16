-- Databricks notebook source
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.t_ins_splunk_hist;

CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.t_ins_splunk_hist (
    clnt_obj_id STRING,
    pers_obj_id STRING,
    mtrc_ky INT,
    mtrc_hits_cnt INT,
    rec_crt_ts DATE,
    db_schema STRING,
    environment string
) USING PARQUET 
PARTITIONED BY (environment)
TBLPROPERTIES ('parquet.compression'='SNAPPY');


--ALTER TABLE ${__GREEN_MAIN_DB__}.t_ins_splunk_hist DROP IF EXISTS PARTITION (environment = '${environment}');

INSERT OVERWRITE TABLE ${__BLUE_MAIN_DB__}.t_ins_splunk_hist PARTITION(environment)
SELECT 
    splunk_hist.clnt_obj_id,
    splunk_hist.pers_obj_id,
    splunk_hist.metric_ky as mtrc_ky,
    COALESCE(splunk_hist.number_of_hits,0) as mtrc_hits_cnt,
    CURRENT_TIMESTAMP,
    unique_rec.db_schema,
    unique_rec.environment
FROM 
( 
     SELECT 
     clnt_obj_id,
     pers_obj_id,
     metric_ky,
     count(1) as number_of_hits
     FROM 
       (
         SELECT 
              tot_hist.orgid as clnt_obj_id,
              tot_hist.aoid as pers_obj_id,
              tot_hist.ins_hash_val,
              metric_ky 
         FROM  ${__BLUE_MAIN_DB__}.emi_prep_recom_metric_view tot_hist
         INNER JOIN 
             (  SELECT  
                   orgid,
                   aoid,
                   event_date
                 FROM  
                     ( SELECT 
                        orgid, 
                        aoid, 
                        to_date(EVENT_TIME) as event_date,
                        ROW_NUMBER() OVER (PARTITION BY orgid,aoid ORDER BY to_date(EVENT_TIME) desc) as rank 
                        FROM ${__BLUE_MAIN_DB__}.emi_prep_recom_metric_view
                      ) sub
                       WHERE rank = 1 
               ) recent_event
          ON  tot_hist.orgid = recent_event.orgid
          AND tot_hist.aoid = recent_event.aoid
          WHERE  to_date(tot_hist.EVENT_TIME) > to_date(DATE_SUB(recent_event.event_date,90))
          AND tot_hist.metric_ky in (2,5,57,59,60,63,65,69,73,74,76,78,79,201,202)
       ) int_hist_mtrc_cnt
     GROUP BY clnt_obj_id,pers_obj_id,metric_ky
) splunk_hist 
INNER JOIN 
(
    SELECT DISTINCT 
        clnt_obj_id,
        pers_obj_id,
        db_schema,
        environment
    FROM 
        ${__RO_BLUE_RAW_DB__}.dwh_t_dim_pers
) unique_rec
ON   splunk_hist.clnt_obj_id = unique_rec.clnt_obj_id
AND  splunk_hist.pers_obj_id = unique_rec.pers_obj_id
