-- Databricks notebook source
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.emi_prep_recom_metric_view_backup;

CREATE TABLE ${__BLUE_MAIN_DB__}.emi_prep_recom_metric_view_backup
using PARQUET AS
select 
    *
from ${__BLUE_MAIN_DB__}.emi_prep_recom_metric_view