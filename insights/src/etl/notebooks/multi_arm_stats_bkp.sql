-- Databricks notebook source
DROP TABLE IF EXISTS ${__BLUE_MAIN_DB__}.multi_arm_stats_backup;
CREATE TABLE IF NOT EXISTS ${__BLUE_MAIN_DB__}.multi_arm_stats_backup
USING PARQUET
AS
SELECT 
    *
  FROM 
    ${__BLUE_MAIN_DB__}.multi_arm_stats