# Spark-data-sync

## Overview

This module consists of a bunch of utilities developed using spark2-api

## hiveTableSyncManager
- this tool reads configuration of hive tables marked for replication from a meta data table SPARK_DATA_SYNC_CATALOG in dxr schema. the meta data includes the following fields

| ColumnName             	| Description                                                                                                                                                                                                                                         	|
|------------------------	|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| SOURCE_TABLE           	| table name that is to be synced                                                                                                                                                                                                                     	|
| SOURCE_SCHEMA          	| schem name of source                                                                                                                                                                                                                                	|
| TARGET_SCHEMA          	| schema name of target                                                                                                                                                                                                                               	|
| FORCE_SYNC_DAY_OF_MNTH 	| day of the month on which the table is to be force synced. All the comparing criteria will be skipped and the table will be over written.  If zero the sync follows the usually flow of comparing and syncing incase of differences. Defaults to 0. 	|
| ACTIVE                 	| flag that specifies if this configuration row is still valid. Its boolean and the possible values are 1 or 0. Default is 0.                                                                                                                         	|
| MAX_HIST_PARTITIONS    	| An integer the represents the maximum number of historical partitions to retain. if 0 no limit is applied on to history. Default is zero                                                                                                            	|
| IS_VIEW                	| Flag that specifies if the copy to made is a view or table. Its boolean so possible values are 1 and 0. Defaults to 0                                                                                                                               	|

- program arguments 

| arg ShortName 	| arg FullName         	| Description                                                                                                                                                                                      	|
|---------------	|----------------------	|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	|
| p             	| password             	| password to DB containing the sync info                                                                                                                                                          	|
| u             	| user-name            	| username of DB containing the sync info                                                                                                                                                          	|
| j             	| jdbc-url             	| jdbc connection URL to DB containing the sync info                                                                                                                                               	|
|               	| application-name     	| optinal application name to be displayed in YARN. Defaults to "Data Sync Util"                                                                                                                   	|
| a             	| adp-internal-clients 	| csv of internal clients to be filtered.                                                                                                                                                          	|
| t             	| table-name           	| optinal source table name to filter from the list of table names. If given this single table will be synced by force skipping comparisons. Defaults to none. sending none will skip table filter 	|
| p             	| parallel-hive-writes 	| number of hive table writes to be done in parallel. Defaults to 2                                                                                                                                	|
 - event logging

    - failures and succesful completion of the each row of meta data is logged into the a table by the name SPARK_HIVE_WRITER_JOB_LOGS in dxr schema. this contains the time taken for writing hive data or message specifying the error of info if the sync was skipped.

 - Supported features

    - detect differences in schema and recreate the entire table based on source in case of mismatch.
    - copy the entire table is missing in target schema.
    - copy missing partitions only if mismatch found.
    - force a table to synced on regular intervels in cases where there are no metadata changes and only the content has changes.

- Features under developement
    - detect if partitions are overridden and sync data
    - create views instead of tables in target pointing to the source
    - limit the number of historical partitions maintained in target


## hiveDDLGenerator
- This tool was developed specially for migartion of existing hive table in CDH to AWS. The program takes target s3 bucket and generates create table DDLS of all the tables under a hive schema adding the location param pointing to s3 bucket location.

- program arguments

| arg shortname 	| are fullName  	| Description                                                                                                                       	|
|---------------	|---------------	|-----------------------------------------------------------------------------------------------------------------------------------	|
| d             	| dbname        	| name of source CDH hive schema                                                                                                    	|
| s             	| s3-buket-path 	| s3 bucket url to placed as Location in the DDL generated                                                                          	|
| f             	| ddl-file-name 	| final name of the file into which all the DDL statements are written. the file is stored under /tmp/ in hdfs with name specified. 	|
| g             	| gluedb-name   	| target glue DB Name that is to replaced in the DDLS 

- features supported 
    - generate create DDL replace dbname with glue DB Name and add s3 bucket in as Location attribute
    - remove TBLPROPERTIES but retain useful ones like parquet.compression
    - add a custom properly DDL.migrated.on with the date on which the DDL was generated for tracking
