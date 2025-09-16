# RDBMS Data Extractor

# Overview

This project contains utilities around the access to RDBMS systems from Hadoop Cluster. Below is the full list.

- **DXR Data Extractor**: Utility to parallelize and distribute the extracts of data from RDBMSes using Cluster resources. 
- **FlatFile Based SQL Executor**: Utility to parallelize and distribute custom pl/SQL scripts on RDBMSes using Cluster resources.
- **FlatFile Based Parallel Data Exporter**: Utility to parallelize and distribute the export of data to RDBMSes. 
- **FlatFile Based Data Extractor (Deprecated)**: Similar to DXR Data extractor, except that the input connections are provided using a flat-file and DXR Catalog is not leveraged. It only supports parallelization of a single SQL at a time.

Support for DXR Catalog will be added to the SQL Executor and Data Exporter utilities also in the near future.

The below documentation goes into each of the above components in detail.

## Maintainers
  * Siddarthasagar Chinne - siddarthasagar.chinne@adp.com
  * Pavan Kumar Soma - pavankumar.soma@adp.com
  * Vamshikrishna Gunnala - vamshikrishna.gunnala@adp.com
  * Nara Vishal - vishal.nara@adp.com
  * Manoj Oleti - manoj.oleti@adp.com


## DXR Data Extractor

Reusable data extractor utility to run free-form SQL queries against databases and retrieve results into HDFS. This component is based on Spark's JDBC support. This is an alternative for Sqoop. Sqoop excels at retrieving tables from RDBMS to HDFS. However, for free form queries, the support is limited. 
  
Some key features/capabilities of this extractor below.

  * Retrieves client-level data - for multiple clients - by firing custom queries against relational databases and saves the results to HDFS. 
  * The list of clients for which the extracts is controllable via DXR Catalog database.
  * The degree of parallelism for importing the data is configurable as a runtime parameter. 
  * Ability to batch imports for performance.
  * Verbose logging of results and performance in DXR Catalog.
  * Support for Parquet/Delimited tables.
  * On-the-fly hashing of data, based on column conventions (columns ending with "\_hash" suffix).
  * Inbuilt data-type support and conversion from JDBC to Hive Datatypes.
  * Inbuilt Opt-out support.
  * Single-pass writes to Landing, Blue & Green Zones in Hive.
  * Currently Supports extracts from Oracle & SQLServer Databases. 
  * the tool generates an internal variable by the name dbSchema which is a concatenation of "OWNER_ID|service name from DNS|CONNECTION_ID". this can be substituted in the extract queries against the placed holder variable __[DBSCHEMA__]
 
 Note: --product-name,--dxr-jdbc-url,--green-db,--landing-db,--optout-master and --sql-files are madatory params with no defaults

#### options:
```
	Usage: DXR Parallel Data Extractor [options]

	--product-name <value>   dxrProduct Name or dxrsql file for which extract has to run
	--dxr-jdbc-url <value>   DXR Database JDBC URL
	--source-db <value>      Source DB Type oracle/sqlserver
	--green-db <value>       Target Green output Hive Database Name
	--blue-db <value>        Blue Database Name
	--landing-db <value>     Landing Database Name
	--optout-master <value>  Optout Master table name. Defaults to 'none'. Set this to 'none' to disable optout. Use this responsibly and with caution
	--sql-conf <value>       Hiveconf variables to replaced in query or in vpd
	--sql-files <value>      List of SQL file names to be used for extract
	--output-partition-spec <value>
							Comma separated Output Partition Spec (column1,column2 or column1=value1,column2=value2).
	--batch-column <value>   The column name to be used for batching retrievals.
	--upper-bound <value>    Optional Upper bound when using batching. Defaults to 200
	--lower-bound <value>    Optional Lower bound when using batching. Defaults to 0
	--fetch-array-size <value>
							Fetch array size for extracts. Set this carefully as it can impact performance. Defaults to 1000
	--num-batches <value>    Number of partitions(or batches) for retrieval for each 'DXR target'. Ignored when batch-column is used. Defaults to 4. At maximum these many queries shall be fired on any given 'DXR target' per SQL
	--application-name <value>
							Name of the application.
	--max-db-parallel-connections <value>
							Max Number of Parallel connections per DB. Defaults to 10
	--null-strings <value>   Comma separated list of string literals which shall be considered as equivalent to NULLs. Defaults to 'UNKNOWN,NULL,unknown,null'
	--persql-limit <value>   Development only setting to limit the number of records extracted from each SQL. Default 0 (no limit).
	--single-target-only <value>
							Perform the extract from only a single target among possible targets. Useful while retrieving metadata from one of the possible connections
	--query-timeout <value>  JDBC Query Timeout in seconds. Defaults to 1800 seconds
	--isolation-level <value>
							Transaction isolation level while reading data from target data bases. possible values are 0,1,2,4 and 8. Default value is 2
	--compression-codec <value>
							Compression Codec. Default is SNAPPY. Set this field to 'UNCOMPRESSED' to disable compression
	--oracle-wallet-location <value>
							Oracle wallet location. Defaults to /app/oraclewallet.
	--s3-staging-bucket <value>
							S3 bucket to be used for holding staging data
	--optout-rerun-spec <value>
							Specify the WHERE condition need to applied while reading Blue Landing DB table to rerun opted-out data to GREEN/BLUE zones. By defalut full data will be considered. Eg. --optout-rerun-spec ingest_month>201909
	--strict-mode <value>    Mark the dxr job as failure in case of connection failures. Default value true
```


#### Example

```bash
	spark-submit --files "$SQL_FILE_PATH/cr_job_data.sql,$SQL_FILE_PATH/cr_resume_data.sql,$SQL_FILE_PATH/cr_score_data.sql,$SQL_FILE_PATH/cr_status_data.sql" \
		    --class com.adp.datacloud.ds.rdbms.dxrParallelDataExtractor \
	 		--master yarn-cluster \
	  		--driver-memory 30G \
	  		--num-executors 10 \
	  		--executor-cores 2 \
			--executor-memory 20G \
	  		--driver-cores 5 \
	  		--jars /var/lib/sqoop/ojdbc6.jar \
	  		$SCRIPTPATH/../rdbms-integration.jar \
			--product-name $DXR_PRODUCT_NAME \
			--dxr-jdbc-url $DXR_JDBC_URL \
			--dxr-db-username $DXR_USERNAME \
			--dxr-db-password $DXR_PASSWORD \
			--output-hive-db __GREEN_RAW_DB__ \
			--blue-hive-db __BLUE_RAW_DB__ \
			--landing-db __BLUE_LANDING_DB__ \
			--input-spec cr_job_data,cr_resume_data,cr_score_data,cr_status_data \
			--num-partitions $NUM_PARTITIONS_PER_DB_SCHEMA \
			--default-partition $TARGET_PARTITION \
			--overwrite-target-table true \
			--batch-column batch_number \
			--max-parallel-threads $DRIVER_PARALLELISM \
			--upper-bound 200
```

## Parallel Data Exporter

Reusable data export utility to export data from a given hive table(or given partition/s of a hive table) to multiple warehouse schemas in parallel.

This is an alternative for Sqoop. Sqoop can do an export from a given hive table/hive query to one database connection at a time (though we can export data from one hive table/query with parallel mappers). This parallel exporter can export data from a given hive table(or given partition/s of a hive table) to multiple warehouse schemas in parallel. Further, it also supports Parquet format tables which are currently not supported in sqoop.

the tool generates an internal variable by the name dbSchema which is a concatenation of "OWNER_ID|service name from DNS|CONNECTION_ID". if the hive to be exported contains a column name by the name ***db_schema*** it automatically filters the data using the column value and exports filtered data to the corresponding DXR targets.

Note: --product-name,--dxr-jdbc-url,--hive-table and --target-rdbms-table are madatory params with no defaults


#### options:

```
	Usage: DXR Parallel Data Exporter [options]

	-c, --product-name <value>
							dxrProduct Name or dxrsql file for which extract has to run
	-h, --dxr-jdbc-url <value>
							Database Datasourcename (DNS)
	-w, --oracle-wallet-location <value>
							folder path of the oracle wallet. Defaults to /app/oraclewallet
	-s, --target-db <value>  Source DB Type oracle/sqlserver. Defaults to oracle 
	-n, --application-name <value>
							Name of the application.
	--source-port <value>    Database Port. Defaults to 1521
	--pre-export-sql <value>
							pre Export sql name
	--post-export-sql <value>
							post Export sql name
	-h, --hive-table <value>
							Export Hive Table Name
	--hive-partition-column <value>
							Export partition column name in the hive table. (Optional)
	--hive-partition-value <value>
							Export partition column value in the hive table. (Optional)
	-t, --target-rdbms-table <value>
							Target warehouse table to which we need to export data
	-s, --use-staging-table <value>
							use staging table before copying data to target table
	-g, --staging-table-name <value>
							Name of the staging table.
	--sql-conf <value>       Sqlconf variables to replaced in sql query
    --plsql-timeout <value>  Time out for PLSQL executions in seconds. Defaults to 14400	
	--help                   Prints usage
	ADP Spark Based DXR Parallel RDBS Data Exporter. 
```  
  
### Example

```bash  
    spark-submit --files "$SCHEMA_FILE" \
 		--master yarn-cluster \
  		--num-executors $NUM_CONNECTIONS \
  		--executor-memory 10G \
  		--executor-cores 1 \
  		--driver-memory 8G \
  		--driver-cores 5 \
  		--conf spark.storage.memoryFraction=0.5 \
  		--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  		--conf spark.shuffle.blockTransferService=nio \
  		--conf spark.rdd.compress=true \
  		--conf spark.akka.frameSize=50 \
  		--conf spark.network.timeout=480 \
  		--conf spark.yarn.executor.memoryOverhead=2400 \
  		--conf spark.yarn.driver.memoryOverhead=4800 \
  		--class com.adp.datacloud.ds.rdbms.parallelDataExporter \
  		--jars /var/lib/sqoop/ojdbc6.jar \
  		$SCRIPTPATH/../rdbms-integration.jar \
  		--file $CONN_FILE_NAME \
  		--export-hive-db __HIVE_TP_DB__ \
  		--export-hive-table t_empl_trnovr_prblty \
  		--export-partition $PARTITION \
  		--target-warehouse-table T_EMPL_TRNOVR_PRBLTY
```


## Sql Executor
   Reusable utility to run given stored procedure/s or SQL's on external databases.

## Parallel Sql Executor
   Reusable utility to run given stored procedure/s on multiple database schemas in parallel.



## How to use


### DXR Parallel Data Extractor Rerun

features:

* Reruns a failed dxr extract by input application ID. All the configuration details from the parent job are used in the rerun.

Syntax:

	DXRParallelDataExtractorRerun v3.7
	Usage: DXR Parallel Data Extractor Rerun [options]

	-i, --input <value>
					ApplicationID of the DXR job to be rerun
	-h, --dxr-jdbc-url <value>
					Database Datasourcename (DNS)
	-u, --dxr-db-username <value>
					Database User/Schema Name
	-p, --dxr-db-password <value>
					Database Password
	--help                   Prints usage
	ADP Spark Based DXR Parallel Data Extractor. 

### Example
```bash
    spark-submit --class com.adp.datacloud.ds.rdbms.dxrParallelDataExtractorReLauncher \
 		--master yarn-cluster \
  		--driver-memory 30G \
  		--num-executors 10 \
  		--executor-cores 2 \
		--executor-memory 20G \
  		--driver-cores 5 \
  		--jars /var/lib/sqoop/ojdbc6.jar \
  		$SCRIPTPATH/../rdbms-integration.jar \
		--dxr-jdbc-url $DXR_JDBC_URL \
		--dxr-db-username $DXR_USERNAME \
		--dxr-db-password $DXR_PASSWORD \
		--input application_1524347154663_0590
```


### Parallel Data Exporter



### Parallel Sql Executor

Syntax:
  
  	ParallelSqlExecutor v3.0
	Usage: Parallel Sql Executor [options]	
	  
      -c, --connFile  <value>
                               Schema connections file
      -n, --application-name <value>
	                           Name of the application.
	  -s, --sqlFile <value>
	                           Sql File to be Executed	  
	  --source-port <value>    Database Port. Defaults to 1521	  
      -m  --max-parallel-threads  <value>
                               Max Number of Parallel connections. Defaults to 5	   
	  --help                   Prints usage
	  
	ADP Spark Based Parallel Sql Executor. 
	  
  
### Example
```bash  
    spark-submit --files "/tmp/sql_executor_conn.txt,$SCRIPTPATH/../../hql/dw/create_partition.sql" \
  		--master yarn-cluster \
  		--num-executors $NUM_CONNECTIONS \
  		--executor-memory 10G \
  		--executor-cores 1 \
  		--driver-memory 8G \
  		--driver-cores 2 \
  		--class com.adp.datacloud.ds.rdbms.parallelSqlExecutor \
  		--jars /var/lib/sqoop/ojdbc6.jar \
  		$SCRIPTPATH/../rdbms-integration.jar \
  		--connFile sql_executor_conn.txt \
  		--sqlFile create_partition.sql
```

### FlatFile Based Parallel Data Extractor

features:

* Takes flat file as input for connection details
	* connections file is a csv expected in the format
		<TARGET_PARTITION>,<USER_NAME>,<DB_HOST_NAME>,<DB_SERVICE_ID>,<DB_PASSWORD>,<SCHEMA_PREFIX>,<VPD_KEY>

* error handling, logs are saved into table data_extractor_logs

* SCHEMA_PREFIX is optional but if provided in connections file substitutes the variable 'SQL_SCHEMA' in the input sql during extract

* VPD_KEY is optional but if provided in connections file sets VPD context for each connection made

* Supports Hashing i.e if any column-name in the input sql is suffixed by '_HASH' then resultant extract will hash the content before storing it. the target column-name will be stripped of the suffix '_HASH'

Syntax:
  
	ParallelRdbmsDataExtractor v3.7
	Usage: Parallel RDBMS Data Extractor [options]

	-c, --file <value>
					Schema connections file
	-n, --application-name <value>
					Name of the application.
	-i, --input-spec <value>
					Input Either of 1) Table Name or 2) Sql file
	--source-port <value>
					Database Port. Defaults to 1521
	-d, --output-hive-db <value>
					Target Hive Database Name
	-t, --output-hive-table <value>
					Target Hive Table Name
	-t, --overwrite-target-table <value>
					Target Hive Table Truncate. Example --target-table-truncate false
	-o, --output-partition-spec <value>
					Comma separated Output Partition Spec (column1,column2 or column1=value1,column2=value2). Defaults to 'db_schema'
	-b, --batch-column <value>
					The column name to be used for batching retrievals.
	--batch-dimension-table <value>
					Only applicable if the batch_column is not numeric. 
	-p, --max-parallel-threads <value>
					Max Number of Parallel connections. Defaults to 5
	-m, --num-partitions <value>
					Number of partitions(or batches) for retrieval for each db. Ignored when use-batches is false. Defaults to 4. At maximum these many parallel connections to a single DB will be opened (provided there are sufficient executor-cores)
	-r, --replace-null-for-strings <value>
					Comma Separate list of strings which should be considered equivalent to NULL
	--rdbms-fetch-size <value>
					Database Fetch Size (number of records). Defaults to 10
	--lower-bound <value>
					Lower bound for generating batches. Only used during batched retrievals. Defaults to 0
	--upper-bound <value>
					Upper bound for generating batches. Only used during batched retrievals. Defaults to 200
	--sql-conf <value>
					Hiveconf variables to replaced in query
	-s, --save-format <value>
					Output Table format. Supported values are 'parquet' and 'text'
	--help                   Prints usage
	ADP Spark Based Parallel RDBMS Data Extractor.
	  
	ADP Spark Based Parallel RDBS Data Extractor. 
	  
  
### Example
  
```bash
    spark-submit --files "$SCHEMA_FILE,$SCRIPTPATH/../../hql/dw/tp_dw_extract.sql" \
	    --class com.adp.datacloud.ds.rdbms.parallelDataExtractor \
 		--master yarn-cluster \
  		--driver-memory 30G \
  		--num-executors 10 \
  		--executor-cores 2 \
		--executor-memory 20G \
  		--driver-cores 5 \
  		--jars /var/lib/sqoop/ojdbc6.jar \
  		$SCRIPTPATH/../rdbms-integration.jar \
 		--file $CONN_FILE_NAME \
  		--input-spec tp_dw_extract.sql \
  		--output-hive-db __HIVE_TP_DB__ \
  		--output-hive-table tp_dw_extract_staging \
  		--overwrite-target-table true \
  		--batch-column clnt_obj_id $CLNT_OBJ_ID_FILTERS \
  		--use-batches true
```


## Reference Implementations
  * Autocoder
  * Turnover Probability
  * Rosie
  * Candidate Relevency


## Documentation
  * [Confluence](To be updated)

### Snowflake Data Processor

Options: Export, Extract
Extract is not yet developed fully to use as of now
Usage: Snowflake data processor [options]

  -z, --dxr-jdbc-url <value>
                           DXR credentials for Logging
  -m, --process-mode <value>
                           Process mode for snowflake options: export, extract
  -h, --target-env <value>
                           Target environment for export/extract. Eg: DIT, FIT, IPE, IAT, UAT, PROD. Required if credentials are not given
  -l, --account <value>    Account details
  -u, --user <value>       Snowflake db UserName
  -r, --role <value>       Role
  -w, --warehouse <value>  Snowflake warehouse specifier
  -d, --database <value>   Snowflake database specifier
  -s, --schema <value>     Snowflake schema specifier
  -n, --target-table <value>
                           Snowflake target table name or comma separated target table names
  -f, --sqlfile <value>    Input Sql file or multiple sql semicolon separated in the sqeuence of target table provided
  -x, --pre-sql <value>    Input pre actions
  -y, --post-sql <value>   Input post actions
  -t, --table-name <value>
                           Table name for export/extract, or multiple table names of comma separated tables in the sequence of target table provided
  -p, --hiveconf <value>   Hiveconf variables to replaced in query
  -q, --sqlconf <value>    Hiveconf variables to replaced in query
  -c, --col-mapping <value>
                           Column mapping approach order/name. Defaulted to order
  -j, --use-stage <value>  Use staging table values on/off. Default on
  -o, --ovr-ky <value>     Overwrite key to truncate the data from the same key
  -b, --partition-size <value>
                           Value for partition size in MB
  --help                   Prints usage

### Example

```bash
spark-submit \
--master yarn \
--class com.adp.datacloud.ds.snow.SnowDataProcess \
--driver-memory 10G \
--driver-cores 8 \
--executor-memory 26G \
--executor-cores 8 \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.master=yarn \
--conf spark.sql.shuffle.partitions=2000 \
--conf spark.task.cpus=2 \
--conf spark.executor.memoryOverhead=2G \
--conf spark.driver.memoryOverhead=1G \
--conf spark.sql.parquet.compression.codec=SNAPPY \
--conf spark.orchestration.cluster.id=j-2FL06UAXDG4JP \
--conf spark.app.name=Snowflake_process_export \
--files $SELECT_FILE,$PRE_FILE,$POST_FILE $SCRIPTPATH/../rdbms-integration.jar \
--dxr-jdbc-url $JDBCURL \
--target-env DIT \
--account $ACCOUNT \
--user $USER \
--warehouse $WH \
--database $DB \
--schema $SCH \
--target-table T_BNCHMRK_ANNL_DATA \
--role $ROLE \
--process-mode export \
--sqlfile $SELECT_FILE \
--pre-sql $PRE_FILE \
--post-sql $POST_FILE \
--col-mapping order \
--use-stage order \
--ovr-ky rpt_dt \
--hiveconf ingest_month=202103 \
--sqlconf pkg_version=20.6.15
```