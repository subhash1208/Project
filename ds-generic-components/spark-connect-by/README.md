# Main - Spark Connect By

## Overview
-------------------

This is a general purpose component which can be used for making hierarchical queries using spark. This is not exactly a Spark SQL extension to provide a hierarchical construct in SQL queries. However, it can be used to compute basic metrics like direct/indirect/total reports, layer etc given necessary inputs. It can be applied on any tables with any columns designated as parent/children and any custom start rows condition. It avoids loops by automatically avoiding to form them.
    
  

## Maintainers
-------------------
 - Manoj Oleti - manoj.oleti@adp.com  
 - Srinivas Kummarapu - Srinivasa.Kummarapu@ADP.com
    
  
## How to use
-------------------
### Downloading

The latest stable version is 2.0 (compiled against Spark 1.6). It can be downloaded from [Nexus](http://cdladpinexus01:8081/nexus/content/groups/public/com/adp/datacloud/ds/spark-connect-by/2.0/spark-connect-by-2.0.jar)

### Usage

Below is the invocation command. The spark parameters listed below are indicative only and may need to be tuned depending on the type/size of dataset. The "docs" folder contains slides providing an overview of the framework.

At a high level, below are the inputs and outputs

  * Inputs
	* DB Name & Table name 
	* OID & Parent OID columns (referred to as column and parent)
	* Group columns (optional). These control if groups/partitions exist. Eg, yyyymm etc.

  * Outputs
	* DB Name 
	* Two output table names (one for metrics and the other for the exploded hierarchy table) 


Example Invocation
	
	spark-submit --class com.adp.datacloud.ds.orgStructure \
	  --master yarn-cluster \
	  --driver-memory 12G \
	  --num-executors 25 \
	  --executor-cores 6 \
	  --executor-memory 12G \
	  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
	  --conf spark.shuffle.blockTransferService=nio \
	  --conf spark.yarn.executor.memoryOverhead=3200 \
	  --conf spark.yarn.driver.memoryOverhead=5400 \
	  --conf spark.rdd.compress=true \
	  --conf spark.akka.frameSize=50 \
	  --conf spark.network.timeout=480 \
	  --conf "spark.executor.extraJavaOptions=-XX:MaxPermSize=3072m -XX:PermSize=3072m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:-UseConcMarkSweepGC" \
	  spark-connect-by-3.0-SNAPSHOT.jar \
	  --input-spec fitdsmain.tp_nas_waf \
	  --output-db-name fitdsmain \
	  --connect-by-column pers_obj_id \
	  --connect-by-parent mngr_pers_obj_id \
	  --hierarchy-table tp_nas_layers \
	  --hierarchy-exploded-table tp_nas_layers_exploded \
	  --group-columns source_system,l2_code,clnt_obj_id,qtr_cd \
	  --enable-checkpoint false \
	  --output-partition-spec pp=until_2017q1

    
The process accepts multiple options to enable users to configure its behavior. If any of the required parameters are not provided, appropriate error message is thrown.

	HierarchyBuilder v2.0
	Usage: Hierarchy Builder [options]
	
	  -n, --application-name <value>
	                           Name of the application.
	  -i, --input-spec <value>
	                           Input Either of 1) Fully qualified table or 2) Sql file
	  -q, --qtr-cd <value>     Quarter code for which layers to be computed.If not provided layers will be computed for all quarters
	  -l, --local-parquet-input-mode <value>
	                           Treat the inputs and outputs as local/hdfs paths to parquet files. Defaults to false. If set to true, the input-partitions-* parameters are ignored
	  -g, --group-columns <value>
	                           Comma Separated Group/partition columns
	  -o, --output-db-name <value>
	                           Hive Database name where output tables should be saved. Ignored if hdfs-output-path is provided
	  --connect-by-column <value>
	                           Primary key for hierarchical join. Defaults to pers_obj_id
	  --connect-by-parent <value>
	                           Primary key for hierarchical join. Defaults to mngr_pers_obj_id
	  --root-indicator-column <value>
	                           Column with boolean data-type to indicate the rows which should be used as root. If not specified, default start-rows logic is applied.
	  --hierarchy-table <value>
	                           Hive Database table name where hierarchy computation results are stored (contains direct_children etc...)
	  --hierarchy-exploded-table <value>
	                           Hive Database table name where hierarchy exploded results are stored (contains level_from_parent field)
	  -t, --output-partition-spec <value>
	                           Output Partition Spec (column=value). Optional. Example pp=2017q1
	  -p, --input-partition-column <value>
	                           Optional partition column to filter the input table with
	  -v, --input-partition-column-values <value>
	                           List of input partition values to query against
	  -n, --num-shuffle-partitions <value>
	                           Number of partitions to be used for spark shuffle. Defaults to 200
	  -d, --distribute-by <value>
	                           Distribute input by the specified columns prior to aggregate processing.
	  -e, --enable-tungsten <value>
	                           Enable/Disable tungsten (true/false). Defaults to true. Disable this while using Spark 1.5
	  -c, --enable-checkpoint <value>
	                           Persist and Checkpoint the input dataframe prior to aggregate processing. Defaults to false.
	  -r, --reliable-checkpoint-dir <value>
	                           RDD Checkpoint directory. Defaults to /tmp/checkpoints/
	  -w, --warn-and-exit <value>
	                           Warn about possible issues prior to building and exit. This adds an pre-step to scan dimensions in the configuration. Defaults to true
	  -s, --save-format <value>
	                           Output Table format. Supported values are 'parquet' and 'text' 
	  --help                   Prints usage
	ADP Hierarchy Builder Framework.     

The above usage can be displayed by invoking the orgStructure with --help option.
