# Main - Spark Sql Wrapper

## Overview
-------------------

Reusable query wrapper to execute hive query from file or individual hive query.Instead of using traditional mapreduce approach to execute hive/beeline queries, this wrapper uses spark sql engine to execute the sql files, which will boost the performance of the query and possibily bring down the total execution time of the query compared to mapreduce approach.


## Maintainers
-------------------
 - Manoj Oleti - manoj.oleti@adp.com  
 - Vamshikrishna Gunnala - Vamshikrishna.Gunnala@adp.com


## How to use
-------------------

### Downloading

The latest stable version is 3.2 (compiled against Spark 1.6). It can be downloaded from [Nexus](http://cdladpinexus01:8081/nexus/content/groups/public/com/adp/datacloud/ds/spark-sql-wrapper/3.2/spark-sql-wrapper-3.2.jar)

### Usage

Below is the invocation command. The spark parameters listed below are indicative only and may need to be tuned depending on the size of dataset.

    
The sql-wrapper process accepts multiple options to enable users to configure its behavior.If any of the required parameters are not provided, appropriate error message is thrown.

	Sql/Hive Query Executor", "v3.2"
	Usage: Sql/Hive Query Executor [options]
	
	  -f, --file <value>
	                           Input Sql file.
	  -e, --execute <value>
	                           Sql string to execute.
	  -p, --hiveconf <value>
	                           Hiveconf variables to replaced in query.
	  -s, --num-shuffle-partitions <value>
	                           Number of partitions to be used for spark shuffle.Defaults to 200.                         
	  -e, --enable-tungsten <value>
	                           Enable/Disable tungsten (true/false). Defaults to true. Disable this while using Spark 1.5.
	  --help                   Prints usage
	  
	ADP Sql/Hive Query Executor Framework.. 
    
Example invocation.

	safe_call spark-submit --files "../nas/tp_nas_pd.sql"
	        --master yarn-cluster \
            --num-executors 70 \
            --executor-memory 24G \
            --executor-cores 4 \
            --driver-memory 32G \
            --driver-cores 4 \
            --conf spark.storage.memoryFraction=0.5 \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.shuffle.blockTransferService=nio \
            --conf spark.rdd.compress=true \
            --conf spark.akka.frameSize=50 \
            --conf spark.network.timeout=480 \
            --conf spark.yarn.executor.memoryOverhead=2400 \
            --conf spark.yarn.driver.memoryOverhead=4800 \
            --class com.adp.datacloud.wrapper.sqlWrapper \
            spark-sql-wrapper.jar \
            --file tp_nas_pd.sql \
            --num-shuffle-partitions 200 \
            --hiveconf LATESTQTR=2017Q1 \
            --hiveconf SOURCE=nas
            
**Note**: Passing sqlfile or hivequery as a argument is mandatory.

The above usage can be displayed by invoking the Sqlwrapper with --help option.

Example Invocation from Overtime - [here](https://bitbucket.es.ad.adp.com/projects/DSMAIN/repos/overtime/browse/cook/shell/prepare_main_dataset.sh#90-107).