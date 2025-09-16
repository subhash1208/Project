# Main - Spark Shape Monitor

## Overview
-------------------

This is a simple xml configuration driven reusable framework for computing dataset shapes, validating data and identifying outliers in the data. This framework builds on top of Amazon Deeque library to identify shapes of data by default. In addition, the framework have native spark sql support to identify shapes of data.

The framework reads configurations from xml and computes following column wise stats on the given table for each partition spec, if applicable, and finally stores in a rdbms table:

  - NUM_RECORDS, 
  - NUM_MISSING, 
  - NUM_INVALID, 
  - MEAN,
  - STANDARD_DEVIATION,
  - MIN,
  - MEDIAN,
  - FIFTH_PERCENTILE,
  - FIRST_DECILE,
  - FIRST_QUARTILE,
  - THIRD_QUARTILE,
  - NINTH_DECILE,
  - NINTY_FIFTH_PERCENTILE,
  - MAX,
  - NUM_APPROX_UNIQUE_VALUES,
  - DISTINCTNESS,
  - ENTROPY,
  - UNIQUE_VALUE_RATIO

After column wise stats computation and storing, the framework performs pre-defined validations using computed stats to identify anomalies in the data. In addition, the framework supports custom validations through xml configuration file.
The framework finally writes all validations details to another rdbms table for reveiw.

## Rdbms tables used by this framework 
--------------------------------------
This framework currently writes all Stats collected information, Validations results and Spark Job run logs to following DXR Catalog tables. The framework itself creates below objects, for first run , in the new environment.

1. **SHAPE_COLUMN_STATISTICS_STG  :** The framework Writes (appends) all Stats collected information to this table.
2. **SHAPE_COLUMN_STATISTICS      :** This is view refers to latest snapshot of **SHAPE_COLUMN_STATISTICS_STG** table.
3. **SHAPE_VALIDATION_SUMMARY_STG :** The framework Writes (appends) all validations details to this table along with results.
4. **SHAPE_VALIDATION_SUMMARY     :** This is view refers to latest snapshot of **SHAPE_VALIDATION_SUMMARY_STG** table.
5. **SPARK_SHAPE_MONITOR_JOB_LOGS :** The framework Writes (appends) all spark jobs run logs to this table.


Note: In future DXR catalog DB will be replaced by AWS RDS instance.


## Maintainers
-------------------
 - Manoj Oleti - manoj.oleti@adp.com
 - Abhishek Kushwaha - abhishek.kushwaha@adp.com
 - Pavan Kumar Soma - pavan.soma@adp.com
 

## How to use
-------------------
### Downloading

The latest working snapshot version can be downloaded from Artifactory.

### Usage

Below is the sample invocation command. The spark configuration parameters listed below are indicative only and may need to be tuned depending on the size of dataset.

**Note**: Angular braces denote mandatory parameters and square braces denote optional parameters 

	safe_call spark-submit --class  com.adp.datacloud.ds.ShapeMonitor \
            --master yarn \
            --deploy-mode cluster \
            --driver-memory 5g \
            --driver-cores 4 \
            --num-executors 100 \
            --executor-cores 1 \
            --executor-memory 8G \
            --name "Shape_Monitor" \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.logConf=true \
            --conf spark.logLineage=true \
            --conf spark.executor.memoryOverhead=2400 \
            --conf spark.yarn.maxAppAttempts=1 \
            --files "<shape_config.xml>" \
            shape-monitor.jar <options...>

    
This command accepts following options 

	Usage: ADP DataShape Statistics Collector and Shape Validator [options]
	
	  -x, --xml-config-file <value>
                           XML config file relative path from current working directory. Mandatory
	  -h, --db-jdbc-url <value>
                           Database Datasourcename (DNS)
      -e, --environment <value>
	                       Logical environment. This is used to determine the default DB connection settings.
      -w, --oracle-wallet-location <value>
	                       Oracle Wallet location
      -f, --full-rerun <value>
                           Enforce full rerun of table stats
      -u, --db-username <value>
                           Database UserName
      -p, --db-password <value>
                           Database Password
      -t, --target-tablename <value>
                           Target table Name to write stats
      -n, --parallel-computations <value>
                           Number of tables need to be computed in Parallel. Defalut is 2
      -v, --time-partition-value <value>
                           Latest Temporal partition value in the data.
      -p, --conf <value>
                           Variables to replaced in sql.
	  --help               Prints usage
	  

The above usage can be displayed by invoking the Shape Monitor with --help option.
Note: Either of the three.. (environment) OR (jdbc-url, username, password) OR (jdbc-url, wallet) options should be provided
        
### Developer Environment Setup

This framework works with Spark versions 2.2.x to 2.4.x.
Follow instructions provided in the parent repo (ds-generic-components) [Developer Environment Setup](https://bitbucket.es.ad.adp.com/projects/DSCOMMON/repos/ds-generic-components/browse/README.md).


## XML Configuration
--------------------

1. "dbname" and "tablename" are mandatory arguments.

        <table dbname="ditdsbase" tablename="employee_base_monthly" />

   This is the minimum configuration that need to be passed for any dataset.

2. "partitioncolumns" tag can be used to specify additional partition spec that can be used along with temporal partitions present, if any, in the dataset. At present the framework supports max two partiions columns. This is optional.

        <partitioncolumns>source_hr</partitioncolumns>

3. "excludecolumns" tag can be used to exclude any columns of input dataset from computation. This is optional.

        <excludecolumns>hr_job_dsc</excludecolumns>

4. "expr" attribute of "column" tag can be used to define virtual columns.

        <column name="annl_sal_range_low_amt_" expr="CASE WHEN annl_sal_range_low_amt &gt;= 0 THEN annl_sal_range_low_amt END"/>

5. Column level validations can be defined as attributes of "column" tag.

        <column name="full_time_part_time_" valid_values="'PT','FT'" nullable="true" max_null_pctg="10"/>
        <column name="annual_base_" max_value="40000" nullable="false" />
        <column name="rate_type" max_value="100" min_value="0" />

6. "uniquecolumns" tag can be used to define columns list need to be considered to check Unique constraint. This is optional.

        <uniquecolumns>region_code,company_code,product_cd</uniquecolumns>

7. "analyzecolumns" is newly introduced to include columns mentioned for stats collection. Further columns added as virtual columns or with validation will also be a part of analysis. Optional.

## Examples
-------------------


* Sample XML configuration file:

		<?xml version="1.0" encoding="UTF-8"?>
                <tables>

                <table dbname="ditdsbase" tablename="client_master" /> 

                <table dbname="ditdsbase" tablename="employee_base_monthly">
                        <partitioncolumns>source_hr</partitioncolumns>
                        <analyzecolumns>employee_guid,status</analyzecolumns>
                        <column name="eff_status_" expr="CASE WHEN status_ is not null THEN status_ END" max_null_pctg="10"/>
                        <column name="full_time_part_time_" valid_values="'PT','FT'" nullable="true" max_null_pctg="10"/>
                        <column name="annual_base_" max_value="40000" nullable="false" />
                        <column name="rate_type_" max_value="100" min_value="0" max_cardinality="2"/>    
                        <column name="work_state_" valid_values="'CA','GA','WA','NY'" max_null_pctg="20" />
                        <column name="work_zip_crc32" expr="crc32(work_zip_)"/>
                </table>

                <table dbname="ditdsbase" tablename="employee_monthly1" />

                <table dbname="ditdsraw" tablename="optout_2018q4" />

                <table dbname="ditdsraw" tablename="optout_2019q1" >
                        <partitioncolumns>p</partitioncolumns>
                        <uniquecolumns>region_code,company_code,product_cd</uniquecolumns>  
                </table>

                <table dbname="ditdsmain" tablename="employee_monthly_facts" >
                        <column name="bonus_" simple_deseason_detrend_metrics="MEAN:-2:2, NUM_MISSING" />
                        <column name="total_overtime_pay_" simple_deseason_detrend_metrics="MEAN" />
                </table>
                
                <table dbname="ditdsraw" tablename="ev4_dim_salaryplan">
                        <excludecolumns>clnt_obj_id</excludecolumns>
                        <column name="annl_sal_range_low_amt_" expr="CASE WHEN annl_sal_range_low_amt &gt;= 0 THEN annl_sal_range_low_amt END"/>
                        <column name="clnt_obj_id_" expr="crc32(clnt_obj_id)"/>
                </table>

                </tables>

