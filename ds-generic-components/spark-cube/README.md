# Main - Spark Cube

## Overview
-------------------

This is a general purpose reusable component which can be used for configuration driven Cube/Rollup builds using Spark. 
    
  

## Maintainers
-------------------
 - Manoj Oleti - manoj.oleti@adp.com  
 - Pilla Venkataramana - venkataramana.pilla@adp.com
    
  
  
## How to use
-------------------
### Downloading

The latest stable version is 2.0 (compiled against Spark 1.6). It can be downloaded from [Nexus](http://cdladpinexus01:8081/nexus/content/groups/public/com/adp/datacloud/ds/spark-cube/2.0/spark-cube-2.0.jar)

### Usage

Below is the invocation command. The spark parameters listed below are indicative only and may need to be tuned depending on the size of dataset. The "docs" folder contains slides providing an overview of the framework.

**Note**: Angular braces denote mandatory parameters and square braces denote optional parameters 

**Note**: If using SQL file as input, ensure that the column selections (or aliases) are all specified in "lower case". This is important because internally the inputs is represented as a spark dataframe and can cause incorrect results/failures if incorrect case is specified.

	safe_call spark-submit --class com.adp.datacloud.ds.cubeBuilder \
            --master yarn-cluster \
            --driver-memory 5g \
            --num-executors 20 \
            --executor-cores 3 \
            --executor-memory 12G \
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
            --conf spark.shuffle.blockTransferService=nio \
            --conf spark.yarn.executor.memoryOverhead=2400 \
            --conf spark.yarn.driver.memoryOverhead=4800 \
            --conf spark.rdd.compress=true \
            --conf spark.akka.frameSize=50 \
            --conf spark.network.timeout=480 \
            --files "<cube.xml>[,optional_input_sql_file.sql]" \
            spark-cube-3.0.jar <options...>

    
The cubing process accepts multiple options to enable users to configure its behavior.

	CubeBuilder v3.1
	Usage: CubeBuilder [options]
	
	  -n, --application-name <value>
	                           Name of the application.
	  -x, --xml-config-file <value>
	                           XML config file relative path from current working directory. Mandatory
	  -i, --input-spec <value>
	                           Input Either of 1) Fully qualified table or 2) Sql file or 3) Fully qualified HDFS path
	  -l, --local-parquet-input-mode <value>
	                           Treat the inputs and outputs as local/hdfs paths to parquet files. Defaults to false. If set to true, the input-partitions-* parameters are ignored
	  -o, --output-db-name <value>
	                           Hive Database name where output tables should be saved. Ignored if hdfs-output-path is provided
	  -t, --output-partition-spec <value>
	                           Output Partition Spec (column or column=value). Optional. Example pp=2017q1
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
	  --cube-depth <value>     Maximum Cube depth (non-null dimensions in each cell). Optional. When set, all cube definitions in the XML input should contain same number of dimensions
	  -w, --warn-and-exit <value>
	                           Warn about possible issues prior to building and exit. This adds an pre-step to scan dimensions in the configuration. Defaults to true
	  -s, --save-format <value>
	                           Output Table format. Supported values are 'parquet' and 'text' 
	  --help                   Prints usage
	  
	ADP Cube Builder Framework. 
    

The above usage can be displayed by invoking the CubeBuilder with --help option.

Example Invocation from Turnover Probability - [here](https://bitbucket.es.ad.adp.com/projects/DSMAIN/repos/turnoverprobability/browse/cook/shell/benchmarks/build_tp_cube.sh#54-68).
    
    
### Developer Environment Setup

In addition to the [Developer Environment Setup](https://bitbucket.es.ad.adp.com/projects/DSCOMMON/repos/ds-generic-components/browse/README.md) instructions provided in the parent repo (ds-generic-components), please also copy the folder **src/main/resources/t\_dim\_day\_rollup** to **/tmp** in your workstation. This is necessary for the test cases to run locally.

## Concepts
-------------------

1. **Fact**. An aggregatable measure to be computed at each cell in the cube. 

1. **Dimension**. An axis for analyzing measures. Dimensions are of two types

	* **Simple Dimensions**. These are standalone dimensions of fixed cardinality.
	* **Hierarchical Dimensions**. These are organized as a hierarchical structure where any lower level dimension is always qualified by the parent dimension in analysis. **Eg:** (Country, State) or (Year, Quarter, Month) or (NAICS2, NAICS6) etc..
	* Further, dimensions can be qualified with the below three attributes
		* mandatory = "true". Indicates that all cells in the cube should have a non-null value for this dimension. Defaults to false
		* max-depth = "true". Indicates that this dimension can be non-null in cube cells upto a given depth.
		* xor-group = "*any\_group\_name*". This is to apply a restriction based on multiple dimensions sharing the same group name. Any cube cell can contain a maximum of 1 non-null such dimensions.

1. **Aggregation**. The aggregation applied to the fact. These can be either simple aggregations or two-level aggreatations. 

	* **One-level Aggregations**. SUM, MAX, MIN, PERCENT, AVG, COUNT, APPROX\_COUNT\_DISTINCT, COUNT\_DISTINCT, NUM\_MISSING, NUM\_NON\_MISSING, HIVE\_PERCENTILE, PERCENTILE\_APPROX
	* **Two-level Aggregations**. PERCENTILE\_OF\_PERCENTAGES, COUNT\_DISTINCT\_PARTITIONED

1. **Cube**. Refers to a multidimensional method of aggregating measures. At a minimum, each cube must have a name, the set of dimensions/hierarchies it is built with, and the set of measures(or facts contained). As opposed to traditional datawarehouse cubes where all possible combinations of dimensions are stored, our definition is also extended to include rollups, grouping sets of combinations.

	Cubes can be of two types.
	
	* **Simple Cubes.**
		* These are cubes which consists of single-level aggregations.
	* **Partitioned Cubes.**
		* These are two level cubes where the first level is used to build the fact to be measured using a **partition spec**. The second level does the actual aggregation. 
		* These are typically used if the fact to be measured is to be first derived from a categorical column.
		* **Eg:** "Median of regular employee percentage". Given an employee level dataset as input, in the first level, the "percentage of regular employees" will be computed for a given client(defined by partition spec). And in the second level the actual aggregation (in this case, median) is computed.

1. **Grouping Set**. These allow to group by multiple user-specified combination of dimensions (instead of all possible combinations of dimensions). In Spark, cubes are just syntactic sugar and are implemented under-the-hood using grouping sets.

> **Note**: All aggregations in any given cube can be either simple aggregations OR two-level aggregations. It is not possible to create a cube which contains both these types of aggregations.   
     


## Basic Configuration
-------------------

The two mandatory inputs needed for building cubes are 

* Input Dataset
	* Provided by a Hive Table or a Hive SQL.
* Cube Definition 
	* Provided via an XML Configuration file.
	* Can contain definitions for a single cube.
	* Defines the facts/dimensions/pivots, postprocessing logic etc.. for the cube.

### Advanced Aggregation Types.

This section explains the functionalities of the advanced aggregation types available in this framework.

1. **APPROX\_COUNT\_DISTINCT** : Uses Spark's approxCountDistinct. This method should be preferred over simple COUNT\_DISTINCT for those columns where cardinality is expected to be high (in order of tens of thousands or more).
2. **COUNT\_DISTINCT\_PARTITIONED** : Two level aggregation which uses APPROX\_COUNT\_DISTINCT within partitions and sums them up in the second level.
3. **HIVE\_PERCENTILE** : Approximate percentile computation using Hive's percentile\_approx function. Under-the-hood uses Ben-Haim & Tom's Approximate percentile method
4. **PERCENTILE\_APPROX** : Same as the HIVE\_PERCENTILE function above, with the difference that this uses a spark-udf which re-implements the same functionality as Hive's percentile\_approx method. This method was developed because Spark 1.6 had a bug which break's inter-operability when Hive-UDFs and COUNT\_DISTINCTs are used in the same configuration.
5. **PERCENTILE\_OF\_PERCENTAGES** : Two level aggregation which computes percentage of a categorical value within a partition, and computes a percentile of this percentage across multiple partitions.


## Advanced Topics
-------------------

* **Spark 1.6**
	* COUNT\_DISTINCT and HIVE\_PERCENTILE facts cannot co-exist because of a [spark defect](https://issues.apache.org/jira/browse/SPARK-18137). If you must use such a configuration, separate out these facts into two different cube definitions and join them back using SQL once the cubes are built.  

* **Spark 2.0**
	* PERCENTILE\_APPROX's underlying UDAF shall be replaced in Spark 2.0 because there is native support for quantiles in this release.

* **Best Practices & Some Good Karma**
	* Use reasonable number of executors and memory per executor while building cubes.
	* Using *"--num-shuffle-partitions"* can improve parallelism but setting it to too high a number will result in very small partitions being created. The default value of 200 is usually sufficient.
	* Use *"--distribute-by"* if you know that the data is quite uniformly distributed along the column that you wish to distribute the data by. By default, spark uses a default *org.apache.spark.HashPartitioner* to ensure that the data is uniformly distributed. However, in certain cases if we know ahead of time that the data is uniformly distributed along a column, then using it as the distribute-by column can yield better performance. On the flip-side, using this switch with a column which doesn't have a uniform distribution of data will cause the entire process to slow down (and in some cases run out of memory). 

* **General Performance Tips**
	* Avoid using COUNT\_DISTINCT on extremely high cardinality columns (eg, above 100000). One such example is "pers\_obj\_id". Using a COUNT\_DISTINCT on this column will make the cube build crawl. There are two ways to handle this problem
		* 	Input Preparation. Adjust the grain of your table such that you get a unique record for each pers\_obj\_id. (This is the way the benchmark_core is created)  
		*  Use APPROX\_COUNT\_DISTINCT instead of COUNT_DISTINCT
		

## Examples
-------------------


* Simple Cube with a filter on input, and an aggregated measure

		<cubes>
			<!-- The cube name becomes the table name in hive -->
			<cube>
				<name>simple_cube_with_filter</name>
				<dimensions>
					<dimension mandatory="true">l2_code</dimension>
					<dimension max-depth="2">country</dimension>
					<dimension xor-group-name="myxorgroup">job_title</dimension>
					<dimension xor-group-name="myxorgroup">pay_group</dimension>
				</dimensions>
				<facts>
					<fact>
						<name>average_value</name>
						<column>value</column>
						<when>tenure_days &lt 365</when> <!-- Any additional conditional criteria to be applied for computation. When this criterial is not met -->
						<aggregation>
							<type>AVG</type>
							<default>50</default> <!-- Nulls in "facts" will be replaced with this value before aggregation, if specified -->
						</aggregation>
					</fact>
				</facts>
				<postprocess> <!-- This entire section is optional. -->
					<column> <!-- Used for defining new columns once the cube is built -->
						<name>percentage_value</name>
						<expr>average_value * 100</expr>
					</column>
					<filters>  <!-- Used to filter cube by certain criteria -->
						<filter>empl_count &gt 10</filter>
					</filters>
				</postprocess>
			</cube>
		</cubes>


* Partitioned Cube

		<cubes>
			<!-- The cube name becomes the table name in hive -->
			<cube>
				<name>partitioned_cube</name>
				<dimensions>
					<dimension>l2_code</dimension>
				</dimensions>
				<type>PARTITIONED</type>
				<partitionspec>
					<dimensions>
						<dimension>clnt_obj_id</dimension>
					</dimensions>
				</partitionspec>
				<facts>
					<fact>
						<name>reg_emp_pctg</name>
						<column>reg_temp_dsc</column>
						<matches>REGULAR</matches>
						<aggregation>
							<type>PERCENTILE_OF_PERCENTAGES</type>
							<arguments>
								<argument>30,90</argument>
							</arguments>
						</aggregation>
					</fact>
				</facts>
			</cube>
		</cubes>


* Simple Cube with hierarchical dimension and inclusion ranges applied for measures. 

		<cubes>
			<!-- The cube name becomes the table name in hive -->
			<cube>
				<name>hierarchy_example</name>
				<dimensions>
					<dimension>l2_code</dimension>
					<hierarchy>
						<dimension>country</dimension>
						<dimension>state</dimension>
					</hierarchy>
				</dimensions>
				<facts>
					<fact>
						<name>salary_hive_percentiles</name> <!-- This becomes an array column in output -->
						<column>salary</column>
						<inclusionrange>0,200</inclusionrange>
						<aggregation>
							<type>HIVE_PERCENTILE</type> <!-- Uses Hive UDAF "PERCENTILE" -->
							<arguments>
								<argument>30,90</argument> <!-- percentiles to be computed -->
								<argument>10000</argument> <!-- This parameter controls approximation accuracy at the cost of memory. Higher values yield better approximations, default is 10,000 -->
							</arguments>
						</aggregation>
					</fact>
					<fact>
						<name>salary_percentiles</name> 
						<column>salary</column>
						<inclusionrange>0,200</inclusionrange>
						<aggregation>
							<type>HIVE_PERCENTILE</type> <!-- Uses Spark Custom UDAF "PERCENTILE" -->
							<arguments>
								<argument>30,90</argument> <!-- percentiles to be computed -->
								<argument>10000</argument> <!-- This parameter controls approximation accuracy at the cost of memory. Higher values yield better approximations, default is 10,000 -->
								<argument>true</argument> <!-- Rounds to nearest integer. Can improve performance considerably by trading accuracy. Will round to one decimal place even when false. -->
							</arguments>
						</aggregation>
					</fact>
				</facts>
			</cube>
		</cubes>



* Cube using custom grouping sets. 

		<cubes>
			<cube>
				<name>groupingsets_example</name>
				
				<groupingsets>
					<groupingset>clnt_obj_id</groupingset>
					<groupingset>d_full_tm_part_tm_cd, d_pay_rt_type_cd</groupingset>
					<groupingset>d_job_cd</groupingset>
				</groupingsets>
				
				<addons>
					<supportcolumns>
						<column>f_work_asgnmt_stus_cd</column>
					</supportcolumns>
				</addons>
		
				<facts>
					<fact>
						<name>num_person_days</name>
						<column>eff_person_days</column>
						<when>f_work_asgnmt_stus_cd in ('P','A','L','S')</when>
						<aggregation>
							<type>SUM</type>
						</aggregation>
					</fact>
				</facts>
				
			</cube>
		</cubes>


* Cube with restricted depth, and with optional post-processing. 

		<?xml version="1.0" encoding="UTF-8"?>
		<cubes>
			<cube>
				<name>depth_restricted_cube</name>
				
				<depth>3</depth> <!-- Cube depth excludes pivots -->
		
				<dimensions>
					<hierarchy>
						<dimension>yr_cd</dimension> <!-- Only the top level dimension in hierarchy is considered while computing depth, as it should be -->
						<dimension>qtr_cd</dimension>
						<dimension>mnth_cd</dimension>
					</hierarchy>
					<dimension>d_full_tm_part_tm_cd</dimension>
					<dimension>d_pay_rt_type_cd</dimension>
					<dimension>d_job_cd</dimension>
					<dimension>d_cmpny_cd</dimension>
					<dimension>d_hr_orgn_id</dimension>
					<dimension>d_eeo_ethncty_clsfn_dsc</dimension>
					<dimension>d_reg_temp_dsc</dimension>
					<dimension>d_flsa_stus_dsc</dimension>
					<dimension>d_work_state_cd</dimension>
					<dimension>d_gndr_cd</dimension>
				</dimensions>
				
				<facts>
		
					.... 
		
				</facts>
		
				<postprocess>
					<!-- Composite Facts -->
					<column>
						<name>annualized_turnover_rt</name>
						<expr>num_terminations / (num_person_days / sys_calendar_days)</expr>
						<!-- sys_calendar_days is a built-in keyword that represents the number of calendar days in a given period. Cube should have one or more "time dimensions" to make use of this keyword - yr_cd, qtr_cd, mnth_cd to use this keyword.-->
					</column>
					<column>
						<name>annualized_voln_turnover_rt</name>
						<expr>num_voluntary_terminations / (num_person_days / sys_calendar_days)</expr>
					</column>
					<column>
						<name>internal_mobility_rate</name>
						<expr>(num_demotions + num_promotions + num_transfers)/(num_hires + num_terminations + num_rehires + num_demotions + num_promotions + num_transfers)</expr>
					</column>
		
					<!-- Optional filters to cut down size of the cube -->
					<filters>
						<filter>yr_cd is not null</filter>
					</filters>
				</postprocess>
		
			</cube>
		
		</cubes> 
