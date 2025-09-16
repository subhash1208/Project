CREATE TABLE IF NOT EXISTS cdldsraw2.emp_data LIKE cdldsraw1.emp_data;
CREATE TABLE IF NOT EXISTS cdldsraw2.emp_data_hive LIKE cdldsraw1.emp_data_hive;
CREATE TABLE IF NOT EXISTS cdldsraw2.emp_data_text LIKE cdldsraw1.emp_data_text;
CREATE TABLE IF NOT EXISTS cdldsraw2.emp_data_text_hive LIKE cdldsraw1.emp_data_text_hive;

-- CTAS TEST
CREATE TABLE IF NOT EXISTS cdldsraw2.emp_data_plain AS
SELECT *
FROM cdldsraw1.emp_data_plain;

CREATE TABLE IF NOT EXISTS cdldsraw2.emp_data_plain2
USING parquet AS
SELECT *
FROM cdldsraw1.emp_data_plain;

CREATE TABLE IF NOT EXISTS cdldsraw2.emp_data_plain3 stored AS parquet AS
SELECT *
FROM cdldsraw1.emp_data_plain;


-- TEXT TABLE TEST
CREATE TABLE IF NOT EXISTS cdldsraw2.emp_data_plain_delim ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' NULL DEFINED AS ''
TBLPROPERTIES('serialization.null.format'='') AS
SELECT *
FROM cdldsraw1.emp_data_plain;

INSERT overwrite TABLE cdldsraw2.emp_data partition(designation,salary)
SELECT *
FROM cdldsraw1.emp_data;

msck repair TABLE cdldsraw2.emp_data;

-- INSERT Into with dynamic partitioning Test
INSERT overwrite TABLE cdldsraw2.emp_data partition(designation,salary)
SELECT *
FROM cdldsraw1.emp_data distribute by Ooid,Age;

INSERT overwrite TABLE cdldsraw2.emp_data partition(designation,salary)
SELECT *
FROM cdldsraw1.emp_data cluster by Ooid,Age;

INSERT overwrite TABLE cdldsraw2.emp_data partition(designation,salary)
SELECT *
FROM
    (SELECT *
    FROM cdldsraw1.emp_data distribute by Ooid,Age);

INSERT overwrite TABLE cdldsraw2.emp_data partition(designation,salary)
SELECT *
FROM cdldsraw1.emp_data cluster by Ooid,Age;

INSERT overwrite TABLE cdldsraw2.emp_data_hive partition(designation,salary)
SELECT *
FROM cdldsraw1.emp_data cluster by Ooid,Age;



INSERT overwrite TABLE cdldsraw2.emp_data_text
SELECT *
FROM cdldsraw1.emp_data_text;

-- external table creation and partition recovery test
DROP TABLE IF EXISTS cdldsraw2.emp_data_ext;

CREATE TABLE IF NOT EXISTS cdldsraw2.emp_data_ext (
	age int, 
	name string, 
	ooid string, 
	designation string, 
	salary int
)   USING parquet partitioned by (designation,salary) 
Location '/tmp/spark-warehouse/cdldsraw2.db/emp_data'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

SHOW PARTITIONS cdldsraw2.emp_data_ext;

MSCK REPAIR TABLE cdldsraw2.emp_data_ext;

SHOW PARTITIONS cdldsraw2.emp_data_ext;

-- INSERT Into External table
INSERT overwrite TABLE cdldsraw2.emp_data_ext partition(designation,salary)
SELECT age,
       name,
       CASE
           WHEN (ooid IS NOT NULL) THEN ooid
           ELSE "noName"
       END,
       designation,
       salary
FROM cdldsraw1.emp_data;

truncate table cdldsraw2.emp_data;

-- INSERT Into with static partitioning Test
INSERT overwrite TABLE cdldsraw2.emp_data partition(designation='batman',salary=1000)
SELECT age,name,ooid
FROM cdldsraw1.emp_data cluster by Ooid,Age;

INSERT overwrite TABLE cdldsraw2.emp_data_hive partition(designation='bruce',salary=2000)
SELECT age,name,ooid
FROM cdldsraw1.emp_data cluster by Ooid,Age;

-- INSERT Into with mixed partitioning Test

INSERT overwrite TABLE cdldsraw2.emp_data partition(designation='batman',salary)
SELECT age,name,ooid,salary
FROM cdldsraw1.emp_data cluster by Ooid,Age;


-- -- wrong partition test/ wrong number of select columns / negitive test

-- -- INSERT overwrite TABLE cdldsraw2.emp_data partition(designation,salary=1000)
-- -- SELECT age,name,ooid,designation
-- -- FROM cdldsraw1.emp_data cluster by Ooid,Age;

-- -- INSERT overwrite TABLE cdldsraw2.emp_data_hive partition(Ooid,Age)
-- -- SELECT *
-- -- FROM cdldsraw1.emp_data_hive cluster by Ooid,Age;

-- -- INSERT overwrite TABLE cdldsraw2.emp_data partition(designation='batman',salary=1000)
-- -- SELECT age,name,ooid,designation,salary
-- -- FROM cdldsraw1.emp_data cluster by Ooid,Age;