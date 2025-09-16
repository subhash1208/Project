# Databricks notebook source
# Databricks notebook source
from pyspark.sql.functions import *
import unittest,io
import datetime
from unittest.runner import TextTestResult
import sys
import json

GREEN_MAIN_DB = dbutils.widgets.get("GREEN_MAIN_DB")
BLUE_MAIN_DB = dbutils.widgets.get("BLUE_MAIN_DB")

# COMMAND ----------

class EMITestValidation(TextTestResult):
  def __init__(self, *args, **kwargs):
    super(EMITestValidation, self).__init__(*args, **kwargs)
    self.successes = []
  def addSuccess(self, test):
    super(EMITestValidation, self).addSuccess(test)
    self.successes.append(test)

# COMMAND ----------

class EMITestMethods(unittest.TestCase):
    
    def setUp(self):
          pass
    
    def test_ins_hash_val_count(self):
      # A count check to check total number of records in T_FACT_INS_STAGING & T_FACT_INS
      t_fact_ins_staging = spark.sql(f"select count(distinct ins_hash_val) as cnt from {BLUE_MAIN_DB}.T_FACT_INS_STAGING").collect()[0][0]
      t_fact_ins = spark.sql(f"select count(distinct ins_hash_val) as cnt from {BLUE_MAIN_DB}.T_FACT_INS").collect()[0][0]
      self.assertTrue(t_fact_ins_staging == t_fact_ins,'Records counts not matched for T_FACT_INS_STAGING & T_FACT_INS')
    
    def test_ins_hash_val_ins_scor_count(self):
      # A count check to check total number of records when ins_scor is not null in T_FACT_INS_STAGING & T_FACT_INS
      t_fact_ins_staging = spark.sql(f"select count(distinct ins_hash_val) as cnt from {BLUE_MAIN_DB}.T_FACT_INS_STAGING where ins_scor is not null").collect()[0][0]
      t_fact_ins = spark.sql(f"select count(distinct ins_hash_val) from {BLUE_MAIN_DB}.T_FACT_INS where ins_scor is not null").collect()[0][0]
      self.assertTrue(t_fact_ins_staging == t_fact_ins,'Records counts not matched for T_FACT_INS_STAGING & T_FACT_INS')
    
    def test_ins_scor_100(self):
      # A count check to check total number of records when ins_scor is not null in T_FACT_INS_STAGING & T_FACT_INS
      t_fact_ins = spark.sql(f"select distinct ins_scor from {BLUE_MAIN_DB}.T_FACT_INS where mtrc_ky = 701").collect()[0][0]
      self.assertTrue(t_fact_ins == 100,'Ins_scor is not 100 in T_FACT_INS')


# COMMAND ----------

if __name__ == '__main__':
      test_classes_to_run = [
                             EMITestMethods
                            ]
  
      suites_list = []
      test_case_result=[]
      curr_ts =  (datetime.datetime.now())
      
      for test_class in test_classes_to_run:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suites_list.append(suite)
        
      run_suite = unittest.TestSuite(suites_list)
      mystream = io.StringIO()
  
      myTestResult = unittest.TextTestRunner(stream=mystream,verbosity=3,resultclass=EMITestValidation).run(run_suite)
      testoutput = mystream.getvalue()
      print(testoutput)
