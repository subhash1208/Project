# Databricks notebook source
# Databricks notebook source
from pyspark.sql.functions import *
import unittest,io
import datetime
from unittest.runner import TextTestResult
import sys
import json
import logging
import ast

# COMMAND ----------

db_name = dbutils.widgets.get('db_name')
table_name = dbutils.widgets.get('table_name')
expected_cols = ast.literal_eval(dbutils.widgets.get('expected_cols'))

print('DB Name: {}, Table Name: {}, Expected_Cols: {}'.format(db_name,table_name,expected_cols))

# COMMAND ----------

class TextTestResults(TextTestResult):
    def __init__(self, *args, **kwargs):
        super(TextTestResults, self).__init__(*args, **kwargs)
        self.successes = []
        self.failures = []
    def addSuccess(self, test):
        super(TextTestResults, self).addSuccess(test)
        self.successes.append(test)
    def addFailures(self, test):
        super(TextTestResults, self).addFailure(test)
        self.failures.append(test)
    

# COMMAND ----------

# Validate if 2 lists are identical
def check_identical_lists(l1,l2):
  print(l1)
  print(l2)
  l1 = [i.lower().strip() for i in l1]
  l2 = [i.lower().strip() for i in l2]
  if len(l1) != len(l2):
      return False
  for i in l1:
      if i not in l2:
          return False
  return True

# COMMAND ----------

class ValidateEMIUserDataInTable(unittest.TestCase):
    
    def setUp(self):
        pass
    
    # Test if all the required columns are present in the table
    def test_required_columns_available(self):
        query = 'select * from {}.{}'.format(db_name,table_name)
        logging.info("Test: test_validate_columns, Query: {}".format(query))
        table_sdf = spark.sql(query)
        actual_cols = table_sdf.columns
        self.assertTrue(check_identical_lists(expected_cols, actual_cols), 'Required columns {c} are not present in the table {t}'.format(c=expected_cols, t=table_name))
    
    # Test the table is not empty
    def test_table_not_empty(self):
        query = 'select * from {}.{}'.format(db_name,table_name)
        logging.info("Test: test_validate_columns, Query: {}".format(query))
        table_sdf = spark.sql(query)
        self.assertTrue(table_sdf.count() > 0, "Table {t} is Empty".format(t=table_name))

# COMMAND ----------

if __name__ == '__main__':
    test_classes_to_run = [
                         ValidateEMIUserDataInTable
                        ]

    suites_list = []
    test_case_result=[]
    curr_ts =  (datetime.datetime.now())

    for test_class in test_classes_to_run:
        suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
        suites_list.append(suite)

    run_suite = unittest.TestSuite(suites_list)
    mystream = io.StringIO()

    #   Run the tests and store the result in mystream instead of the default sys.out
    myTestResult = unittest.TextTestRunner(stream=mystream,verbosity=3,resultclass=TextTestResults).run(run_suite)
    
    #   Store the value in testoutput. This can be returned to the calling program / written to external file .
    testoutput = mystream.getvalue()
    print(testoutput)

# COMMAND ----------

print(testoutput)

# COMMAND ----------

# Assert that total number of failures should be equal to 0

assert len(myTestResult.failures) == 0,"Unit Tests have failed. Please check the above cell for more details"
