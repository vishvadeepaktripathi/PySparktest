# Imports
# to set the pyspark python path to remove the other dependencies
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pyspark
import logging
import unittest
from pyspark.sql import SparkSession

def suppress_py4j_logging():
 logger = logging.getLogger('py4j')
 logger.setLevel(logging.WARN)

def create_testing_pyspark_session():
 return (SparkSession.builder
 .master('local[2]')
 .appName('my-local-testing-pyspark-context')
 .enableHiveSupport()
 .getOrCreate()
    ) #return close

