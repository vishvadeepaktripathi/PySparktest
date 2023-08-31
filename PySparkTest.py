# Imports
import os
import sys
from msilib import schema
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#from pyspark.shell import spark
#import pyspark
import logging
import unittest
from pyspark.sql import SparkSession

#from unittest import TestCase

class PySparkTest(unittest.TestCase):
    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.info())

    @classmethod
    def create_testing_pyspark_session(cls):
        spark_session = SparkSession \
            .builder \
            .master('local[2]') \
            .appName('my-local-testing-pyspark-context') \
            .enableHiveSupport() \
            .getOrCreate()
        cls.ss = spark_session

    @classmethod
    def setupClass(cls):
        cls.suppress_py4j_logging()
        cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()