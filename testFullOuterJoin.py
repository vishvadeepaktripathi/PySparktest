# Imports
import os
import sys
from msilib import schema

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce,lit

# Create SparkSession
spark = SparkSession.builder\
    .appName("fullouterjoin").getOrCreate()

# Create DataFrame
columns = ["dpid","yr_month","cnt"]
dummyData =[(1,"201204",10),(2,"201205",20)]
empDF = spark.createDataFrame(data=dummyData, schema = columns)
empDF.printSchema()
empDF.show(truncate=False)

print("================== test second table ==================")
#dummyData2 =[(None,None,None)] #,(2,"201205",20)

#deptColumns = ["dpid1","yr_month1","cnt1"]
#deptDF = spark.createDataFrame(data=dummyData2, schema = deptColumns)
#deptDF = spark.createDataFrame([], schema=deptColumns).dtypes
deptDF = spark.createDataFrame([], 'dpid1 INT,yr_month1 String,cnt1 INT')
deptDF.dtypes
deptDF.printSchema()
deptDF.show(truncate=False)


print("================== Full outer start ==================")
deptDF.join(empDF,empDF.dpid == deptDF.dpid1 , "full" ).show(truncate=False)