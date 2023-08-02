# Imports
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import pyspark
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName('testDBconnection') \
    .config("spark.jars",".\jars\mysql-connector-java-8.0.13.jar") \
    .getOrCreate()

# Create DataFrame
columns = ["id","name","age","gender"]
dummyData =[(1,"vikas",30,"M"),(2,"vipin Jain",40,"M"),(3,"Amit",44,"M"),(4,"Shayama",44,"F"), \
    (5,"anupama",44,"F"), \
            (6,"Harish",44,"M"), \
            (7,"Ranjeet",34,"M")
            ]
sampleDf =spark.sparkContext.parallelize(dummyData).toDF(columns)

# Write to MySwl Table
sampleDf.write \
  .format("jdbc") \
  .option("driver","com.mysql.cj.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/testvd?useSSL=false") \
  .option("dbtable", "employee") \
    .mode("overwrite") \
  .option("user", "root") \
  .option("password", "root") \
  .save()


# Read from MySQL Table
df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/testvd?useSSL=false") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "root") \
    .load()


df.show()
print("========================== Reading from City from world database =======================================")
df_worldCity = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/world?useSSL=false") \
    .option("dbtable", "city") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

df_worldCity.show()
print("========================== Reading from country from world database =======================================")
df_worldCountry = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/world?useSSL=false") \
    .option("dbtable", "country") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

df_worldCountry.show()

print("======================= joins data set ================================")
DF_country_city = df_worldCity.join(df_worldCountry,df_worldCity.CountryCode == df_worldCountry.Code , "inner" ) #\.show(truncate=False)
DF_country_city1 = DF_country_city.toDF(*[val + str(i) for i, val in enumerate(DF_country_city.columns)])
#DF_country_city1.printSchema()
DF_country_city1.select(DF_country_city1["Name1"].alias('CityName'),DF_country_city1["Name6"].alias('CountryName')).show()