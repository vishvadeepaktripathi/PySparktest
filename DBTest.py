# Imports
import os
import sys
# import pyspark
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# Create SparkSession
spark = SparkSession.builder \
    .appName('test_dbconnection') \
    .config("spark.jars", "./jars/mysql-connector-java-8.0.13.jar").getOrCreate()

# Create DataFrame
columns = ["id", "name", "age", "gender"]
dummyData = [(1, "vikas", 30, "M"), (2, "siting Jain", 40, "M"), (3, "Amit", 44, "M"), (4, "Shyam", 44, "F"),
             (5, "anupama", 44, "F"), (6, "Harish", 44, "M"), (7, "Ranjeet", 34, "M")
             ]
sampleDf = spark.sparkContext.parallelize(dummyData).toDF(columns)
sampleDf.show()

# Write to MySwl Table
sampleDf.write \
    .format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/testvd?useSSL=false") \
    .option("dbtable", "employee") \
    .mode("overwrite") \
    .option("user", "root") \
    .option("password", "root") \
    .save()
# Read from MySQL Table
df = spark.read \
    .format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/testvd?useSSL=false") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

df.show()
print("========================== Reading from City from world database =======================================")
df_worldCity = spark.read \
    .format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/world?useSSL=false") \
    .option("dbtable", "city") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

df_worldCity.show()
print("========================== Reading from country from world database =======================================")
df_worldCountry = spark.read \
    .format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/world?useSSL=false") \
    .option("dbtable", "country") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

df_worldCountry.show()

print("======================= joins data set ================================")
DF_country_city = df_worldCity.join(df_worldCountry, df_worldCity.CountryCode == df_worldCountry.Code,
                                    "inner")  # \.show(truncate=False)
DF_country_city1 = DF_country_city.toDF(*[val + str(i) for i, val in enumerate(DF_country_city.columns)])
# print("======================= Joined Schema ================================")
# DF_country_city1.printSchema()
DF_country_city1.select(DF_country_city1["Name1"].alias('CityName'),
                        DF_country_city1["Name6"].alias('CountryName')).show()
DF_join_countery_city = DF_country_city1.select(DF_country_city1["Name1"].alias('CityName'),
                        DF_country_city1["Name6"].alias('CountryName'))
DF_join_countery_city.show()
