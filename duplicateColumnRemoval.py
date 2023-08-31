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