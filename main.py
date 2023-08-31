from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkFiles
# from pyspark.sql.functions import *



if __name__ == "__main__":
    spark: SparkSession = SparkSession\
        .builder\
        .appName("Names start with H")\
        .master("local[2]")\
        .getOrCreate()
    data_list = [("Harry", "20"),
                 ("Hermoine", "26"),
                 ("Tom", "50"),
                 ("Micquel", "44"),
                 ("Hank", "20.0"),
                 ("Super", "26"),
                 ("Fig", "ABC"),
                 ("Ashley", "-40")]
    df = spark.createDataFrame(data_list).toDF("Name", "Age")
    # df.show()
    df.printSchema()
    df.createOrReplaceTempView("Age")
    # df4 = list(df3)
    # print(df4)
    df2 = spark.sql("select IF(Age<0, Age*-1, Age) as Age from Age")
    df2.printSchema()
    # df2 = spark.sql("select IF(Name='Hank','Ron',Name) as Name,cast(Age as int),IF(IF(Name='Hank','Ron',Name) in ('Harry','Ron'),'M','F') as Gender from Age where Name like 'H%'")
    df2.show()