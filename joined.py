from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg , when , col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GroupBy Aggregation Example") \
    .getOrCreate()

data = [(1,"ravi") ,(2,"Venkat"), (3,"kiran")]
col = ("id" , "name")

df1 = spark.createDataFrame(data , col)

data_1 = [(1,88) ,(2,92) ,(3,78) ]
col_1 = ("id" ,"marks")

df2 = spark.createDataFrame(data_1 , col_1)

joineddf = df1.join(df2 , on = "id" , how = "inner")

joineddf.show()

spark.stop()
