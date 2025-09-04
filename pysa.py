import typing
import io
import re

if not hasattr(typing, "io"):
    typing.io = io
if not hasattr(typing, "re"):
    typing.re = re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max, sum, desc, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.master("local").appName("bjk").getOrCreate() 

data = [(1, "A", 100), (2, "B", 200), (3, "A", 300), (4, "C", 400)]
columns = [ "id" , "category" , "amount"]

df = spark.createDataFrame(data,columns)

df.createOrReplaceTempView("table")

fdf = df.filter(col("amount") > 100)

filterdf = spark.sql("""select * from table where amount > 100""")

fdf.show()

filterdf.show()

gdf =  df.groupBy("category").agg(
	count("*").alias("count"),
	max("amount").alias("max_col"),
	sum("amount").alias("summ_col"))

max_df = spark.sql("""select category , count(*) from table group by category""")

max_df.show()

gdf.show()

windowspec = Window.partitionBy("category").orderBy(desc("amount"))

windf = df.withColumn("rnk" , row_number().over(windowspec))

windf.show()

spark.stop()

	

