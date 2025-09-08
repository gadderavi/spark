from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number, desc , avg
from pyspark.sql.window import Window
from pyspark.sql.functions import when
from typing.io import BinaryIO 

spark = SparkSession.builder.master("local").appName("pivot").getOrCreate()


data = [
    (1, "Alice",   "IT",    "East",  500, "2024-01-01"),
    (2, "Bob",     "HR",    "West",  300, "2024-01-02"),
    (3, "Charlie", "IT",    "East",  None,"2024-01-03"),
    (4, "David",   "IT",    "West",  400, "2024-01-04"),
    (5, "Eve",     "HR",    None,    250, "2024-01-05"),
    (6, "Frank",   "Sales", "East",  700, "2024-01-06"),
    (7, "Grace",   "Sales", "West",  800, "2024-01-07"),
    (8, "Henry",   "IT",    "East",  600, "2024-01-08")
]

column = [ "emp_id" , "emp_name" , "dept" , "region" , "sales" , "date" ]

df = spark.createDataFrame( data , column)

fildf = df.filter(col("sales") > 300)

fildf.show()

dropdf = fildf.dropna()

dropdf.show()

dropdf1 = fildf.fillna({"sales" : 0})

dropdf1.show()

groupdf = dropdf1.groupBy("dept").agg(sum("sales").alias("tot_sa") , avg("sales").alias("avg_sale"))

groupdf.show()

groupdf1 = dropdf1.groupby("dept").pivot("region").agg(sum("sales").alias("values"))

groupdf1.show()

window_spec = Window.partitionBy("dept").orderBy(desc("sales"))

windf = dropdf1.withColumn("rnk" , row_number().over(window_spec))

windf.show()

spark.stop()
