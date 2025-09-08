from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number, desc
from pyspark.sql.window import Window
from pyspark.sql.functions import when

spark = SparkSession.builder.master("local").appName("hf").getOrCreate()

data = [
    ("Alice", "2024-07", 5000),
    ("Alice", "2024-08", 7000),
    ("Bob", "2024-07", None),
    ("Bob", "2024-08", 4500),
    ("Charlie", "2024-07", 6000),
    ("Charlie", "2024-08", None),
    ("Alice", "2024-07", 5000),   # duplicate row
    (None, "2024-08", 3000),      # missing employee name
]
columns = ["employee", "month", "sales"]


df = spark.createDataFrame(data , columns)

df_drop = df.dropDuplicates()

df_drop.show()

df_drop1 = df.dropna(subset =["employee"])
df_drop1.show()

df_drop2 = df_drop1.fillna({"sales" : 0 })
df_drop2.show()

filt_df = df_drop2.filter(col("sales") >= 5000)

filt_df.show()

bonus_df = filt_df.withColumn("bonus", when(col("sales") >= 6000, col("sales")*0.1).otherwise(col("sales")*0.05))
bonus_df.show()

gdf = filt_df.groupBy("employee").agg(sum("sales").alias("tol_sales"))

gdf.show()

wind_spec = Window.partitionBy("employee").orderBy(desc("month"))

windf = filt_df.withColumn("rnk" , row_number().over(wind_spec))

windf.show()

spark.stop()
