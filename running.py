from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum, avg, count, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import pyspark.sql.functions as F
from pyspark.sql.functions import col


spark = SparkSession.builder.master("local").appName("fcg").getOrCreate()

data = [
    ("U1", "Home",     50, "Mobile",  "2024-01-01 10:00:00"),
    ("U1", "Product",   5, "Mobile",  "2024-01-01 10:05:00"),
    ("U1", "Cart",     30, "Desktop", "2024-01-03 09:00:00"),
    ("U2", "Home",     12, "Mobile",  "2024-01-02 12:00:00"),
    ("U2", "Product",  55, "Desktop", "2024-01-04 15:20:00"),
    ("U2", "Checkout", 18, "Desktop", "2024-01-05 11:10:00"),
    ("U3", "Home",     10, "Desktop", "2024-01-02 13:40:00"),
    ("U3", "Product",  45, "Mobile",  "2024-01-03 14:10:00"),
    ("U3", "Cart",      8, "Mobile",  "2024-01-04 10:30:00")
]

columns = ["user_id", "page", "duration_sec", "device", "date_time"]



df = spark.createDataFrame( data , columns)

filter_df = df.filter(col("duration_sec") >= 15 )	

filter_df.show()

group_df = filter_df.groupBy("user_id").agg(sum("duration_sec").alias("total_duration"),count("page").alias("total_pages"))

group_df.show()

window_spec = Window.partitionBy("user_id").orderBy(col("date_time").asc()	)

main_df = filter_df.withColumn("running_total" , sum("duration_sec").over(window_spec))

main_df.show()

window_spec1 = Window.partitionBy("user_id").orderBy(col("duration_sec").desc())


result_df = filter_df.withColumn("rnk" , row_number().over(window_spec1))


result_df.show()

