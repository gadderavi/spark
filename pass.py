from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg , when , col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GroupBy Aggregation Example") \
    .getOrCreate()


# Sample data
data = [("Alice", "Math", 85),
        ("Bob", "Math", 90),
        ("Alice", "English", 78),
        ("Bob", "English", 82),
        ("Charlie", "Math", 95),
		("Charlie" , "English" , 95)]

columns = ["name", "subject", "score"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

groupdf = df.groupBy("name")\
		.agg(avg("score").alias("avg_scr"))

passdf = groupdf.withColumn("staus" , when(col("avg_scr")> 80 , "pass").otherwise("fail") )

filterdf = passdf.filter(col("staus") == "pass")

filterdf.show()

spark.stop()
