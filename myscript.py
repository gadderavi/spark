from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GroupBy Aggregation Example") \
    .getOrCreate()

# Sample data
data = [("A", "Math", 80),
        ("A", "Math", 90),
        ("B", "Math", 70),
        ("A", "Physics", 85),
        ("B", "Physics", 60)]

columns = ["class", "subject", "score"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Group by and aggregate
groupdf = df.groupBy("class", "subject") \
    .agg(count("*").alias("count"), avg("score").alias("avgscr"))

groupdf.show()

spark.stop()
