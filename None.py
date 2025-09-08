from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg , when , col

# Initialize Spark sessio
spark = SparkSession.builder.master("local").appName("null").getOrCreate()

# Sample data
data = [("Alice", "Math", 85),
        (None, "Math", 90),
        ("Alice", "English", 78),
        ("Bob", None, 82),
        ("Charlie", "Math", None),
		("Charlie" , "English" , 95),
		(None , None, None)]

columns = ["name", "subject", "score"]

df = spark.createDataFrame(data , columns)

dfna = df.dropna()

dfna.show()


dfna1 = df.dropna(how = "all")

dfna1.show()

fillna = df.fillna({"name" : "unknown" , "subject" : "unknown" , "score" : 0})

fillna.show()

spark.stop()

 

