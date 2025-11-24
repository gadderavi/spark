from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, sum, avg, when
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import pyspark.sql.functions as F
from pyspark.sql.functions import col


# Create Spark Session
spark = SparkSession.builder.master("local").appName("ProductSalesData").getOrCreate()

# Sample Data
data = [
    (101, "Laptop", "Electronics", 1500, "2025-07-01"),
    (102, "Smartphone", "Electronics", 2000, "2025-07-02"),
    (103, "T-Shirt", "Clothing", 500, "2025-07-01"),
    (104, "Jeans", "Clothing", 800, "2025-07-03"),
    (105, "Bread", "Grocery", 100, "2025-07-02"),
    (106, "Milk", "Grocery", 200, "2025-07-03")
]

columns = ["ProductID", "ProductName", "Category", "Sales", "SaleDate"]

df1 = spark.createDataFrame(data , columns)

groupdf = df1.groupBy("Category")\
		  .agg(sum("sales").alias("total_sales"))

windf = df1.withColumn("rnk" , row_number().over(Window.partitionBy("Category").orderBy(F.desc("sales"))))

mwindf = windf.filter(col("rnk") == 1)

mwindf.show()
groupdf.show()


spark.stop()


