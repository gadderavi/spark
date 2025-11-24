from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import pyspark.sql.functions as F
from pyspark.sql.functions import col


spark = SparkSession.builder.master("local").appName("WindowFuncDemo").getOrCreate()

data = [
    (1, "Alice", "Electronics", 5000, "2025-08-01"),
    (2, "Bob", "Electronics", 7000, "2025-08-02"),
    (3, "Carol", "Electronics", 6000, "2025-08-03"),
    (4, "David", "Clothing", 4000, "2025-08-01"),
    (5, "Eva", "Clothing", 5500, "2025-08-02"),
    (6, "Frank", "Clothing", 3000, "2025-08-03"),
    (7, "Grace", "Grocery", 8000, "2025-08-01"),
    (8, "Heidi", "Grocery", 7500, "2025-08-02"),
    (9, "Ivan", "Grocery", 8500, "2025-08-03")
]

columns = ["EmpID", "Name", "Department", "Sales", "Date"]

df1 = spark.createDataFrame(data,columns)

widdf = df1.withColumn("sale_rank" , row_number().over(Window.partitionBy("Department").orderBy(F.desc("sales"))))

maindf = widdf.filter(col("sale_rank") == 1)

maindf.show()

spark.stop()