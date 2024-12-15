from pyspark.sql import SparkSession

# Initialize Spark Session

spark = SparkSession.builder.appName("App Name").getOrCreate()


# From a collection

data = [("Alice", 1), ("Bob", 2)]

# From a CSV file
df = spark.createDataFrame(data, ["Name", "Id"])

# # From a JSON
# df = spark.read.json("file.json")

# Show DataFrame
df.show()


