from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import json

appName = "PySpark Example - JSON file to Spark Data Frame"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()


j = [{'abc':1, 'def':1, 'ghi':1},{'abc':2, 'def':2, 'ghi':2},{'abc':3, 'def':3, 'ghi':3}]
a = json.dumps(j)
print(a)
jsonRDD = SparkContext.parallelize(a)
df = spark.read.json(jsonRDD)

# Create data frame
json_file_path = 'data/example.json'
df = spark.read.json(json_file_path)
df.show()
