import findspark
from pyspark.sql import SparkSession


findspark.init()
# 创建 SparkSession
spark = SparkSession.builder \
    .appName("HousePricePrediction") \
    .getOrCreate()