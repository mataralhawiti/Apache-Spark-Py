from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("labor").getOrCreate()

spark.sql("show databases").show()
# laborData = spark.read.csv(r"C:\Users\user\Desktop\Laborer4.csv",encoding='utf-16', header=True, inferSchema=True)

# print(laborData.dtypes)
# laborData.createOrReplaceTempView("Labor2019")


# ct = spark.sql("""
# SELECT *
# FROM Labor2019
# limit 1
# """)

# ct.show()