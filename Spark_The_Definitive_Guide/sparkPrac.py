from pyspark import SparkConf

from pyspark import SparkContext

from pyspark.sql import *

 

 

spark = SparkSession\

                        .builder\

                        .appName("PythonWordCount")\

                        .getOrCreate()

 

 

# -- work with df, create df from data source

df_flightData2015 = spark\

                        .read\

                        .option("inferSchema", "true")\

                        .option("header", "true")\

                        .csv("file:///C:/Users/E005016/Desktop/2015-summary.csv")

 

 

print(type(df_flightData2015))

print(df_flightData2015.count())

print("#------------------ dfWay")

df = df_flightData2015\

            .groupBy('DEST_COUNTRY_NAME')\

            .count()\

            .sort("count", ascending=False)

 

df.printSchema()

df.show(3,truncate= True)

print(df.count())

 

print("#------------------ sqlWay")

### sql : register df as table

df_flightData2015.createOrReplaceTempView("tmTable")

sqlWay = spark.sql("""

SELECT DEST_COUNTRY_NAME, count(1)

FROM tmTable

GROUP BY DEST_COUNTRY_NAME

ORDER BY sum(count) DESC

LIMIT 5

""") ## return

 

print(type(sqlWay))

 

sqlWay.show()

 

 

 

 

# myRange = spark.range(1000).toDF("number")

# d = myRange.where("number % 2 = 0")

# result = d.count()

# print(result)

 

 

# print("#------------------ show")

# showDF = flightData2015.take(3)

# for i in showDF :

#          print(i)

 

 

#print(df.take(5)) #print(df.collect())