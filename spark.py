from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("FirstSparkApp").getOrCreate()

## DataFrame
# pipelining
# let's creat Spark DataFrame --> this is "Transformation" [narrow] becuase it's one-to-one --> lazy operation
# we read CSV into Datafram and converte it into list of rows "python"
# flightData2015_df = spark\
#                     .read\
#                     .option("inferSchema","true")\
#                     .option("header","true")\
#                     .csv(r"C:\Users\user\Desktop\MyGitHub\Spark-The-Definitive-Guide\data\flight-data\csv\2015-summary.csv")

# #By default, when we perform a shuffle, Spark outputs 200 shuffle partitions. Let’s set this value to 5 to reduce the number of the output partitions from the shuffle
# # 5 : is the logical partitions
# spark.conf.set("spark.sql.shuffle.partitions", "5")


# # shuffle 
# # let's do another transformation on our df [wide] --> "because rows will need to be compared with one another" --> Expalin() : flightData2015.sort("count").explain()
# # then, we spcify an action to kick off our plan. Sorting by cloumn "count"
# flightData2015_df.sort("count").take(2)



## SQL
# we can also express our business logic in SQL

# register our DF as tabl or view
# flightData2015_sql = spark\
#                     .read\
#                     .option("inferSchema","true")\
#                     .option("header","true")\
#                     .csv(r"C:\Users\user\Desktop\MyGitHub\Spark-The-Definitive-Guide\data\flight-data\csv\2015-summary.csv")


flightData2015_sql = spark\
                    .read.csv(r"C:\Users\user\Desktop\MyGitHub\Spark-The-Definitive-Guide\data\flight-data\csv\2015-summary.csv", header=True, inferSchema=True)
                    
flightData2015_sql.createOrReplaceTempView("flight_data_2015")

# query our data in SQL
#  SQL query against a DataFrame returns another DataFrame ****
sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

sqlWay.explain()


# we can do the same logic using DataFrame and it'll give same logical plan
dataFrameWay =  flightData2015_sql\
                .groupBy("DEST_COUNTRY_NAME")\
                .count()

dataFrameWay.explain()


# DataFrames (and SQL) in Spark already have a huge number of manipulations available
# let's try max

spark.sql(""" SELECT max(COUNT) FROM flight_data_2015 """).take(1)

# or this way with DF :
# Methods on DataFrames feel very SQL-like
from pyspark.sql.functions import max
flightData2015_sql.select(max("count")).take(1)



# multi-transformation --> SQL
maxSQL = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSQL.show()



# multi-transformation --> DataFrame
# Methods on DataFrames feel very SQL-like
from pyspark.sql.functions import desc
flightData2015_sql\
    .groupBy("DEST_COUNTRY_NAME")\
    .sum("count")\
    .withColumnRenamed("sum(count)", "destination_total")\
    .sort(desc("destination_total"))\
    .limit(5)\
    .show() # Action, spark won't run till reach here

# the underlying plans for both (SQL and DataFrame) are the same
# in our multi-transformation we have seven steps that take us all the way back to the source data, [ conceptual plan ] as following :
# read -> groupby -> sum -> rename colmun -> sort -> limit -> collecct

flightData2015_sql.printSchema()