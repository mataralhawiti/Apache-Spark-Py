#test function
def dbTest(id, expected, result):
  assert str(expected) == str(result), "{} does not equal expected {}".format(result, expected)







-- How to start Spark in Standalone cluster mode :

1- : run below command to start the master (C:\Spark\spark-2.3.3-bin-hadoop2.7\bin\):
spark-class org.apache.spark.deploy.master.Master
  -->  2019-04-22 09:21:01 INFO  Utils:54 - Successfully started service 'sparkMaster' on port 7077.
  -->  2019-04-22 09:21:02 INFO  Master:54 - Starting Spark master at spark://172.25.100.113:7077
  the master now is working on :
  172.25.100.113:7077 (http://localhost:8080/)

2 - : access the master :
  http://localhost:8080/


3 - : in differnt cmd, run the following command to start the worker (C:\Spark\spark-2.3.3-bin-hadoop2.7\bin\) :
  spark-class org.apache.spark.deploy.worker.Worker spark://172.25.100.113:7077

  --> Successfully started service 'sparkWorker' on port 53326
  the worker now is working on :
  172.25.100.113:53326


4 - in differnt cmd, run the following command to start start Spark History server
  spark-class org.apache.spark.deploy.history.HistoryServer 
  # setup the logs
  # https://supergloo.com/spark-monitoring/
  # Spark UI vs Spark History server
  # Spark is not configured for the History server by default
  #  http://localhost:18080/
  



## submitting jobs to Spark

1 :
spark-submit --master spark://172.25.100.113:7077 C:\spark-course\popular-movies-nicer.py



2 :
spark-submit --master spark://172.25.100.113:7077 --deploy-mode cluster C:\spark-course\ratings-counter2.py
    Error: Cluster deploy mode is currently not supported for python applications on standalone clusters.

3 :
spark-submit --class org.apache.spark.examples.SparkPi --master spark://172.25.100.113:7077 --deploy-mode cluster C:\Spark\spark-2.3.3-bin-hadoop2.7\examples\src\main\scala\org\apache\spark\examples\SparkPi.scala
  #--> when try Scala it will fail when use port:7077 cuz submission must be via REST API on port 6066
  "2019-04-23 10:16:07 INFO  StandaloneRestServer:54 - Started REST server for submitting applications on port 6066"

spark-submit --class org.apache.spark.examples.SparkPi --master spark://172.25.100.113:6066 --deploy-mode cluster C:\Spark\spark-2.3.3-bin-hadoop2.7\examples\jars\spark-examples_2.11-2.3.3.jar


--------

# resources
https://stackoverflow.com/questions/36593446/failed-to-start-master-for-spark-in-windows
https://stackoverflow.com/questions/44437591/how-to-set-up-cluster-environment-for-spark-applications-on-windows-machines
https://stackoverflow.com/questions/28807490/what-conditions-should-cluster-deploy-mode-be-used-instead-of-client
https://stackoverflow.com/questions/29625926/unable-to-submit-jobs-to-spark-cluster-cluster-mode
