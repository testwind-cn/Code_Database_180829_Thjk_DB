import sys
# from operator import add
import os
import psutil


# Python 3.6 (venv_spark)
# C:\Users\wangjun\Desktop\python\venv_spark\Scripts\python.exe

from pyspark import SparkContext

from pyspark import SparkConf


from pyspark.sql import HiveContext, Row

#from pyspark.sql import SQLContext, Row


# Path for spark source folder
os.environ['SPARK_HOME'] = "C:\\Tools\\spark-2.3.2-bin-hadoop2.6"
os.environ['HADOOP_HOME'] = "C:\\Tools\\hadoop-common-2.2.0-bin-master"
os.environ['HADOOP_CONF_DIR'] = "C:\\Tools\\hadoop-common-2.2.0-bin-master\\conf"
#/etc/hive/conf.cloudera.hive/yarn-site.xml
os.environ['HADOOP_USER_NAME'] = "hadoop"

# Exception in thread "main" java.lang.Exception: When running with master 'yarn' either HADOOP_CONF_DIR or YARN_CONF_DIR must be set in the environment.


# Append pyspark to Python Path
sys.path.append("C:\\Tools\\spark-2.3.2-bin-hadoop2.6\\python")
sys.path.append("C:\\Tools\\spark-2.3.2-bin-hadoop2.6\\python\\lib\\py4j-0.10.7-src.zip")
sys.path.append("C:\\Tools\\spark-2.3.2-bin-hadoop2.6\\bin")
sys.path.append("C:\\Tools\\hadoop-common-2.2.0-bin-master\\bin")


conf = SparkConf().setMaster("local").setAppName("My App").set("spark.executor.memory","600m")

sc = SparkContext(conf=conf)
hiveCtx = HiveContext(sc)

rows = hiveCtx.sql("SELECT count(*) FROM loginfo.loginfo_monthly")
# rows.collect()
# keys = rows.map(lambda row: row[0])
rr = rows.collect()

#keys = rows.map(lambda row: row[0])
keys = rows.rdd.map(lambda row: row.id)

firstRow = rows.first()




print(firstRow.name)
print(firstRow.id)
print(firstRow.sex)
print(firstRow.age)

