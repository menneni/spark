import os
import sys

# Path for spark source folder
os.environ['SPARK_HOME']="/home/thej/Downloads/spark-1.6.2-bin-hadoop2.6"

# Append pyspark  to Python Path
sys.path.append("/home/thej/Downloads/spark-1.6.2-bin-hadoop2.6/python")
sys.path.append("/home/thej/Downloads/spark-1.6.2-bin-hadoop2.6/python/lib")

# ip: (number, target_number)
# returns (set bit count, number) as tuple
def myFunc(a, b):
    return bitcount(a,a&b)


# ip: (number, AND(number,target_number))
# counting number of 1's after applying AND operation
# returns (set bit count, number)
def bitcount(x,n):
    count = 0
    temp = x
    while n > 0:
        if (n & 1 == 1): count += 1
        n >>= 1

    return (count,temp)


try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    conf = (SparkConf()
        .setMaster("local[2]")
        .setAppName("My app")
        .set("spark.executor.memory", "1g"))
    sc = SparkContext(conf = conf)
    ip = sc.parallelize(xrange(1,255))

    #target_number : 166
    mapRdd = ip.map(lambda x:myFunc(x,166))
    print mapRdd.reduceByKey(lambda a,b: a if a>b else b).collect()
    print mapRdd.max()



except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)