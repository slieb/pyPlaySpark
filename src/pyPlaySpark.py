import os
import sys

sparkHome = "/Users/slieb/dev/3rd/spark-2.0.0-bin-hadoop2.7"

# Set the SPARK_HOME environment variable
os.environ['SPARK_HOME'] = sparkHome

# Append the necessary pyspark directories to the system PATH
sys.path.append (sparkHome + "/python")
sys.path.append (sparkHome + "/python/lib/py4j-0.10.1-src.zip")

# Now we are ready to import Spark Modules
try:
    from pyspark import SparkContext
    from pyspark import SparkConf

except ImportError as e:
    print "Error importing Spark Modules", e
    sys.exit(1)

import numpy as np

conf = SparkConf().setMaster("local").setAppName("sparkPlay")
sc = SparkContext(conf = conf)
lines = sc.textFile (sparkHome + "/README.md")
print "File has ", lines.count(), " lines in it"

nums = sc.parallelize ([1, 2, 3, 4])
squared = nums.map (lambda x: x * x).collect()
for num in squared:
    print "%i " % (num)

lines = sc.parallelize (["hello world", "hi"])
words = lines.flatMap (lambda line: line.split(" "))
print words.first()