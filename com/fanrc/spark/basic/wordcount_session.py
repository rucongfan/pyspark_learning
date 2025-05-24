import sys
from operator import add

from pyspark.sql import SparkSession
spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

lines = spark.read.text("/data/words.txt").rdd.map(lambda r: r[0])
counts = lines.flatMap(lambda line: str.split(line, " ")) \
.map(lambda data: (data, 1)).reduceByKey(lambda a, b: a + b)

swap = counts.map(lambda pair: (pair[1], pair[0])).sortByKey(ascending=False).map(lambda pair: (pair[1], pair[0]))

output = swap.collect()
for (word, count) in output:
        print("%s: %s" % (word, count))



spark.stop()