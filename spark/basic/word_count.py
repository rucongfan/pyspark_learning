#coding:utf8
from pyspark import SparkContext, SparkConf

# driver负责
appName='word_count'
master='local[*]'
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

# executor负责
frdd=sc.textFile('hdfs://localhost:8020/data/words.txt')

numP = frdd.getNumPartitions()
print('默认分区数：' + str(numP))

counts=frdd.flatMap(lambda line: str.split(line, " "))\
    .map(lambda data: (data, 1))\
    .reduceByKey(lambda a, b: a + b)

# driver负责
res = counts.collect()
print(res)