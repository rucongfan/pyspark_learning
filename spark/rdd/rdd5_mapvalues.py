#coding:utf8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('yarn-test').setMaster('local[*]')

sc = SparkContext(conf=conf)
frdd = sc.textFile("/data/words.txt")
mapped = frdd.flatMap(lambda line: line.split(' ')).map(lambda data: (data, 1))

grouped = mapped.groupByKey().map(lambda data: (data[0], list(data[1])))

res = grouped.mapValues(lambda data: sum(data)).collect()


print(grouped.collect())
print(res)

