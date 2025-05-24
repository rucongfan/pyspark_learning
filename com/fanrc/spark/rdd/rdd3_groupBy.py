#coding: utf8

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('flatMapDemo').setMaster('local[*]')
sc = SparkContext(conf=conf)

srdd = sc.parallelize([
    (1001, 'zhangsan'),(1002, 'fanfan'),(1003, 'fanfan'), (1004, 'cuicui')
],3)

print("source: ", srdd.glom().collect())

grouppedRdd = srdd.groupBy(lambda value: value[0])

print("grouped", grouppedRdd.glom().collect())

mapped = grouppedRdd.map(lambda data: (data[0], list(data[1])))
print(mapped.collect())

