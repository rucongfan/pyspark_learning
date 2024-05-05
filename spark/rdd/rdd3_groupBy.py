#coding: utf8

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('flatMapDemo').setMaster('local[2]')
sc = SparkContext(conf=conf)

srdd = sc.parallelize([
    (1001, 'zhangsan'),(1002, 'fanfan'),(1003, 'fanfan'), (1004, 'cuicui')
])

grouppedRdd = srdd.groupBy(lambda value: value[0])
mapped = grouppedRdd.map(lambda data: (data[0], list(data[1])))
print(mapped.collect())

