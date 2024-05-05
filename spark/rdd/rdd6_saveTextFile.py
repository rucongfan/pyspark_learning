#coding:utf8
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('yarn-test').setMaster('local[*]')
sc = SparkContext(conf=conf)

frdd = sc.textFile("/data/words.txt")

mapped = frdd.flatMap(lambda line: line.split(' ')).map(lambda data: (data, 1))
mapped.groupByKey().saveAsTextFile('/data/spark_out/')