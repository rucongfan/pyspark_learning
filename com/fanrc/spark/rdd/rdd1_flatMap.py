#coding: utf8

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('flatMapDemo').setMaster('local[2]')
sc = SparkContext(conf=conf)

frdd = sc.textFile('/data/words.txt')

mapped = frdd.map(lambda line: line.split(" "))

flatmapped = frdd.flatMap(lambda line: line.split(" "))
print('frdd: ' + str(frdd.collect()))
print('mapped: ' + str(mapped.collect()))
print('flatmapped: ' + str(flatmapped.collect()))
