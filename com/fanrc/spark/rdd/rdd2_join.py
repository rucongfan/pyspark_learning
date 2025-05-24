#coding:utf8

from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('flatMapDemo').setMaster('local[2]')
sc = SparkContext(conf=conf)

userRdd = sc.parallelize([
    (1001, 'zhangsan', 'henan'),(1002, 'fanfan','guangdong'),(1003, 'cuicui', 'hongkong')
])

workRdd  = sc.parallelize([
    (1002, '科技部', 'guangdong'),(1003, '财务部', 'henan')
])

# [(key1, (t1value1, t2value2)), (key2, (t1value1, t2value2))]
joined = userRdd.join(workRdd)
print(joined.collect())

leftted = userRdd.leftOuterJoin(workRdd)
print(leftted.collect())

rightted = userRdd.rightOuterJoin(workRdd)
print(rightted.collect())
