#coding:utf8

from pyspark import SparkContext, SparkConf
import jieba
from jieba_demo import *

def initSc():
    conf = SparkConf().setAppName('yarn-test').setMaster('local[*]')
    # conf.set("spark.submit.pyFiles", "./jieba_demo.py")
    sc = SparkContext(conf=conf)
    return sc


if __name__ == '__main__':
    sc = initSc()
    frdd = sc.textFile('/data/user_click/input/SogouQ.txt')
    # frdd = sc.textFile('file:///Users/mark/Downloads/SogouQ.txt')
    lineRdd = frdd.flatMap(lambda line: line.split('\n'))
    lineRdd = lineRdd.map(lambda data: data.split('\t'))
    # 缓存切分后的数据源
    lineRdd.cache()

    #1.用户搜索关键词分析top5
    #获取搜索内容
    kWord = lineRdd.map(lambda data: data[2]).flatMap(lambda word: cut_keyword_to_list(word))
    filtered = kWord.filter(lambda data: data not in ['谷', '邦', '客'])
    # res = filtered.map(lambda data: supplyWord(data)).reduceByKey(lambda i1, i2: i1 + i2, numPartitions=1)#.map(lambda data: (data[1], data[0]))
    # print(res.top(5)), 次数是根据key来排序，因此需要手动指定sortBy
    res = filtered.map(lambda data: supplyWord(data)).reduceByKey(func=lambda i1, i2: i1+i2, numPartitions=1).sortBy(lambda data: data[1], ascending=False)
    print(res.take(5))

    #2. 

