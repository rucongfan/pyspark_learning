#coding:utf8
from pyspark import SparkContext, SparkConf
# import os

# 1、需要设置环境变量告知hadoop_conf_dir，spark以此找到yarn相关配置
# os.environ['HADOOP_CONF_DIR']='/usr/local/Cellar/hadoop/3.4.0/libexec/etc/hadoop'

conf = SparkConf().setAppName('yarn-test').setMaster('local[*]')
# 2、通过只会提及main函数的文件，如果依赖了其他文件需要手动配置上传的依赖
# 可以指定单个文件，也可以通过压缩一个zip包指定多个依赖文件
# conf.set("spark.submit.pyFiles", "path os file")

sc = SparkContext(conf=conf)
frdd = sc.textFile("/data/words.txt")
mapped = frdd.flatMap(lambda line: line.split(' ')).map(lambda data: (data, 1))
byKeyRes = mapped.reduceByKey(lambda i1, i2: i1 + i2).collect()
#groupBy
preReduce = mapped.groupByKey().mapValues(lambda data: sum(data)).collect()
    # .collect()

print(byKeyRes)
print(preReduce)