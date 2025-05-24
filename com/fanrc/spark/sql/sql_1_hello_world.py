#coding:utf8
import time

import pyspark.sql.types
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType

def initSparkSession():
    spark = SparkSession.builder\
        .master('local[*]')\
        .appName('spark_sql') \
        .getOrCreate()
    return spark

def mapParFunc(iter):
    count = 1
    for idx, elem in iter:
        # print(idx, '----', elem)
        for inner in elem:
            count += 1
    return [count]

if __name__ == '__main__':

    spark = initSparkSession()
    # fdf = spark.read.csv('/data/user_info.sql',sep=' ')
    # fdf = spark.read.text('/data/user_info.sql', lineSep='\n')
    # df = fdf.toDF('name', 'age', 'address')
    # df.createTempView('user_click')
    # spark.sql("select * from user_click").show()


    # rdd to df
    # frdd = spark.sparkContext.textFile('/data/user_info.sql')
    # mapped = frdd.map(lambda data: data.split(' ')).map(lambda data: (data[0], int(data[1]), data[2]))
    # df = spark.createDataFrame(mapped, schema=['name', 'age', 'address'])
    # df.printSchema()
    # df.createOrReplaceTempView("user_click")
    # spark.sql("select * from user_click where name ='cuicui'").show(2, True)
    # print(mapped.collect())
    # spark.read.parquet()

    sc = spark.sparkContext

    rdd = sc.parallelize([
        "hadoop flink flink spark",
        "spark spark hive flink"
    ], 1)
    flatted = rdd.flatMap(lambda data: data.split(' '))

    schema = StructType().add("sms", StringType(), True)
    #通过spark sql的方式实现group
    # df = flatted.map(lambda data: (data,)).toDF(schema=schema)
    # df.groupBy("sms").count().sort("count", ascending=False).show(20, False)


    # 使用rdd的方式实现看看是否存在预聚合
    mapped = flatted.map(lambda data: (data ,1))
    groupped = mapped.groupByKey()
    summed = groupped.map(lambda data: (data[0], sum(data[1])))
    print(summed.toDebugString())
    # time.sleep(600)


