#coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,LongType

def initSparkSession():
    spark = SparkSession.builder.master('local[*]').appName('spark_sql').getOrCreate()
    return spark

if __name__ == '__main__':
    spark = initSparkSession()

    # schema = StructType().add("user_id", StringType(), True) \
    #     .add("movie_id", StringType(), True) \
    #     .add("score", LongType(), True) \
    #     .add("date_time", LongType(), True)
    #
    # df = spark.read.csv('/data/spark/data/data_lean',
    #                     sep=' ',
    #                     lineSep='\n',
    #                     schema=schema)

    sc = spark.sparkContext
    rdd = sc.parallelize([1,2,3,4,5,11,12,13,14,15], 3)

    print(rdd.glom().collect())

    def mapPartitionFunc(ita):
        count = 0
        for elem in ita:
            count += 1
        res = list()
        res.append(count)
        return res

    # 以分区为单位传入一个迭代器，方法也需要返回一个迭代器
    res = rdd.mapPartitions(mapPartitionFunc).collect()
    print(res)


    indexNum = {}
    # def mapPartitionWithIndex(idx, ita):
    #     count = 0
    #     for elem in ita:
    #         count += 1
    #
    #     partitionList = list()
    #     partitionList.append(count)
    #
    #     indexNum[idx] = partitionList
    #     return partitionList

    def mapPartitionWithIndex(idx, ita):
        count = sum(1 for _ in ita)  # 使用生成表达式计算元素数量
        partitionList = [count]
        indexNum[idx] = partitionList  # 更新全局变量
        return partitionList

    res2 = rdd.mapPartitionsWithIndex(mapPartitionWithIndex).collect()
    print(indexNum)