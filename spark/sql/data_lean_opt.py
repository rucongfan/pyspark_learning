#coding:utf8
from pyspark.sql.types import StructType,StringType,LongType
from sql_1_hello_world import initSparkSession
import time
import random
from pyspark.sql.types import (
    StructType,
    Row,
)

def keyAddSalt(rowData):
    randInt = random.randint(0, 4)
    return Row(str(randInt) + "-"+ rowData[0],
               rowData[1],
               rowData[2],
               rowData[3])

def deleteSalt(data):
    key = data[0]
    if "-" in key:
        return Row(key.split('-')[1],
                   int(data[1]))
    return Row(data[0], int(data[1]))
if __name__ == '__main__':
    spark = initSparkSession()

    schema = StructType().add("user_id", StringType(), True) \
        .add("movie_id", StringType(), True) \
        .add("score", LongType(), True) \
        .add("date_time", LongType(), True)

    df = spark.read.csv('/data/spark/data/data_lean',
                        sep=' ',
                        lineSep='\n',
                        schema=schema)
    # 这里user_id=405会出现数据倾斜的问题
    # countDf = df.repartition(20).groupBy('user_id').count()
    #df.groupBy("user_id").count().sort("count", ascending=False).show(20,False)

    #查看分区分布情况
    reDf = df.repartition(10,"user_id")
    def countPartitions(iterator):
        count = 0
        for _ in iterator:
            count += 1
        return [count]  # 返回分区索引和记录数
    preOptTotal = reDf.count()
    partition_counts = reDf.rdd.mapPartitions(countPartitions).collect()
    # 输出每个分区的数据量
    print("数据倾斜优化前的数据分布：", partition_counts, " 优化前的总数: ", preOptTotal)


    # 优化数据倾斜问题
    # 1.过滤出405的用户，进行加盐操作
    user405Rdd = df.where("user_id='405'").rdd
    added405 = user405Rdd.map(keyAddSalt).toDF(schema=schema)
    # added405.groupBy("user_id").count().sort("count", ascending=False).show(20, False)

    #过滤405以外的数据
    otherUser = df.where("user_id <> '405'")
    optedDf = otherUser.union(added405)

    optedTotal = optedDf.count()
    optedRdd = optedDf.rdd.repartition(10)
    optedInfo = optedRdd.mapPartitions(countPartitions).collect()
    print("优化后的分区数据分布：", optedInfo, " 优化后的总数为：", optedTotal)

    #进行聚合操作
    pre1Counted = optedRdd.map(lambda data: (data[0], 1))\
        .reduceByKey(lambda i1, i2: i1+i2)
    print("------第一次预聚合的结果-------")
    print(pre1Counted.sortBy(lambda data:data[1], ascending=False).take(10))

    # 定义新的scheme也就是聚合结果的schema
    newSchema = StructType().add("user_id", StringType(), False).add("count", LongType(), False)
    # 对出聚合的结果进行减盐然后进行最终的聚合
    deleted = pre1Counted.map(deleteSalt).reduceByKey(lambda i1, i2: i1+i2)

    # 减盐然后查看效果
    print("---------最终聚合结果为---------")
    print(deleted.sortBy(lambda data:data[1], ascending=False).take(10))

    print("-------验证结果--------")

    df.createOrReplaceTempView("origin_data")
    spark.sql("""
        select user_id,count(1) from origin_data group by user_id order by count(1) desc
    """).show(10,False)