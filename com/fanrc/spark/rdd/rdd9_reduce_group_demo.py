#coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,LongType

def initSparkSession():
    spark = SparkSession.builder.master('local[*]')\
        .appName('spark_sql')\
        .getOrCreate()
    return spark


if __name__ == '__main__':
    spark = initSparkSession()
    sc = spark.sparkContext

    rdd = sc.parallelize([
        ('zhangsan', 18, 'shezhen'),
        ('zhangsan', 10, 'guangdong'),
        ('cuicui', 19, 'henan'),
        ('fanfan', 19, 'henan')
    ])

    res = rdd.map(
        lambda data: (data[0], 1)
    ).reduceByKey(lambda i1, i2: i1 + i2)

    sc.setCheckpointDir("hdfs:///data/spark/checkpoint")
    res.checkpoint();

    resList = res.collect()
    print("reduceBykey: ", resList)

    # res2 = rdd.map(
    #     lambda data: (data[0], 1)
    # ).reduce
