from sql_1_hello_world import initSparkSession
from pyspark.sql.types import StructType,StringType,LongType,StructField
import time

if __name__ == '__main__':
    spark = initSparkSession()

    schema = StructType().add("user_id", StringType(), True)\
            .add("movie_id", StringType(), True)\
            .add("score", LongType(), True)\
            .add("date_time", LongType(), True)

    df = spark.read.csv('/data/spark/data/u.data', sep='\t',schema=schema)

    # 1.根据user_id聚合
    less200Df = df.groupBy("user_id")\
        .count()\
        .where('count < 21')\
        .withColumnRenamed("user_id", "user_id_agg")

    # 2.join 原始数据获取user全部信息
    joinedDf = df.join(less200Df, df.user_id == less200Df.user_id_agg, 'inner')\
        .select(["user_id", "movie_id","score","date_time"])

    # 3.过滤user_id=405
    user405Df = df.where("user_id='405'")
    # 4.制造数据倾斜的数据源
    unionedDf = user405Df.union(joinedDf)

    # 验证数据分布：
    # unionedDf.groupBy("user_id").count().sort("count", ascending=False).show(20,False)

    unionedDf.write.csv('hdfs:///data/spark/data/data_lean', sep=' ', lineSep='\n')

    # unionedDf.createOrReplaceTempView("movies_info")

    # user top n
    # spark.sql("select user_id, count(1) from movies_info group by user_id order by count(1) desc").show(10,False)

    # movies top n
    # spark.sql("select movie_id, count(1) from movies_info group by movie_id order by count(1) desc").show(10,False)

    # time.sleep(300 * 1000)