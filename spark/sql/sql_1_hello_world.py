#coding:utf8

from pyspark.sql import SparkSession

def initSparkSession():
    spark = SparkSession.builder.master('local[*]').appName('spark_sql').getOrCreate()
    return spark
if __name__ == '__main__':

    spark = initSparkSession()
    # fdf = spark.read.csv('/data/user_info.sql',sep=' ')
    # fdf = spark.read.text('/data/user_info.sql', lineSep='\n')
    # df = fdf.toDF('name', 'age', 'address')
    # df.createTempView('user_click')
    # spark.sql("select * from user_click").show()


    # rdd to df
    frdd = spark.sparkContext.textFile('/data/user_info.sql')
    mapped = frdd.map(lambda data: data.split(' ')).map(lambda data: (data[0], int(data[1]), data[2]))
    df = spark.createDataFrame(mapped, schema=['name', 'age', 'address'])
    df.printSchema()
    df.createOrReplaceTempView("user_click")
    spark.sql("select * from user_click where name ='cuicui'").show(2, True)
    # print(mapped.collect())

    # spark.read.parquet()

