#coding:utf8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,LongType

def initSparkSession():
    spark = SparkSession.builder.master('local[*]').appName('spark_sql').getOrCreate()
    return spark

if __name__ == '__main__':
    print("{corrupt_col} IS NOT NULL".format(corrupt_col=))