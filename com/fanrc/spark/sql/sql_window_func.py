from sql_1_hello_world import initSparkSession
from pyspark.sql.types import StructType,StringType,LongType
# import pyspark.sql.functions
if __name__ == '__main__':
    spark = initSparkSession()
    sc = spark.sparkContext

    rdd = sc.parallelize([
        ('zhangsan', '销售1', '202404', 500),
        ('zhangsan', '销售1', '202405', 1500),
        ('lisi', '销售1', '202405', 1499),
        ('cuicui', '销售2', '202404', 5000),
        ('cuicui', '销售2', '202405', 5800),
        ('cuicui', '销售2', '202406', 6000),
        ('fanfan', '销售2', '202404', 6000),
        ('fanfan', '销售2', '202404', 3500),
        ('wangwu', '销售3', '202403', 900),
        ('wangwu', '销售3', '202404', 500),
        ('zhaoliu', '销售3', '202404', 501),
    ])

    schema = StructType().add("name", StringType(), False)\
        .add("department", StringType(), False)\
        .add("date_month", StringType(), False)\
        .add("amount", LongType(), False)\

    df = rdd.toDF(schema=schema)
    df.createOrReplaceTempView("sales")

    spark.sql("""
    select
        *,
        row_number() over(partition by department order by amount desc) as r_num,
        rank() over(partition by department order by amount desc) as r_rank
        --dense_rank() over(partition by department order by amount desc) as d_rank
        --lead(amount, 1) over(partition by department order by amount desc) as lead_a,
        --lag(amount, 1) over(partition by department order by amount desc) as lag_a
    from sales
    """).show(20,False)