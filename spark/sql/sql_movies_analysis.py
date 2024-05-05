#coding:utf8
import string

from sql_1_hello_world import initSparkSession
from pyspark.sql.functions import avg

if __name__ == '__main__':
    spark = initSparkSession()
    df = spark.read.csv('/data/spark/data/u.data', sep='\t')
    df = df.toDF("user_id", "m_id", "score","date_time")
    df.createOrReplaceTempView("movies_info")

    # #用户平均分
    spark.sql("select user_id, avg(score) from movies_info group by user_id").show(5, False)
    # #电影平均分
    spark.sql("select m_id, avg(score) from movies_info group by m_id").show(5, False)

    # 查询大于平均分的电影数量 #55375
    avg_score = spark.sql("""
    select avg(score) as a_score from movies_info
    """).take(1)
    sql='select count(1) from movies_info where score > ' + str(avg_score[0]['a_score'])
    spark.sql(sql).show(1,False)

    # 方法二：
    cnt = df.where(
            df['score'] > df.select(
                                avg(df['score'])
                            ).first()['avg(score)']
          ).count()
    print('方法二：',cnt)

    # 评分超过3的评分次数最多的用户：450, 平均分是：3.864814
    user = spark.sql("""
    select
        user_id, count(1) as cnt
    from movies_info
    where score > 3
    group by user_id
    order by cnt desc
    limit 1
    """).take(1)

    topUserSql = """
        select
            avg(score)
        from movies_info
        where user_id = {user_id}
    """.format(user_id = user[0]['user_id'])
    spark.sql(topUserSql).show(1,False)

    sscore = df.select(
                avg(
                    df.where(df['user_id'] == df.where(df['score'] > 3) \
                     .groupBy('user_id') \
                     .count() \
                     .sort('count', ascending=False).first()['user_id']
             )['score'])).first()['avg(score)']
    print(sscore)

    # secondAvg = df.select(
    #     avg(
    #         df.where(
    #             df['user_id'] == df.where(df['score'] > 3)
    #                             .groupBy('user_id')
    #                             .count()
    #                             .sort('count',ascending=False)['user_id']
    #         )['score']
    #     )
    # ).first()
    # print('第二种方式', secondAvg['avg(score)'])