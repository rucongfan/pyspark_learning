#coding:utf8

from user_click_analysis import initSc
from pyspark.broadcast import Broadcast

def mapWithFilter(broadcast: Broadcast, data):
    if data not in broadcast.value:
        return (data, 1)

def countWithAcc(data):
    global acc
    if data in broadcast.value:
        acc += 1

if __name__ == '__main__':
    sc = initSc()
    frdd = sc.textFile('/data/spark/data/broadcast_data.text')

    flatted = frdd.flatMap(lambda data: data.split(' '))
    # 过滤空字符串
    filtered = flatted.filter(lambda data: data not in ['', None, 'None'])
    filtered.cache()
    # 计算除特殊符号外单词的数量
    signList = ['!', '#', '%', ',']
    # 广播配置
    broadcast = sc.broadcast(signList)

    # 需求1，统计除特殊字符外单词的topN
    wordWithOne = filtered.map(lambda data: mapWithFilter(broadcast=broadcast, data=data))
    filteredWordOne = wordWithOne.filter(lambda data: data != None)
    counts = filteredWordOne.reduceByKey(lambda i1, i2: i1 + i2)
    sorted = counts.sortBy(keyfunc=lambda data: data[1]
                              ,ascending=False
                              ,numPartitions=1)
    print(sorted.collect())

    # 需求2， 统计特殊字符出现的次数
    signFilter = filtered.filter(lambda data: data in broadcast.value)
    print(signFilter.count())

    # 使用累加器完成
    acc = sc.accumulator(0)
    # 使用map来统计
    filtered.foreach(countWithAcc)
    print("特殊字符的数量为" + str(acc.value))
