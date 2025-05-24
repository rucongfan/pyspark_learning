#coding:utf8

from user_click_analysis import initSc
from broadcast_demo import matchUserInfo
# def matchUserInfo(data):
#     user_info_bc = userInfoBroadcast.value
#     name = ""
#     for user in user_info_bc:
#         if data[0] == user[0]:
#             name = user[1]
#             return (name, data[1], data[2])


if __name__ == '__main__':
    sc = initSc()

    prdd = sc.parallelize([
        (1, '语文', 95),
        (1, '数学', 80),
        (1, '英语', 93),
        (2, '语文', 80),
        (2, '英语', 100),
        (2, '数学', 99),
        (3, '语文', 98),
    ])

    # 定义本地维度信息
    user_info = [
        (1, 'fanfan', 1001),
        (2, 'cuicui', 1003),
        (3, 'dingdan', 1004)
    ]

    #广播维度信息
    userInfoBroadcast = sc.broadcast(user_info)

    print(prdd.map(lambda data: matchUserInfo(userInfoBroadcast=userInfoBroadcast, data=data)).collect())

