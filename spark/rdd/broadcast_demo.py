#coding:utf8
from pyspark.broadcast import Broadcast

def matchUserInfo(userInfoBroadcast: Broadcast, data):
    user_info_bc = userInfoBroadcast.value
    name = ""
    for user in user_info_bc:
        if data[0] == user[0]:
            name = user[1]
            return (name, data[1], data[2])