#coding:utf8

import jieba

def cut_keyword_to_list(keyword):
    return list(jieba.cut_for_search(keyword, True))


# words = jieba.cut_for_search('我要努力学习然后找到一个满意的工作', True)
# # for data in words:
# #     print(data)
#
# print(list(words))

def supplyWord(data):
    if data=='传智播': data='传智播客'
    if data=='院校': data='院校邦'
    if data=='博学': data='博学谷'
    return (data, 1)