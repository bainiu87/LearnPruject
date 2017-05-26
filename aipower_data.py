#encoding=utf-8
__author__ = 'baiyun'

import pandas as pd
import math
import numpy as np
import torch

"""
0.把目前的NN结构搞清楚
1.构造更多的样本


采用更多的样本以后，loss 上升 ；尝试失败

2.核心问题：为什么采用更多样本后 ？loss上升导致失败呢 ？
"""
df = pd.read_csv('~/data/Tianchi_power.csv')

df['record_date'] = pd.to_datetime(df['record_date'])


df = df[['record_date','power_consumption']].groupby(['record_date']).sum().reset_index()



print len(df)
print df.head()

print df['power_consumption'].astype('float64').max()

print df['power_consumption'].describe()



df['power_consumption'] = df['power_consumption']/df['power_consumption'].max()
df['power_consumption'] = df['power_consumption'].astype('float64')


# sample  = []
#
# k = 60
#
# for i in range(609-k+1):
#     b = i
#     e = i + k
#     ss=  df['power_consumption'][b:e,].values
#     # print ss,ss.shape
#     sample.extend( ss)
#     print b,e

torch.save(df['power_consumption'][9:].values.reshape(15,40), open('traindata.pt', 'wb'))
#torch.save(np.array( sample) .reshape(609-k+ 1,k), open('traindata.pt', 'wb'))
