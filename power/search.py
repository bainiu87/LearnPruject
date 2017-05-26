from __future__ import print_function
import torch
import torch.nn as nn
from torch.autograd import Variable
import torch.optim as optim
import numpy as np
import datetime,time
import matplotlib
# matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import pandas as pd

from sklearn.metrics import mean_squared_error
from sklearn.metrics import median_absolute_error




def search(pc, lag ):

    target = pc[-lag:]
    candi = -1
    min_mse = 1e10
    for i in range(  int( len(pc) -1 - 1* lag)) :
        y = pc[i:i+lag]

        # mse = mean_squared_error(target,y)
        # mse = mean_absolute_error(target, y)
        mse = median_absolute_error(target, y)

        if mse < min_mse:
            min_mse = mse
            candi = i

    print  ( 'search' , candi , min_mse)


    y_hat = pc[candi+lag] * sum(target) /sum ( pc[candi:candi+lag])
    # y_hat = pc[candi+lag] * sum(target) /sum ( pc[candi:candi+lag])

    pc.append(y_hat)

    return y_hat





if __name__ == '__main__':
    # set ramdom seed to 0
    np.random.seed(0)
    torch.manual_seed(0)
    # load data and make training set
    # data = torch.load('traindata.pt')
    #
    # data_power = torch.load('traindata.power.pt')

    df = pd.read_csv('~/data/power.csv')

    df = df[['record_date','power_consumption']].groupby(['record_date']).sum().reset_index()

    df['power_consumption'] = df['power_consumption'] / df['power_consumption'].max()
    df['power_consumption'] = df['power_consumption'].astype('float64')
    print ( df.head())
    pca  = df['power_consumption'].values.tolist()
    pc = pca[:]

    print (type(pc))

    y = []
    for i in range(30):
        print ('step ' , i )
        yi   = search( pc, 3 )
        y.append(yi)
    print(len(pc))
    plt.plot(np.arange(len(pc[-30:])), np.array(pc[-30:]), '^' , linewidth=2.0)

    # plt.plot(np.arange(30) , np.array(pca[-30:]) , 'y' )

    plt.show()



    def build_date(day,cnt):
        ds = []
        d = datetime.datetime.strptime(day,'%Y%m%d')
        for i in range(cnt):
            c = d + datetime.timedelta(days=i)
            ds.append(  c.strftime('%Y%m%d'))
        return ds
    p =  np.array(y) * 4905574.
    p =  [ int(i) for i in p ]



    pd.DataFrame({'predict_date': build_date('20160901',30) , 'predict_power_consumption': p }).to_csv('Tianchi_power_predict_table.csv',index=False)


    print ( 'show' )

