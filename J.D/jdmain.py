# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd
import datetime
from sklearn.datasets import make_hastie_10_2
from sklearn.ensemble import GradientBoostingClassifier

class learn(object):
    def packageData(self):
        beginTime = datetime.datetime(2016,04,01)
        state = True
        data = []
        while state:
            print beginTime
            dt = pd.read_csv("./actionInfo_day/label_"+beginTime.strftime("%Y-%m-%d")+".csv",dtype=str,
                             index_col=0)
            dt.drop(['time_x','user_reg_tm','time'],axis=1,inplace=True)
            data.append(dt)
            beginTime = beginTime + datetime.timedelta(days=1)
            if beginTime == datetime.datetime(2016,04,12):
                state = False
        date2 = pd.concat(data)
        date2.to_csv("pgd04.csv")
    def testsmple(self):
        beginTime = datetime.datetime(2016,04,12)
        state = True
        data = []
        while state:
            dt = pd.read_csv("./actionInfo_day/label_" + beginTime.strftime("%Y-%m-%d") + ".csv", dtype=str,
                             index_col=0)
            dt.drop(['time_x', 'user_reg_tm', 'time'], axis=1, inplace=True)
            data.append(dt)
            beginTime = beginTime + datetime.timedelta(days=1)
            if beginTime == datetime.datetime(2016, 04, 16):
                state = False
        date2 = pd.concat(data)
        date2.to_csv("testsmple.csv")
    def begin(self):
        dt = pd.read_csv("pgd04.csv",dtype=str,index_col=0)
        testDT = pd.read_csv("testsmple.csv",dtype = str,index_col=0)
        X_train = dt.drop(['user_id','sku_id','Y'],axis=1,inplace=False).fillna(0)
        Y_train = dt['Y']
        clf = GradientBoostingClassifier().fit(X_train,Y_train)
        x_test = testDT.drop(['user_id','sku_id','Y'],axis=1,inplace=False).fillna(0)

        end = clf.predict_proba(x_test)
        end_post = [i[0] for i in end]
        print end_post
        testDT['socre'] = end_post
        testDT.to_csv('jg.csv')

if __name__=='__main__':
    obj = learn()
    obj.begin()