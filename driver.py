#encoding=utf-8
import pandas as pd
import  numpy as np
import datetime,sys,subprocess
import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import  roc_auc_score

base='/Users/baipeng/PycharmProjects/try/JData'
def main():
    day = datetime.datetime.strptime('2016-01-31' ,'%Y-%m-%d')

    for i in range(30):
        cday = (day + datetime.timedelta(days= i )).strftime('%Y-%m-%d')
        out = '{base}/sample/{day}.csv'.format(base=base,day=cday)
        inf = '{base}/partial/{day}.csv'.format(base = base, day=cday)
        print inf
        subprocess.check_call('python jd.py each_sample {out} {inf} '.format(
            out = out ,
            inf = inf
        ),shell=True)
if __name__ == '__main__':
    main()
