#encoding=utf-8
import pandas as pd
import  numpy as np
import datetime,sys,subprocess
import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import  roc_auc_score

base='/Users/baipeng/PycharmProjects/try/JData'
def main():
    day = datetime.datetime.strptime('2016-04-01' ,'%Y-%m-%d')

    for i in range(15):
        cday = (day + datetime.timedelta(days= i )).strftime('%Y-%m-%d')
        out = '{base}/sample/{day}.csv'.format(base=base,day=cday)
        inf = '{base}/partial/{day}.csv'.format(base = base, day=cday)
        print inf
        subprocess.check_call('python jd2.py each_sample {out} {inf} '.format(
            out = out ,
            inf = inf
        ),shell=True)
def concat():
    day = datetime.datetime.strptime('2016-04-11' ,'%Y-%m-%d')

    ds  =[]
    for i in range(5):
        cday = (day + datetime.timedelta(days= i )).strftime('%Y-%m-%d')
        ds.append(cday)
    print ds
    subprocess.check_call('python jd2.py concat {days} '.format(
           days = '_'.join(ds)
        ),shell=True)

if __name__ == '__main__':

    if len(sys.argv) > 1 and sys.argv[1] == 'concat':
        concat()
    else:
        main()
