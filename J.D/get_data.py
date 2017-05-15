# -*- coding: utf-8 -*-
import pandas as pd
import datetime

base = "./mysql/smple"
user_Action_fiel = 'day,user_id,sku_id,type_1,type_2,type_3,type_4,type_5,type_6,Ltype_1,Ltype_2,Ltype_3,Ltype_4,Ltype_5,Ltype_6'

with open('Data.csv',"w") as f:
    f.write(user_Action_fiel+'\n')


def fileFiel(fileName,fiel):
    with open(fileName,"w") as l:
        l.write(fiel)

state = True
while state:
    beginTime = datetime.datetime(2016,02,01)
    lastdayTime = beginTime - datetime.timedelta(days=1)

    NowfileDir = base+"/day_"+beginTime.strftime("%Y-%m-%d")+".txt"
    lastDayFileDir = base+"/day_"+lastdayTime.strftime("%Y-%m-%d")+".txt"
    dayObj = pd.read_csv(NowfileDir,dtype=str,sep='\t',usecols=['user_id',"sku_id",'type'])
    lastdayObj = pd.read_csv(lastDayFileDir,dtype=str,sep='\t',usecols=['user_id','sku_id','type'])
    DataObj = pd.merge(dayObj,lastdayObj,how='outer',left_on=['user_id','sku_id'],right_on=['user_id','sku_id'])
    for i in DataObj.groupby(['user_id','sku_id']):

        LtypeX = ['','','','','','']
        LtypeY = ['','','','','','']
        first = list(i[0])
        typeX = i[1].type_x.value_counts().to_dict()
        typeY = i[1].type_y.value_counts().to_dict()

        if len(typeX) != 0:
            for k in typeX.keys():
                LtypeX[int(k)-1] = str(typeX[k])
        if len(typeY) != 0:
            for k in typeY.keys():
                LtypeY[int(k)-1] = str(typeY[k])
        Data = first + LtypeX + LtypeY
        DataStr = ','.join(Data)
        with open('Data.csv',"a") as F:
            F.write(DataStr+'\n')
    beginTime = beginTime + datetime.timedelta(days=1)
    if beginTime.strftime("%Y-%m-%d") == "2016-04-16":
        state = False

