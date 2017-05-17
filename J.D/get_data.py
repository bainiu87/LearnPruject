# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import datetime
class get_smple(object):

    def produce_action_info(self):

        state = True

        beginTime = datetime.datetime(2016, 02, 01)
        lastTime = (beginTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        lastAction_df = pd.read_csv('mysql/smple/day_' + lastTime + '.txt', sep='\t',
                                    usecols=['time', 'user_id', 'sku_id', 'type'])

        lastAction_df['time'] = lastAction_df['time'].str[:10]

        Lastuser_sku_type = lastAction_df.groupby(['user_id', 'sku_id', 'type', 'time']).size().reset_index()
        Lastuser_sku_type_2 = pd.pivot_table(Lastuser_sku_type, values=0, index=['user_id', 'sku_id', 'time'],
                                             columns='type').reset_index()

        if len(Lastuser_sku_type_2.columns[3:]) != 6:
            setColumn = list(set(Lastuser_sku_type_2.columns[3:])^set([1,2,3,4,5,6]))
            for i in setColumn:
                Lastuser_sku_type_2[i] = np.nan

        num = 0

        while state:
            num += 1
            print num

            action_df = pd.read_csv('mysql/smple/day_' + beginTime.strftime("%Y-%m-%d") + '.txt', sep='\t',
                                    usecols=['time', 'user_id', 'sku_id', 'type'])

            action_df['time'] = action_df['time'].str[:10]

            user_sku_type = action_df.groupby(['user_id', 'sku_id', 'type', 'time']).size().reset_index()
            user_sku_type_2 = pd.pivot_table(user_sku_type, values=0, index=['user_id', 'sku_id', 'time'],
                                             columns='type').reset_index()

            if len(user_sku_type_2.columns[3:]) != 6:
                userColumn = list(set(user_sku_type_2.columns[3:])^set([1,2,3,4,5,6]))
                for i in userColumn:
                    user_sku_type_2[i] = np.nan

            df = pd.merge(user_sku_type_2, Lastuser_sku_type_2, how='left', left_on=['user_id', 'sku_id'],
                          right_on=['user_id', 'sku_id'])

            Lastuser_sku_type_2 = user_sku_type_2

            df.to_csv("./actionInfo_day/Ac_" + beginTime.strftime("%Y-%m-%d") + ".csv")

            beginTime = beginTime + datetime.timedelta(days=1)

            if beginTime == datetime.datetime(2016, 04, 16):
                state = False



    def produce_user_info(self):
        userObj = pd.read_csv("JData/JData_User.csv",header=0)
        userObj['st'] = 1

        user_sex = pd.pivot_table(userObj,values='st',index=['user_id','user_reg_tm'],columns='sex')
        user_sex.columns = user_sex.columns.map(lambda x:str(x)+"_sex")

        user_age = pd.pivot_table(userObj,values='st',index=['user_id','user_reg_tm'],columns='age')

        user_lv = pd.pivot_table(userObj,values='st',index=['user_id','user_reg_tm'],columns='user_lv_cd')
        user_lv.columns = user_lv.columns.map(lambda x:str(x)+"_lv")

        Data = pd.merge(user_sex.reset_index(),user_age.reset_index(),how='left',left_on=['user_id','user_reg_tm'],
                        right_on=['user_id','user_reg_tm'])

        AllData = pd.merge(Data,user_lv.reset_index(),how='left',left_on=['user_id','user_reg_tm'],
                           right_on=['user_id','user_reg_tm'])

        AllData.to_csv("UserInfo.csv")

    def produce_product_info(self):
        proObj = pd.read_csv("JData/JData_Product.csv",header=0)
        proObj['st'] = 1

        pro_a1 = pd.pivot_table(proObj,values='st',index='sku_id',columns='a1')
        pro_a1.columns = pro_a1.columns.map(lambda x:str(x)+'_a1')

        pro_a2 = pd.pivot_table(proObj,values='st',index='sku_id',columns='a2')
        pro_a2.columns = pro_a2.columns.map(lambda x:str(x)+'_a2')

        pro_a3 = pd.pivot_table(proObj,values='st',index='sku_id',columns='a3')
        pro_a3.columns = pro_a3.columns.map(lambda x:str(x)+'_a3')

        pro_cate = pd.pivot_table(proObj,values='st',index='sku_id',columns='cate')
        pro_cate.columns = pro_cate.columns.map(lambda x:str(x)+'_cate')

        pro_brand = pd.pivot_table(proObj,values='st',index='sku_id',columns='brand')
        pro_brand.columns = pro_brand.columns.map(lambda x:str(x)+'brand')

        data = pd.merge(pro_a1.reset_index(),pro_a2.reset_index(),how='left',left_on='sku_id',right_on='sku_id')
        data1 = pd.merge(data,pro_a3.reset_index(),how='left',left_on='sku_id',right_on='sku_id')
        data2 = pd.merge(data1,pro_cate.reset_index(),how='left',left_on='sku_id',right_on='sku_id')
        data3 = pd.merge(data2,pro_brand.reset_index(),how='left',left_on='sku_id',right_on='sku_id')

        data3.to_csv("ProductInfo.csv")

    def produce_smple(self0):
        beginTime = datetime.datetime(2016,02,01)

        userinfo = pd.read_csv("UserInfo.csv",index_col=0)
        productinfo = pd.read_csv("ProductInfo.csv",index_col=0)

        state = True

        while state:
            print beginTime

            acDay = pd.read_csv("./actionInfo_day/Ac_"+beginTime.strftime("%Y-%m-%d")+".csv",index_col=0)
            acDay.drop('time_y',axis=1,inplace=True)

            aumerge = pd.merge(acDay,userinfo,how='left',left_on='user_id',right_on='user_id')
            aupmerge = pd.merge(aumerge,productinfo,how='left',left_on='sku_id',right_on='sku_id')

            aupmerge.to_csv("./actionInfo_day/smp_"+beginTime.strftime("%Y-%m-%d")+".csv")
            beginTime = beginTime + datetime.timedelta(days=1)

            if beginTime == datetime.datetime(2016,4,16):
                state = False

    def search_back(self,target,ds):
        pre = ds[0]
        for d in ds:
            if target <= d:
                return pre
            pre = d
        return pre

    def mergecomment(self):
        existed = ['2016-02-01', '2016-02-08', '2016-02-15', '2016-02-22', '2016-02-29', '2016-03-07',
                   '2016-03-14','2016-03-21', '2016-03-28', '2016-04-04', '2016-04-11', '2016-04-15']

        beginTime = datetime.datetime.strptime('2016-02-01','%Y-%m-%d')

        com = pd.read_csv("./JData/JData_Comment.csv")

        state = True

        while state:
            beginTimeStr = beginTime.strftime("%Y-%m-%d")
            print beginTimeStr
            smp = pd.read_csv("./actionInfo_day/smp_"+beginTimeStr+".csv",index_col=0)

            if beginTimeStr not in existed:
                backDay = self.search_back(beginTimeStr,existed)
                back_df = com[com['dt'] == backDay].copy()
                back_df.drop('dt',axis=1,inplace=True)
                dt = pd.merge(smp,back_df,how='left',left_on='sku_id',right_on='sku_id')
                dt.to_csv("./actionInfo_day/smple_"+beginTimeStr+".csv")
            else:
                back_df = com[com['dt'] == beginTimeStr].copy()
                back_df.drop('dt', axis=1, inplace=True)
                dt = pd.merge(smp,back_df,how='left',left_on='sku_id',right_on='sku_id')
                dt.to_csv("./actionInfo_day/smple_" + beginTimeStr + ".csv")

            beginTime = beginTime + datetime.timedelta(days=1)
            if beginTime == datetime.datetime(2016,04,16):
                state = False

    def produce_label(self):
        lb = pd.read_csv('./mysql/smple/label.txt',sep='\t',dtype=str)
        lbcolum = "Y,time,user_id,sku_id"
        num = 0
        with open("label_post.csv","w") as f:
            f.write(lbcolum+'\n')
            for node in lb.values:
                num += 1
                print num
                time,user_id,sku_id = node[0],node[2],node[3]
                day = datetime.datetime.strptime(time[:10],"%Y-%m-%d")
                for i in xrange(1,6):
                    d = (day - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
                    line = "1,{d},{user_id},{sku_id}".format(d=d,user_id=user_id,sku_id=sku_id)
                    f.write(line+'\n')

    def produce_labelfile(self):
        beginTime = datetime.datetime(2016,02,01)
        label = pd.read_csv("label_post.csv")
        state = True
        while state:
            print beginTime
            ac = pd.read_csv("./actionInfo_day/smple_"+beginTime.strftime("%Y-%m-%d")+".csv",index_col=0)
            ac_lb = pd.merge(ac,label,how='left',left_on=['user_id','sku_id','time_x'],right_on=['user_id','sku_id','time'])
            ac_lb['Y'] = ac_lb['Y'].map(lambda x:'1' if x == 1 else '-1')
            ac_lb.to_csv("./actionInfo_day/label_" + beginTime.strftime("%Y-%m-%d") + ".csv")
            beginTime = beginTime + datetime.timedelta(days=1)
            if beginTime == datetime.datetime(2016,04,16):
                state = False



if __name__=='__main__':
    obj = get_smple()
    obj.produce_labelfile()