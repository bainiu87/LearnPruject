# -*- coding: utf-8 -*-
import pandas as pd
import datetime
class get_smple(object):
    def produce_action_info(self):
        state = True
        Data = []
        beginTime = datetime.datetime(2016, 02, 01)
        lastTime = (beginTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        lastAction_df = pd.read_csv('mysql/smple/day_' + lastTime + '.txt', sep='\t',
                                    usecols=['time', 'user_id', 'sku_id', 'type'])
        lastAction_df['time'] = lastAction_df['time'].str[:10]
        Lastuser_sku_type = lastAction_df.groupby(['user_id', 'sku_id', 'type', 'time']).size().reset_index()
        Lastuser_sku_type_2 = pd.pivot_table(Lastuser_sku_type, values=0, index=['user_id', 'sku_id', 'time'],
                                             columns='type').reset_index()
        if len(Lastuser_sku_type_2.columns[3:]) != 6:
            pass
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

            df = pd.merge(user_sku_type_2, Lastuser_sku_type_2, how='left', left_on=['user_id', 'sku_id'],
                          right_on=['user_id', 'sku_id'])
            Lastuser_sku_type_2 = user_sku_type_2
            Data.append(df)
            beginTime = beginTime + datetime.timedelta(days=1)
            if beginTime == datetime.datetime(2016, 04, 16):
                state = False
        DateDt = pd.concat(Data)
        # DateDt.to_csv("ActionInfo.csv")

    def produce_user_info(self):
        userObj = pd.read_csv("JData/JData_User.csv",header=0)
        userObj['st'] = 1
        user_sex = pd.pivot_table(userObj,values='st',index=['user_id','user_reg_tm'],columns='sex')
        user_sex.columns = user_sex.columns.map(lambda x:str(x)+"_sex")
        user_age = pd.pivot_table(userObj,values='st',index=['user_id','user_reg_tm'],columns='age')
        user_lv = pd.pivot_table(userObj,values='st',index=['user_id','user_reg_tm'],columns='user_lv_cd')
        user_lv.columns = user_lv.columns.map(lambda x:str(x)+"_lv")
        Data = pd.merge(user_sex.reset_index(),user_age.reset_index(),how='left',left_on=['user_id','user_reg_tm'],right_on=['user_id','user_reg_tm'])
        AllData = pd.merge(Data,user_lv.reset_index(),how='left',left_on=['user_id','user_reg_tm'],right_on=['user_id','user_reg_tm'])

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
        # acObj = pd.read_csv("ActionInfo.csv",header=0,usecols=['1_x','1_y','2_x','2_y','3_x','3_y','4_x','4_y','5_x','5_y','6_x','6_y','sku_id','time_x','user_id'])
        # userObj = pd.read_csv("UserInfo.csv",header=0,usecols=['user_id','user_reg_tm','0.0_sex','1.0_sex','2.0_sex','-1','15-','16-25','26-35','36-45','46-55','56-','1_lv','2_lv','3_lv','4_lv','5_lv'])
        meObj = pd.read_csv("merge1.csv",header=0,index_col=0)
        proObj = pd.read_csv("ProductInfo.csv",header=0,index_col=0)
        data = pd.merge(meObj,proObj,how='left',left_on='sku_id',right_on='sku_id')
        # data2 = pd.merge(data,proObj,how='left',left_on='sku_id',right_on='sku_id')
        data.to_csv("merge.csv")

if __name__=='__main__':
    obj = get_smple()
    obj.produce_smple()
    # obj.test()