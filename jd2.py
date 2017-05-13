#encoding=utf-8
import pandas as pd
import  numpy as np
import datetime,sys,subprocess
import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import  roc_auc_score

root = '/Users/baipeng/PycharmProjects/try/JData/'
user_csv = '{root}/JData_User.csv'.format(root=root)
prod_csv = '{root}/JData_Product.csv'.format(root=root)
comment_csv = '{root}/JData_Comment.csv'.format(root=root)

action_csv = '{root}/JData_Action.csv'.format(root=root) #全部的action放一起

headact_csv = '{root}/J20.csv'.format(root=root)
act_error_csv = '{root}/Je.csv'.format(root=root)

full_csv = '{root}/full.20.csv'.format(root=root)
label_aux_csv = '{root}/label_aux.csv'.format(root=root)
label_pos_csv = '{root}/label_pos.csv'.format(root=root)

"""
    dt,sku_id,comment_num,has_bad_comment,bad_comment_rate

"""
comment_dts = '2016-02-01,2016-02-08,2016-02-15,2016-02-22,2016-02-29,2016-03-07,2016-03-14,2016-03-21,2016-03-28,2016-04-04,2016-04-11,2016-04-15'.split(',')

keys = ',user_id,sku_id,time,model_id,type,cate_x,brand_x,age,sex,user_lv_cd,user_reg_tm,a1,a2,a3,cate_y,brand_y'.split(',')
full_keys = ',Y,user_id,sku_id,time,model_id,type,cate_x,brand_x,age,sex,user_lv_cd,user_reg_tm,a1,a2,a3,cate_y,brand_y,comment_num,has_bad_comment,bad_comment_rate'.split(',')






def transfer_label():
    df = pd.read_csv(label_aux_csv,dtype=str, index_col=0)
    # print df
    c = 0
    with open(label_pos_csv, 'w') as f:
        keys = 'Y,time,user_id,sku_id'
        f.write(keys+'\n')
        for node in df.values:
            print c
            c+=1
            time,user_id ,sku_id = node[0],node[1],node[2]
            # print time ,user_id,sku_id
            day = datetime.datetime.strptime(time[0:10],'%Y-%m-%d')
            for i in range(1,6):
                d = (day -  datetime.timedelta(days = i )).strftime('%Y-%m-%d')
                line = '1,{d},{user_id},{sku_id}'.format(d=d,user_id=user_id,sku_id=sku_id)

                f.write(line+'\n')


"""
Use Pandas to speed.
"""
def build_sample_df(each_out,each_in):
    comment_df = pd.read_csv(comment_csv,dtype=str)

    aux_df = pd.read_csv(label_pos_csv,dtype=str)
    print aux_df.head()
    aux_df['time'] = aux_df['time'].str[0:10]


    action_user_prod_df = pd.read_csv(each_in,index_col=0,dtype=str) #partial with index

    action_user_prod_df['time'] = action_user_prod_df['time'].str[0:10] # day

    act_comment_df = pd.merge(action_user_prod_df , comment_df , how= 'left', left_on=['time','sku_id'], right_on=['dt','sku_id'])

    print act_comment_df.columns,act_comment_df.count()
    #with label
    sample_df = pd.merge(act_comment_df , aux_df , how='left' , left_on = ['time', 'user_id','sku_id'] , right_on= ['time', 'user_id','sku_id'])
    # print sample_df.columns ,sample_df.count()
    sample_df['Y'] = sample_df['Y'].map(lambda x: '1' if x == '1' else '-1')
    print sample_df.head()
    sample_df.to_csv(each_out)

def split_action():

    act_user_prod_df = pd.read_csv('out.csv',index_col=0)


    for name , g in act_user_prod_df.groupby( act_user_prod_df['time'].str[0:10] ):
        g.to_csv( './partial/' + name + '.csv')


"""
第一列是 label
后续是  特征
直接用scikit 学习

time不能做特征
"""
def train():
    fs = []
    i = -1
    with open(full_csv,'r') as f:
        for L in f:
            full_keys = L.strip().split(',')
            break
    print full_keys
    for k in full_keys:
        i += 1
        # if i == 0:
        #     continue
        if k in ['time', 'Y','',  'user_reg_tm']: #这些不作为特征
            continue
        fs.append( i )
    print fs


    full_df = pd.read_csv(full_csv, dtype=str )

    print full_df.head()
    y = full_df['Y']
    print y.values

    print len(y.values), len(full_df)


    y_real = [ int(i)  for i in y.values ]

    print y.head()
    X = full_df.iloc[:, fs]
    print 'dummies' * 10
    X =  pd.get_dummies(X,sparse=True)
    print 'dummies ok'
    print X.head()
    lr  = LogisticRegression(C=1 , penalty='l1', tol=0.0001,verbose=True)
    lr.fit(X, y)


    y_hat = lr.predict_proba(X)
    y_pos_hat = [ i[1] for i in y_hat]



    print 'auc = {0}'.format(roc_auc_score(y_real ,y_pos_hat))
    # predict X
    # ds  = []
    # day = datetime.datetime.strptime('2017-04-15', '%Y-%m-%d')
    # for i in range(5):
    #     d = (day + datetime.timedelta(days = i )).strftime('%Y-%m-%d')
    #     ds.append(d)
    # pred_df = full_df[ full_df['time'].str[0:10] in d ]
    #
    # pred_X = pred_df.iloc[:,fs]
    #
    # #具体是probability还是别的，需要在考虑
    # proba = lr.predict_proba(pred_X)
    # print proba[1:10]
    # print 'train_ok'
    # subprocess.check_call(' echo train_ok | mail -s coach baipeng1@xiaomi.com ', shell=True)





def main():
    print '-'*10 + 'main ' + '-'*10
    user_df = pd.read_csv(user_csv)
    prod_df = pd.read_csv(prod_csv)
    comment_df = pd.read_csv(comment_csv)

    print comment_df.groupby('dt').count()

    # action_df = pd.read_csv(headact_csv)
    action_df = pd.read_csv(action_csv)

    label_aux_df = action_df[ action_df['type'] == 4 ][['time','user_id','sku_id']]
    label_aux_df.to_csv(label_aux_csv)

    act_user_df = pd.merge(action_df,user_df,left_on='user_id',right_on='user_id')


    act_user_prod_df = pd.merge(act_user_df,prod_df,left_on='sku_id',right_on='sku_id')




    act_user_prod_df.to_csv('out.csv')

    # subprocess.check_call(' echo main_ok | mail -s coach baipeng1@xiaomi.com ',shell=True)
    print act_user_prod_df.head()



if __name__ == '__main__':


    if sys.argv[1] == 'main':
        main()

    elif sys.argv[1] == 'each_sample':
        out = sys.argv[2]
        inf = sys.argv[3]
        print '*'*10
        build_sample_df(out , inf)
        print '^'*10
    elif sys.argv[1] == 'train':
        train()
    elif sys.argv[1] == 'flow':
        raise  'Not Support'
        main()
        # build_sample()
        train()
    elif sys.argv[1] == 'split':
        split_action()
    elif sys.argv[1] == 'transfer_label':
        transfer_label()
    else:
        pass
