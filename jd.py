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

def zipmap(ks,vs):
    m = {}
    for k,v  in zip(ks,vs):
        m[k] = v
    return m

def daybias(reg,day):
    pass
def find_dt(dtstr ):
    # print dt_time
    # dt = datetime.datetime.fromtimestamp(dt_time.time())
    # print dtstr

    dt = datetime.datetime.strptime(dtstr ,'%Y-%m-%d')
    for dstr in comment_dts:

        day =  datetime.datetime.strptime(dstr,'%Y-%m-%d')
        diff_cur =  (dt - day).days
        # print dt ,day , diff_cur
        if diff_cur >= 0:
            return dstr
    return comment_dts[0]

def get_label(fea_dt, sku_id,user_id , aux_df):

    day= datetime.datetime.strptime(fea_dt ,'%Y-%m-%d')
    for i in range(1,6):
        label_day = (day + datetime.timedelta(days= i )).strftime('%Y-%m-%d')

        df = aux_df[
            (aux_df['time'] == label_day ) & ( aux_df['sku_id'] == sku_id ) & (aux_df['user_id'] == user_id)
        ]
        if len(df) > 0:
            print df.head()
            return '1'
    return '-1'




"""
注意这里要生成label
"""
def build_sample_inner(in_file='out.csv'):
    comment_df = pd.read_csv(comment_csv,dtype=str)

    aux_df = pd.read_csv(label_aux_csv,dtype=str)

    aux_df['time'] = aux_df['time'].str[0:10]

    # in_file = 'out.csv'
    # if each_in != '':
    #     in_file = each_in

    # print comment_df
    with open(in_file) as f:
        c = 0
        for L in f:
            if c == 0:
                yield ','.join(full_keys)
                c+=1  # head line
                continue
            us = L.strip().split(',')

            kv = zipmap(keys,us)

            dt = kv['time'][0:10] #day

            sku_id = kv['sku_id']
            user_id = kv['user_id']

            target_dt = find_dt(dt)

            # print dt,sku_id,target_dt
            cmt = comment_df[
                (comment_df['dt'] == target_dt) & (comment_df['sku_id'] == sku_id)
            ]
            if len(cmt) > 0:

                ndarr = cmt.loc[:,['comment_num','has_bad_comment','bad_comment_rate']].values[0]
                cstr =  ','.join( [ str(int(ndarr[0])) , str(int(ndarr[1])),str(ndarr[2])  ])
            else:
                # raise 'no comment'
                cstr =  ','.join ([ '0','-1','-1' ])

            y = get_label(dt, sku_id, user_id ,aux_df )

            o = [us[0] , str(y)  ]
            o.extend(us[1:])
            o.extend(cstr.split(','))

            yield ','.join(o)

def build_sample():
    with open(full_csv , 'w') as f:
        for line in build_sample_inner():
            f.write(line + '\n')

    print 'build_sample_ok'
    # subprocess.check_call(' echo build_sample_ok | mail -s coach baipeng1@xiaomi.com ', shell=True)

def build_sample_each(each_out, each_in):
    with open(each_out , 'w') as f:
        for line in build_sample_inner(each_in):
            f.write(line + '\n')

    print 'build_sample_ok' + '\t '+ each_out


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
    elif sys.argv[1] == 'sample':
        build_sample()
    elif sys.argv[1] == 'each_sample':
        out = sys.argv[2]
        inf = sys.argv[3]
        print '*'*10
        build_sample_df(out , inf)
        print '^'*10
    elif sys.argv[1] == 'train':
        train()
    elif sys.argv[1] == 'flow':
        main()
        build_sample()
        train()
    elif sys.argv[1] == 'split':
        split_action()
    elif sys.argv[1] == 'transfer_label':
        transfer_label()
    else:
        pass
