# -*- coding: utf-8 -*-
import subprocess
import sys
import datetime
class CmdHandle(object):

    def __init__(self):
        # 获取文件目录
        self.fileDir = sys.path[0]

    #  生成label 文件
    def getLabel(self):
        filename = 201602
        for i in xrange (0,3):
            sql = "select time,type,user_id,sku_id from action_"+str(filename)+" where type=4"
            cmd = 'mysql -uroot -proot -Bse "set names utf8;use dj;%s;">>%s\\smple\\label.txt'%(sql,self.fileDir)
            subprocess.check_call(cmd,shell=True)
            filename += 1

    # 生成一天的action 文件
    def produceDayAction(self,begin,over):
        sql = "select user_id,sku_id,time,model_id,type,cate,brand from action_201604 where '%s'<=time and time<'%s'"%(begin,over)
        cmd = 'mysql -uroot -proot -Bse "set names utf8;use dj;%s;">%s\\smple\\day_%s.txt' % (sql, self.fileDir,str(begin)[:10])
        subprocess.check_call(cmd, shell=True)

    # 加载一天的数据
    def loadDayToMysql(self,begin):
        sql = "load data local infile 'D:\\\\github\\\\LearnPruject\\\\J.D\\\\mysql\\\\smple\\\\day_%s.txt' into table day_%s FIELDS ESCAPED BY '\\\\' TERMINATED BY '\\t' LINES TERMINATED BY '\\n' ( user_id , sku_id ,time, model_id ,type , cate , brand )"%(str(begin)[:10],str(begin)[:10].replace('-','_'))
        cmd = 'mysql -uroot -proot -Bse "set names utf8;use dj;%s;"'%(sql)
        subprocess.check_call(cmd, shell=True)

    #创建一天的action 表
    def createTable(self,begin):
        sql = "create table day_%s(id int not null auto_increment,user_id int,sku_id int,time datetime,model_id varchar(20),type int,cate int,brand int,primary key (id))"%(str(begin)[:10].replace('-','_'))
        cmd = 'mysql -uroot -proot -Bse "set names utf8;use dj;%s;"' % (sql)
        subprocess.check_call(cmd, shell=True)

    #开始生产 每天的数据
    def timeToTime(self):
        begin = datetime.datetime(2016,04,01,00,00,00)
        state = True
        while state:
            over = begin + datetime.timedelta(days=1)
            self.createTable(begin=begin)
            self.produceDayAction(begin=begin,over=over)
            self.loadDayToMysql(begin=begin)
            begin = over
            if begin == datetime.datetime(2016,04,16,00,00,00):
                state = False

    # 联表查询商品、用户、评论详情
    def getInfo(self):
        sql = ""
if __name__=='__main__':
    cmdObject = CmdHandle()
    # cmdObject.timeToTime()
    # cmdObject.getLabel()
    #处理201603表中2月的数据
    # begin = datetime.datetime(2016,02,29,00,00,00)
    # over = begin + datetime.timedelta(days=1)
    # cmdObject.produceDayAction(begin=begin,over=over)
    # cmdObject.loadDayToMysql(begin=begin)

    # 处理201604表中3月的数据
    # begin = datetime.datetime(2016,03,31,00,00,00)
    # # over = begin + datetime.timedelta(days=1)
    # # cmdObject.produceDayAction(begin=begin,over=over)
    # cmdObject.loadDayToMysql(begin=begin)