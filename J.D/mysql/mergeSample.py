# -*- coding: utf-8 -*-
import MySQLdb
import datetime
class mergeSample(object):

    def __init__(self):
        self.mysql_conn = MySQLdb.connect(host="127.0.0.1", user="root", passwd="root", db="dj", charset="utf8")
        self.cursor = self.mysql_conn.cursor()

    def joinDays(self,dayDate,beginTime):
        fiveBeginTime = beginTime + datetime.timedelta(days=3)
        fiveOverTime = beginTime + datetime.timedelta(days=8)
        fileDir = "./smple/day_%s.txt"%(str(dayDate)[:10])
        fileName = "./smple/Smple_%s.csv"%(str(dayDate)[:7])
        print "当前时间数据读取开始："+fileDir
        print "数据储存位置："+fileName
        with open(fileDir,"r") as F:
            for l in F:
                rowList = l.strip().split("\t")
                userInfo = list(self.getUserInfo(user_id=rowList[0]))
                productInfo = list(self.getProductInfo(sku_id=rowList[1]))
                commentInfo = list(self.getCommentInfo(dayDate=str(dayDate)[:10],sku_id=rowList[1]))
                labelState = self.getLabel(user_id=rowList[0],sku_id=rowList[1],beginTime=fiveBeginTime,overTime=fiveOverTime)
                data = labelState+rowList+userInfo+productInfo+commentInfo
                dataStr = ",".join(map(str,data))+"\n"
                with open(fileName,"a+") as f:
                    f.write(dataStr)
        print "============完成==========="

    #获取用户信息
    def getUserInfo(self,user_id):
        sql = "select age,sex,user_lv_cd,user_reg_tm from user where user_id=%s"%(user_id)
        self.cursor.execute(sql)
        result = list(self.cursor.fetchall()[0])
        result[0] = self.setage(str=result[0])
        return result

    #设置年龄区段
    def setage(self,str):
        if str == -1:
            return -1
        else:
            if "16" in str:
                return 1
            elif "26" in str:
                return 2
            elif "36" in str:
                return 3
            elif "46" in str:
                return 4
            elif "56" in str:
                return 5

    #获取商品信息
    def getProductInfo(self,sku_id):
        sql = "select a1,a2,a3,cate,brand from product where sku_id=%s"%(sku_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if len(result)==0:
            return ['','','','','']
        else:
            return result[0]

    #获取评论信息
    def getCommentInfo(self,dayDate,sku_id):
        sql = "select dt,comment_num,has_bad_comment,bad_comment_rate from comment where sku_id=%s"%(sku_id)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if len(result) == 0:
            return ['','','','']
        else:
            for l in result:
                if str(l[0]-datetime.timedelta(days=3)) <= dayDate and dayDate < str(l[0]+datetime.timedelta(days=4)):
                    return l

    #获取label
    def getLabel(self,user_id,sku_id,beginTime,overTime):
        with open("./smple/label.txt","r") as F:
            for l in F:
                labelList = l.strip().split("\t")
                if str(beginTime) <= labelList[0] and labelList[0] < str(overTime):
                    if labelList[2] == user_id and labelList[3] == sku_id:
                        return [1]
                    else:
                        return [-1]

    #按时间生产数据
    def timeToTime(self):
        beginTime = datetime.datetime(2016,01,31,00,00,00)
        gameOver = datetime.datetime(2016,04,16,00,00,00)
        state = True
        while state:
            print "三天开始时间："+str(beginTime)
            for i in xrange(0,3):
                days = i
                dayDate = beginTime + datetime.timedelta(days=days)
                self.joinDays(dayDate=dayDate,beginTime=beginTime)
            beginTime = beginTime + datetime.timedelta(days=1)
            if beginTime == gameOver:
                state = False

if __name__=='__main__':
    Obj = mergeSample()
    Obj.timeToTime()
