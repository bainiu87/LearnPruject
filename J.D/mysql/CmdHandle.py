# -*- coding: utf-8 -*-
import subprocess
import sys
class CmdHandle(object):

    def __init__(self):
        # 获取文件目录
        self.fileDir = sys.path[0]

    #  生成label 文件
    def getLabel(self):
        filename = 201602
        for i in xrange (0,3):
            sql = "select time,type,user_id,sku_id from action_"+str(filename)+" where type=4"
            cmd = 'mysql -uroot -proot -Bse "set names utf8;use dj;%s;">%s\\smple\\label_%s.txt'%(sql,self.fileDir,filename)
            subprocess.check_call(cmd,shell=True)
            filename += 1

    # 联表查询商品、用户、评论详情
    def getInfo(self):
        sql = ""
if __name__=='__main__':
    cmdObject = CmdHandle()
    cmdObject.getLabel()