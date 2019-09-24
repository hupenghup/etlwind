#! /usr/local/bin/python
# -*- coding: utf-8 -*-

import platform
import datetime


class dataProc(object):
    def __init__(self,
                 tgt_cur=None,
                 tgt_conn=None):
        # data target database cursor
        self.tgt_cur = tgt_cur
        # data target database connect
        self.tgt_conn = tgt_conn


    def exec_sql(self,sql):
        """
        :return: 执行sql
        """
        print("%s %s" % ("------------", datetime.datetime.now()))
        print(sql)
        self.tgt_cur.execute(sql)
        self.tgt_conn.commit()
        print("%s %s~~~ %s" % (self.tgt_cur.rowcount,"rows done", datetime.datetime.now()))


    def exec_delete(self,table_name,col_name,dodate):
        """
        :return: 删除此表此日期之后的数据，以便插入新的数据
        """
        print("%s  %s" % ("------------", datetime.datetime.now()))
        sql ="""
            delete from %s where %s >= '%s'
            """ % (table_name,col_name,dodate)
        print(sql)
        self.tgt_cur.execute(sql)
        self.tgt_conn.commit()
        print("%s %s~~~ %s" % (self.tgt_cur.rowcount,"rows done", datetime.datetime.now()))


    def exec_trun(self,table_name):
        """
        :return: 删除此表所有数据，以便插入新的数据
        """
        print("%s  %s" % ("------------", datetime.datetime.now()))
        sql ="""
            truncate  table %s  
            """ % (table_name)
        print(sql)
        self.tgt_cur.execute(sql)
        self.tgt_conn.commit()
        print("%s %s~~~ %s" % (self.tgt_cur.rowcount,"rows done", datetime.datetime.now()))
