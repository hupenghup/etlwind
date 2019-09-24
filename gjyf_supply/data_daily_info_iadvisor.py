#!/usr/bin/python
# -*-coding:utf-8-*-
import sys
import pymysql
import datetime
import time
import os
import cx_Oracle
import smtplib
from email.mime.text import MIMEText
import pandas as pd

sys.path.append("../")
from utils.conf import ai_cur, ai

default_encoding = 'utf-8'

# for i in cursordb.execute(sql).fetchall():
#	print i[0].decode('GBK').encode(d_charset)


pro_name = 'iadvisor'
chk_db = 'gjyf_supply'
chk_tb = 'data_daily_info'
rlt_tb = 'table_trade_date'

dodate = datetime.datetime.now().strftime("%Y-%m-%d")

sql_rlt = "select pro_name,table_name,trade_date_col from %s.%s where  pro_name='%s'" % (chk_db, rlt_tb, pro_name)
df = pd.read_sql(sql_rlt, ai)
print(df)


def gjyf_data(pro_name):
    ai_cur.execute("delete from %s.%s where view_date='%s' and pro_name='%s'" % (chk_db, chk_tb, dodate, pro_name))

    sql = """
	SELECT  table_name FROM information_schema.tables WHERE table_schema='%s' 
	""" % (pro_name)
    print(sql)
    ai_cur.execute(sql)

    for tables in ai_cur.fetchall():
        td_flag = -1
        try:
            tb_name = tables[0]
            print(tb_name)

            sql1 = """
			select  count(*) from %s.%s  
			""" % (pro_name, tb_name)

            ai_cur.execute(sql1)
            tmp = ai_cur.fetchall()
            table_rows = tmp[0][0]

            sql_ins = """
                                                    insert into %s.%s set table_name='%s',pro_name='%s',view_date='%s',table_rows='%s'
                                                    """ % (chk_db, chk_tb, tb_name, pro_name, dodate, table_rows)
            ai_cur.execute(sql_ins)
            ai_cur.execute('commit')

            if df[df['table_name'] == tb_name].empty:
                td_flag = 0
            else:
                index = df[df['table_name'] == tb_name].index.tolist()
                print(index)
                print(df[df['table_name'] == tb_name]['trade_date_col'])
                print('---------------')
                print(df[df['table_name'] == tb_name]['trade_date_col'])
                td_col = df[df['table_name'] == tb_name]['trade_date_col'][index].tolist()[0]
                print('---------------')
                print(td_col)

                sql_ut_val = """
                            select  max(%s) from %s.%s  
                            """ % (td_col, pro_name, tb_name)
                ai_cur.execute(sql_ut_val)
                timedata = ai_cur.fetchall()
                td_val = timedata[0][0]

            if td_flag == 0:
                pass
            else:
                sql2 = """
                    update   %s.%s set  last_trade_date='%s'
                    where table_name='%s' and pro_name='%s' and view_date='%s'
                    """ % (chk_db, chk_tb, td_val, tb_name, pro_name, dodate)
                ai_cur.execute(sql2)
                ai_cur.execute('commit')

            sql_td = """
            			SELECT  count(1) FROM information_schema.COLUMNS WHERE table_schema='%s' AND column_name='update_time' and TABLE_NAME = '%s'
            			""" % (pro_name, tb_name)
            ai_cur.execute(sql_td)
            flag_td = ai_cur.fetchall()
            ftd = flag_td[0][0]

            print(ftd)
            if ftd == 1:
                sql_td_val = """
            				select  max(update_time) from %s.%s  
            				""" % (pro_name, tb_name)
                ai_cur.execute(sql_td_val)
                ut_val = ai_cur.fetchall()[0][0]

                sql3 = """
                                    	update %s.%s set  last_update_time='%s'
                                    	where table_name='%s' and pro_name='%s' and view_date='%s'
                                    	""" % (chk_db, chk_tb, ut_val, tb_name, pro_name, dodate)
                print(sql3)
                ai_cur.execute(sql3)
                ai_cur.execute('commit')


        except:
            pass

    ai_cur.execute('commit')


def main():
    gjyf_data(pro_name)


if __name__ == "__main__":

    subject = sys.argv[0].split("/")[-1]
    try:

        # begin_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        main()
        # end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    except Exception as e:
        content = """
        Time    :  %s
        err     :  %s
        """ % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), e)
        end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(e, subject)
