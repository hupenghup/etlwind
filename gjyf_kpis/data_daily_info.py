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

sys.path.append("../")
from utils.conf import wd_cur, ai_cur

default_encoding = 'utf-8'

# for i in cursordb.execute(sql).fetchall():
#	print i[0].decode('GBK').encode(d_charset)


pro_names = ['gjyf']
chk_db = 'gjyf_kpis'
chk_tb = 'data_daily_info'

dodate = datetime.datetime.now().strftime("%Y-%m-%d")


def gjyf_data(pro_name):

    ai_cur.execute("delete from %s.%s where view_date='%s' and pro_name='%s'" % (chk_db, chk_tb, dodate, pro_name))

    sql = """
	SELECT  table_name FROM information_schema.COLUMNS WHERE table_schema='%s' AND column_name='src_opdate' 
	""" % (pro_name)
    print(sql)
    ai_cur.execute(sql)

    for tables in ai_cur.fetchall():
        try:
            tb_name = tables[0]
            print(tb_name)

            sql1 = """
			select  count(*) from %s.%s  
			""" % (pro_name, tb_name)

            ai_cur.execute(sql1)
            tmp = ai_cur.fetchall()
            table_rows = tmp[0][0]




            sql_ut_val = """
						select  max(update_time),max(create_time),max(src_opdate) from %s.%s  
						""" % (pro_name, tb_name)
            ai_cur.execute(sql_ut_val)
            timedata = ai_cur.fetchall()
            ut_val = timedata[0][0]
            print(ut_val)

            ct_val = timedata[0][1]
            op_val = timedata[0][2]

            print(ct_val, op_val)
            #td_val = None

            sql_td = """
            			SELECT  count(1) FROM information_schema.COLUMNS WHERE table_schema='%s' AND column_name='trade_date' and TABLE_NAME = '%s'
            			""" % (pro_name, tb_name)
            ai_cur.execute(sql_td)
            flag_td = ai_cur.fetchall()
            ftd = flag_td[0][0]
            print(ftd)
            if ftd == 1:
                sql_td_val = """
            				select  max(trade_date) from %s.%s  
            				""" % (pro_name, tb_name)
                ai_cur.execute(sql_td_val)
                td_val = ai_cur.fetchall()[0][0]
                sql2 = """
                	insert into %s.%s set table_name='%s',pro_name='%s',view_date='%s',table_rows='%s',last_src_opdate='%s',last_insert_time='%s',last_update_time='%s',last_trade_date='%s'
                	""" % (chk_db, chk_tb, tb_name, pro_name, dodate, table_rows, op_val, ct_val, ut_val, td_val)
            else :
                sql2 = """
                    insert into %s.%s set table_name='%s',pro_name='%s',view_date='%s',table_rows='%s',last_src_opdate='%s',last_insert_time='%s',last_update_time='%s'
                    """ % (chk_db, chk_tb, tb_name, pro_name, dodate, table_rows, op_val, ct_val, ut_val)
            ai_cur.execute(sql2)
        except:
            pass

    ai_cur.execute('commit')


def main():
    for pro_name in pro_names:
        begin_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        gjyf_data(pro_name)
        end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
