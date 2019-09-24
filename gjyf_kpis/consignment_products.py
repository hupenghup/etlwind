#!/usr/bin/python
#-*-coding:utf-8-*- 
import sys
import pymysql
import datetime
import time
import os
import cx_Oracle
import smtplib
from email.mime.text import MIMEText
sys.path.append("../")
from utils.conf import wd_cur,ai_cur

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

exe_interval=300

tb_name='consignment_products'

sql="""
select   part_num  , name   ,sub_type_cd  ,prod_category  
from(
select  t3.part_num  ,t3.name   ,sub_type_cd  ,prod_category  
       ,t5.created        
       ,row_number() over(partition by t3.part_num order by t5.created) rn
 from    DW_STAGE.s_prod_int t3   
 inner join DW_STAGE.cx_sign_prod_x  t5
 on trim(t3.row_id)=trim(t5.prod_base_id)
 )
 where rn = 1
"""
cols="""
part_num  , name   ,sub_type_cd  ,prod_category
"""



def dump_oracle():

        file=open("%s.txt"%(tb_name),"w")
        total=0
        sql_ora=sql
        print(sql_ora)
        wd_cur.execute(sql_ora)

        for row in wd_cur.fetchall():
            total=total+1
   #         a=''.join('%s,'*(len(row)-1))+'%s\n'
            for i in range(len(row)):
                if i==0:
                        values="'%s'"%(row[i])
                else:
                        if row[i] is None:
                                values="%s,%s"%(values,"\\N")
                        else:
                                values="%s,'%s'"%(values,row[i])
            file.write("%s\n"%(values))

            if total % 1000==0:
                print(total)

        print(total)
        file.close()


def import2mysql():
	sql_trun="truncate table gjyf_kpis.%s"%(tb_name)
	print(sql_trun)
	ai_cur.execute(sql_trun)
	ai_cur.execute('commit')

	sql="""load data local infile '%s.txt' replace into table  gjyf_kpis.%s fields terminated by ',' enclosed by "'" (%s) """%(tb_name,tb_name,cols)
	print(sql)
	ai_cur.execute(sql)
	ai_cur.execute('commit')


try:
        print("%s %s start dump Wind all table data .........."%(tb_name , datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S") ))
        dump_oracle()

        import2mysql()
        print("%s %s start dump ODS whole table  data Success ......."%(tb_name , datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S") ))
except Exception as e:
        print("%s %s err: %s"%(tb_name ,datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"),e))
