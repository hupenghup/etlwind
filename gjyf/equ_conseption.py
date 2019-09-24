#! /usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import warnings
import datetime


sys.path.append("../")
from utils.conf import  wd,wd_cur,ai,  ai_cur
from utils.etl import ETL

warnings.filterwarnings('ignore')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

if len(sys.argv) == 1:
    dodate = (datetime.datetime.now()  - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
else:
    dodate = sys.argv[1]

table_name = 'equ_conseption'
file_name = 'equ_conseption'

sql="""
select  replace(regexp_substr(s_info_windcode,'.*?\.'),'.') as  ticker,
wind_sec_code  as src_sec_code,
wind_sec_name  as  sec_name,
to_date(entry_dt,'yyyy-mm-dd')  as  into_date,
to_date(remove_dt,'yyyy-mm-dd')  as  remove_date,
cur_sign   as cur_sign
,'wind' as  src_channel, 1 AS src_table, object_id as src_object_id ,  
s_info_windcode  as         src_code, 
opdate as src_opdate  
 from   eterminal.AShareConseption  where   to_char(opdate,'YYYY-MM-DD') >='%s' 
""" % (dodate)

cols="""
 ticker,src_sec_code,sec_name,
into_date,remove_date,cur_sign,src_channel,src_table,src_object_id ,src_code,src_opdate  
"""

unique_key = "src_object_id"

print(sql)
etl = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql,
          table_name=table_name,
          columns=cols,
          unique_key=unique_key)

# 加载数据
etl.dump_data(file_name)

# 写入数据
etl.import_data(file_name)

#etl.update_unique_id()
wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
