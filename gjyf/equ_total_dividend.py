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

table_name = 'equ_total_dividend'
file_name = 'equ_total_dividend'

sql="""
select  replace(regexp_substr(s_info_windcode ,'.*?\.'),'.')  as  ticker, S_INFO_WINDCODE  as  src_code ,substr(REPORT_PERIOD,1,4) as report_year,sum(CASH_DVD_PER_SH_AFTER_TAX)as total_div ,1  as src_table,
'wind' as src_channel,max(opdate) as  src_opdate，concat(S_INFO_WINDCODE ,substr(REPORT_PERIOD,1,4))  as src_object_id 
from eterminal.AShareDividend
where S_DIV_PROGRESS='3' and  to_char(opdate,'YYYY-MM-DD') >='%s' 
group by  S_INFO_WINDCODE，substr(REPORT_PERIOD,1,4) 
""" % (dodate)

cols="""ticker,src_code ,report_year,total_div ,src_table,src_channel,src_opdate, src_object_id  """

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

etl.update_unique_id()
wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
