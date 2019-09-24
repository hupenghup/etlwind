#! /usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import warnings
from datetime import datetime


sys.path.append("../")
from utils.conf import  wd,wd_cur,ai,  ai_cur
from utils.etl import ETL

warnings.filterwarnings('ignore')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

if len(sys.argv) == 1:
    dodate = (datetime.datetime.now()  - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
else:
    dodate = sys.argv[1]

table_name = 'sw_idx_daily_mkt'
file_name = 'sw_idx_daily_mkt'

sql="""
select 
a.OBJECT_ID as  src_object_id,
a.s_info_windcode  as src_code,
b.s_info_code  as idx_cd,
to_date(trade_dt ,'yyyy-mm-dd') as TRADE_DATE,
s_dq_preclose  as pre_close_idx,
s_dq_open  as OPEN_idx,
s_dq_high  as HIGHEST_Idx,
s_dq_low  as LOWEST_Idx,
s_dq_close  as CLOSE_Idx,
s_dq_amount *1000 as TURNOVER_VALUE,
s_dq_volume *100 as TURNOVER_VOL,
s_val_pe  as pe,
s_val_pb  as pb,
s_dq_mv as float_mv,
s_val_mv  as total_mv,
1  as src_table,
'wind' as src_channel,
a.opdate  as src_OPDATE 
from  eterminal.ASWSIndexEOD	a join eterminal.AindexDescription b
on  a.s_info_windcode=b.s_info_windcode 
where to_char(a.opdate,'YYYY-MM-DD') >='%s'
""" % (dodate)

cols="""src_object_id,src_code,idx_cd,TRADE_DATE,pre_close_idx,OPEN_idx,HIGHEST_Idx,LOWEST_Idx,CLOSE_Idx,TURNOVER_VALUE,TURNOVER_VOL, pe,pb,float_mv,total_mv,src_table,src_channel,src_OPDATE"""


print(sql)
etl = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql,
          table_name=table_name,
          columns=cols,
          unique_key=None)

# 加载数据
etl.dump_data(file_name)

# 写入数据
etl.import_data(file_name)

#etl.update_unique_id()
wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
