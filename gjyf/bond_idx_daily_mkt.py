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

table_name = 'bond_idx_daily_mkt'
file_name = 'bond_idx_daily_mkt'

sql="""
select 
OBJECT_ID as  src_object_id,
s_info_windcode  as src_code,
to_date(trade_dt ,'yyyy-mm-dd') as TRADE_DATE,
replace(regexp_substr(s_info_windcode,'.*?\.'),'.')  as idx_cd,
s_dq_preclose  as pre_close_idx,
s_dq_open  as OPEN_idx,
s_dq_high  as HIGHEST_Idx,
s_dq_low  as LOWEST_Idx,
s_dq_close  as CLOSE_Idx,
s_dq_amount  as TURNOVER_VALUE,
s_dq_volume as TURNOVER_VOL,
s_dq_change  as CHG,
s_dq_pctchange  as CHG_PCT,
'wind' as    src_channel,
1 as    src_table,
opdate  as    src_opdate
from
eterminal.CBIndexEODPrices   where   to_char(opdate,'YYYY-MM-DD') >='%s'
""" % (dodate)

cols = """src_object_id, src_code,TRADE_DATE,idx_cd,pre_close_idx,OPEN_idx,HIGHEST_Idx,LOWEST_Idx,CLOSE_Idx,TURNOVER_VALUE,TURNOVER_VOL,CHG,CHG_PCT,src_channel,src_table,
src_opdate
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
