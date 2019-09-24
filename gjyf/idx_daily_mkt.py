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
    dotime = (datetime.datetime.now()  - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
else:
    dotime = sys.argv[1]

table_name = 'idx_daily_mkt'
file_name = 'idx_daily_mkt'



sql="""
select 
a.OBJECT_ID as  src_object_id,
a.s_info_windcode  as src_code,
b.s_info_code  as idx_cd,
to_char(to_date(trade_dt ,'yyyy-mm-dd'),'yyyy-mm-dd') as TRADE_DATE,
s_dq_preclose  as pre_close_idx,
s_dq_open  as OPEN_idx,
s_dq_high  as HIGHEST_Idx,
s_dq_low  as LOWEST_Idx,
s_dq_close  as CLOSE_Idx,
s_dq_amount  as TURNOVER_VALUE,
s_dq_volume as TURNOVER_VOL,
s_dq_change  as CHG,
s_dq_pctchange  as CHG_PCT,
1  as src_table,
'wind' as src_channel,
a.opdate  as src_opdate 
from
eterminal.AIndexWindIndustriesEOD  a join 
eterminal.aindexdescription b
on a.s_info_windcode=b.s_info_windcode
and a.s_info_windcode in ('881001.WI','882001.WI'
,'882002.WI'
,'882003.WI'
,'882004.WI'
,'882005.WI'
,'882006.WI'
,'882007.WI'
,'882008.WI'
,'882009.WI'
,'882010.WI'
,'882011.WI')
where to_char(a.opdate,'YYYY-MM-DD') >='%s'
""" % dotime

cols = """src_object_id,src_code,idx_cd,TRADE_DATE,pre_close_idx,OPEN_idx,HIGHEST_Idx,LOWEST_Idx,CLOSE_Idx,TURNOVER_VALUE,TURNOVER_VOL,CHG,CHG_PCT,src_table,src_channel,src_opdate"""
#unique_key = rows[0][3]
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

file_name2 = 'idx_daily_mkt2'
sql2="""
select 
a.OBJECT_ID as  src_object_id,
a.s_info_windcode  as src_code,
b.s_info_code  as idx_cd,
to_char(to_date(trade_dt ,'yyyy-mm-dd'),'yyyy-mm-dd') as TRADE_DATE,
s_dq_preclose  as pre_close_idx,
s_dq_open  as OPEN_idx,
s_dq_high  as HIGHEST_Idx,
s_dq_low  as LOWEST_Idx,
s_dq_close  as CLOSE_Idx,
s_dq_amount  as TURNOVER_VALUE,
s_dq_volume as TURNOVER_VOL,
s_dq_change  as CHG,
s_dq_pctchange  as CHG_PCT,
1  as src_table,
'wind' as src_channel,
a.opdate  as src_OPDATE 
from
eterminal.AIndexEODPrices  a join 
eterminal.aindexdescription b
on a.s_info_windcode=b.s_info_windcode
where to_char(a.opdate,'YYYY-MM-DD') >='%s'
""" % dotime

cols2="""
src_object_id,src_code,idx_cd,TRADE_DATE,pre_close_idx,OPEN_idx,HIGHEST_Idx,LOWEST_Idx,CLOSE_Idx,TURNOVER_VALUE,TURNOVER_VOL,CHG,CHG_PCT,src_table,src_channel,src_OPDATE
"""
unique_key = "src_object_id"

print(sql2)
etl2 = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql2,
          table_name=table_name,
          columns=cols2,
          unique_key=unique_key)

# 加载数据
etl2.dump_data(file_name2)

# 写入数据
etl2.import_data(file_name2)

etl2.update_unique_id()
wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
