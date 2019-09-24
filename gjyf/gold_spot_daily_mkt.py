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

table_name = 'gold_spot_daily_mkt'
file_name = 'gold_spot_daily_mkt'

sql="""
select  
a.OBJECT_ID  as src_object_id,
a.s_info_windcode as src_code,
to_date(trade_dt,'yyyy-mm-dd') as trade_date,
replace(regexp_substr(a.s_info_windcode ,'.*?\.'),'.')  as  ticker, 
'wind' as src_channel,
1 as src_table,
s_dq_open as OPEN_PRICE,
s_dq_high as HIGH_PRICE,
s_dq_low as LOW_PRICE,
s_dq_close as CLOSE_PRICE,
s_dq_avgprice  as avg_price,
s_dq_oi  as open_interest,
s_dq_volume as TURNOVER_VOL,
s_dq_amount as turnover_amt，
s_dq_settle as settle_price,
del_amt  as del_amt,
a.opdate as src_opdate
 from   eterminal.CGoldSpotEODPrices  a   join eterminal.CGoldSpotDescription    b
 on a.s_info_windcode=b.s_info_windcode   where   to_char(a.opdate,'YYYY-MM-DD') >='%s'
""" % (dodate)

cols = """src_object_id,src_code,trade_date, ticker, src_channel,src_table, OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,avg_price,open_interest,
 TURNOVER_VOL,turnover_amt,settle_price,del_amt,src_opdate
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
