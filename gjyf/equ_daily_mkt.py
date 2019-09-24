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

table_name = 'equ_daily_mkt'
file_name = 'equ_daily_mkt'

sql="""
select 
a.OBJECT_ID  as src_object_id,
a.s_info_windcode as src_code,
replace(regexp_substr(a.s_info_windcode ,'.*?\.'),'.')  as  ticker,
to_date(trade_dt,'YYYY-MM-DD') as TRADE_DATE,
'wind' as src_channel,
1 as src_table,
a.crncy_code as CURRENCY_CD,
s_dq_preclose as PREV_CLOSE_PRICE,
s_dq_open as OPEN_PRICE,
s_dq_high as HIGH_PRICE,
s_dq_low as LOW_PRICE,
s_dq_close as CLOSE_PRICE,
s_dq_change as CHANGE_PRICE,
s_dq_pctchange as CHANGE_PCT,
s_dq_volume as TURNOVER_VOL,
s_dq_amount as TURNOVER_AMOUNT,
s_dq_adjpreclose as PRE_CLOSE_PRICE_ADJ,
s_dq_adjopen as OPEN_PRICE_ADJ,
s_dq_adjhigh as HIGH_PRICE_ADJ,
s_dq_adjlow as LOW_PRICE_ADJ,
s_dq_adjclose as CLOSE_PRICE_ADJ,
s_dq_adjfactor as ADJ_FACTOR,
s_dq_avgprice as AVG_PRICE,
s_dq_tradestatus as TRADE_STATUS,
a.opdate as src_opdate
from eterminal.ASHAREEODPRICES  a  join  eterminal.asharedescription b
on  a.s_info_windcode=b.s_info_windcode 
where  
 to_char(a.opdate,'YYYY-MM-DD') >='%s' 
""" % (dodate)

cols="""src_object_id,src_code,ticker,TRADE_DATE,src_channel,src_table,CURRENCY_CD,PREV_CLOSE_PRICE,OPEN_PRICE,HIGH_PRICE,LOW_PRICE,CLOSE_PRICE,
CHANGE_PRICE,CHANGE_PCT,TURNOVER_VOL,TURNOVER_AMOUNT,PRE_CLOSE_PRICE_ADJ,OPEN_PRICE_ADJ,HIGH_PRICE_ADJ,LOW_PRICE_ADJ,CLOSE_PRICE_ADJ,ADJ_FACTOR,AVG_PRICE,TRADE_STATUS,src_opdate
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
