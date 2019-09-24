#! /usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import warnings
import datetime

sys.path.append("../")
from utils.conf import wd, wd_cur, ai, ai_cur
from utils.etl import ETL

warnings.filterwarnings('ignore')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

if len(sys.argv) == 1:
    dodate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
else:
    dodate = sys.argv[1]

table_name = 'closed_fund_daily_mkt'
file_name = 'closed_fund_daily_mkt'

dodate = '2019-01-01'

sql = """
select
replace(regexp_substr(a.s_info_windcode ,'.*?\.'),'.')  as  ticker,
to_date(TRADE_DT,'yyyy-mm-dd')  as  trade_date  ,
CRNCY_CODE  as currency_cd ,
S_DQ_PRECLOSE  as  prev_close_price,
S_DQ_OPEN  as  open_price  ,
S_DQ_HIGH   as   high_price  ,
S_DQ_LOW   as   low_price ,
S_DQ_CLOSE  as   close_price ,
S_DQ_CHANGE as   change_price  ,
S_DQ_PCTCHANGE as    change_pct  ,
S_DQ_VOLUME   as   turnover_vol  ,
S_DQ_AMOUNT  as  turnover_amount ,
S_DQ_ADJPRECLOSE  as   pre_close_price_adj ,
S_DQ_ADJOPEN as   open_price_adj  ,
S_DQ_ADJHIGH  as   high_price_adj  ,
S_DQ_ADJLOW as   low_price_adj ,
S_DQ_ADJCLOSE as   close_price_adj ,
S_DQ_ADJFACTOR as    adj_factor  ,
TRADES_COUNT as    trades_count  ,
DISCOUNT_RATE as  discount_rate ,
'wind' as    src_channel,
1 as    src_table,
a.object_id as    src_object_id,
a.s_info_windcode  as         src_code,
a.opdate  as    src_opdate
from  eterminal.ChinaClosedFundEODPrice a 
where to_char(a.opdate,'YYYY-MM-DD') >= '%s'
""" % (dodate)

cols = """ticker,trade_date,currency_cd,prev_close_price,open_price,high_price,low_price,close_price,change_price,change_pct,turnover_vol,
turnover_amount,pre_close_price_adj,open_price_adj,high_price_adj,low_price_adj,close_price_adj,adj_factor,trades_count,discount_rate,src_channel,
src_table,src_object_id,src_code,src_opdate
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

# etl.update_unique_id()
wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
