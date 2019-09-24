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

table_name = 'jx_consult_info'
file_name = 'jx_consult_info'

sql="""
select * from (select substr(a.s_info_windcode,1,6) as code , a.s_info_windcode  as  wind_code,to_date(a.trade_dt,'yyyy-mm-dd') as trade_dt,s_dq_amount *1000 as volume_transaction,s_dq_volume *100 as transaction_number ,s_val_pe_ttm as  pe_ratio ,s_val_pb_new  as pb_ratio,
s_val_mv *10000  as market_value 
,case when nvl(s_dq_close,0) = 0 or nvl(c.sumdiv,0) = 0 then 0 else round(c.sumdiv / s_dq_close,6) end as dividend_yield
,tot_shr_today *10000 as total_capital,b.float_a_shr_today *10000  as circulation_capital ,s_dq_adjclose as closing_price ,s_dq_close as nofq_closing_price，a.s_dq_adjfactor  as adj_factor，a.object_id as src_object_id, a.opdate as src_opdate
 from eterminal.AShareEODPrices a join eterminal.AShareEODDerivativeIndicator  b
on a.s_info_windcode=b.s_info_windcode and a.trade_dt=b.trade_dt 
left join (select S_INFO_WINDCODE,substr(REPORT_PERIOD,1,4) as reportyear,sum(CASH_DVD_PER_SH_AFTER_TAX)as sumdiv 
from eterminal.AShareDividend
where S_DIV_PROGRESS='3'
group by  S_INFO_WINDCODE，substr(REPORT_PERIOD,1,4))c
on  a.s_info_windcode=c.s_info_windcode
and substr(a.trade_dt,1,4)=reportyear+1)
where to_char(src_opdate,'YYYY-MM-DD') >='%s'
""" % (dodate)

cols="""code,wind_code,trade_dt,volume_transaction,transaction_number, pe_ratio,pb_ratio, market_value,dividend_yield,total_capital,circulation_capital,
closing_price,nofq_closing_price,adj_factor,src_object_id, src_opdate
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
