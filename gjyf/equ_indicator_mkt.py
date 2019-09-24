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

table_name = 'equ_indicator_mkt'
file_name = 'equ_indicator_mkt'

sql="""
select object_id as src_object_id ,
s_info_windcode as src_code  ,
replace(regexp_substr(s_info_windcode ,'.*?\.'),'.')  as  ticker,
to_date(trade_dt,'yyyy-mm-dd')  as trade_date  ,
1 as src_table,
crncy_code as currency_cd ,
s_val_mv  as market_value  ,
s_dq_mv  as  float_market_value  ,
s_pq_high_52w_  as high_price_52w  ,
s_pq_low_52w_  as low_price_52w ,
s_val_pe  as pe  ,
s_val_pb_new  as pb  ,
s_val_pe_ttm  as pe_ttm  ,
s_val_pcf_ocf  as pcf_ocf ,
s_val_pcf_ocfttm  as pcf_ocf_ttm ,
s_val_pcf_ncf  as pcf_ncf ,
s_val_pcf_ncfttm  as pcf_ncf_ttm ,
s_val_ps  as ps  ,
s_val_ps_ttm  as ps_ttm  ,
s_dq_turn  as turnover_rate ,
s_dq_freeturnover  as turnover_rate_free  ,
tot_shr_today  as total_shares  ,
float_a_shr_today  as float_shares  ,
s_dq_close_today  as close_price ,
s_price_div_dps  as price_div_dps ,
s_pq_adjhigh_52w  as high_price_adj_52w  ,
s_pq_adjlow_52w  as low_price_adj_52w ,
free_shares_today  as free_shares ,
net_profit_parent_comp_ttm  as net_profit_parent_comp_ttm  ,
net_profit_parent_comp_lyr  as net_profit_parent_comp_lyr  ,
net_assets_today  as net_assets_today  ,
net_cash_flows_oper_act_ttm  as net_cash_flows_oper_act_ttm ,
net_cash_flows_oper_act_lyr  as net_cash_flows_oper_act_lyr ,
oper_rev_ttm  as oper_rev_ttm  ,
oper_rev_lyr  as oper_rev_lyr  ,
net_incr_cash_cash_equ_ttm  as net_incr_cash_cash_equ_ttm  ,
net_incr_cash_cash_equ_lyr  as net_incr_cash_cash_equ_lyr  ,
up_down_limit_status  as up_down_limit_status  ,
Lowest_highest_status  as lowest_highest_status ,
opdate  as src_opdate ,
'wind' as src_channel
from eterminal.AShareEODDerivativeIndicator 
where to_char(opdate,'YYYY-MM-DD') >='%s'
""" % (dodate)

cols = """src_object_id,src_code,ticker,trade_date,src_table,currency_cd,market_value,float_market_value,high_price_52w,
low_price_52w,pe,pb,pe_ttm,pcf_ocf,pcf_ocf_ttm,pcf_ncf,pcf_ncf_ttm,ps,ps_ttm,turnover_rate,turnover_rate_free,
total_shares,float_shares,close_price,price_div_dps,high_price_adj_52w,low_price_adj_52w,free_shares,
net_profit_parent_comp_ttm,net_profit_parent_comp_lyr,net_assets_today,net_cash_flows_oper_act_ttm,
net_cash_flows_oper_act_lyr,oper_rev_ttm,oper_rev_lyr,net_incr_cash_cash_equ_ttm,net_incr_cash_cash_equ_lyr,
up_down_limit_status,lowest_highest_status,src_opdate,src_channel
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
