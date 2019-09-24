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

table_name = 'idx_weight'
file_name = 'idx_weight'

sql="""
SELECT
a.OBJECT_ID  AS SRC_OBJECT_ID,
replace(regexp_substr(a.s_info_windcode,'.*?\.'),'.')  AS idx_cd,
replace(regexp_substr(a.s_con_windcode ,'.*?\.'),'.')  as  ticker,
s_con_windcode as SRC_CODE,
to_date(trade_dt,'yyyy-mm-dd')  as TRADE_DATE,
weight as WEIGHT,
tot_mv  as T_SHARES,
free_shr_ratio as TRAD_RATIO,
weightfactor as W_FACTOR,
closevalue as CLOSE_PRICE,
open_adjusted  as AD_OPEN_REF_P,
shr_calculation as AD_SHARES,
tot_mv  as T_MKT_V,
mv_calculation as AD_MKT_V,
'WIND'  AS SRC_CHANNEL,
2  as SRC_TABLE,
a.opdate  as SRC_OPDATE
from  eterminal.AIndexCSI800Weight   a join  eterminal.asharedescription b
on  a.s_con_windcode=b.s_info_windcode
 where to_char(a.opdate,'YYYY-MM-DD') >='%s'
union 
 select a.OBJECT_ID  AS src_object_id ,
replace(regexp_substr(a.s_info_windcode,'.*?\.'),'.')  AS idx_cd,
replace(regexp_substr(a.s_con_windcode ,'.*?\.'),'.')  as  ticker, 
s_con_windcode as src_code ,
to_date(trade_dt,'yyyy-mm-dd') as trade_date,
i_weight as WEIGHT,
s_in_index AS AD_SHARES,
i_weight_11 AS T_SHARES, 
i_weight_12 AS TRAD_RATIO,
i_weight_14 AS W_FACTOR,
i_weight_15 AS CLOSE_PRICE,
i_weight_16 AS AD_OPEN_REF_P,
i_weight_17 AS T_MKT_V, 
i_weight_18 AS AD_MKT_V,
'wind'  as src_channel,
1 as src_table,
a.OPDATE AS src_OPDATE
from  eterminal.AIndexHS300Weight 
 a join  eterminal.asharedescription b
on  a.s_con_windcode=b.s_info_windcode
 where to_char(a.opdate,'YYYY-MM-DD') >='%s'
union
SELECT
a.OBJECT_ID  AS SRC_OBJECT_ID,
replace(regexp_substr(a.s_info_windcode,'.*?\.'),'.')  as idx_cd, 
replace(regexp_substr(a.s_con_windcode ,'.*?\.'),'.')  as  ticker,
s_con_windcode as SRC_CODE,
to_date(trade_dt,'yyyy-mm-dd')  as TRADE_DATE,
weight as WEIGHT,
open_adjusted  as AD_OPEN_REF_P,
tot_mv  as T_SHARES,
free_shr_ratio as TRAD_RATIO,
weightfactor as W_FACTOR,
closevalue as CLOSE_PRICE,
shr_calculation as AD_SHARES,
tot_mv  as T_MKT_V,
mv_calculation as AD_MKT_V,
'WIND'  AS SRC_CHANNEL,
4  as SRC_TABLE,
a.opdate  as SRC_OPDATE
from  eterminal.AindexCSI500Weight  a join  eterminal.asharedescription b
on  a.s_con_windcode=b.s_info_windcode 
 where to_char(a.opdate,'YYYY-MM-DD') >='%s'
 union
SELECT
a.OBJECT_ID  AS SRC_OBJECT_ID,
replace(regexp_substr(a.s_info_windcode,'.*?\.'),'.')  as idx_cd,
replace(regexp_substr(a.s_con_windcode ,'.*?\.'),'.')  as  ticker, 
s_con_windcode as SRC_CODE,
to_date(trade_dt,'yyyy-mm-dd')  as TRADE_DATE,
weight as WEIGHT,
open_adjusted  as AD_OPEN_REF_P,
tot_mv  as T_SHARES,
free_shr_ratio as TRAD_RATIO,
weightfactor as W_FACTOR,
closevalue as CLOSE_PRICE,
shr_calculation as AD_SHARES,
tot_mv  as T_MKT_V,
mv_calculation as AD_MKT_V,
'WIND'  AS SRC_CHANNEL,
3  as SRC_TABLE,
a.opdate  as SRC_OPDATE
from  eterminal.AindexSSE50Weight  a join  eterminal.asharedescription b
on  a.s_con_windcode=b.s_info_windcode
 where to_char(a.opdate,'YYYY-MM-DD') >='%s'
"""%(dodate,dodate,dodate,dodate)

cols="""
sRC_OBJECT_ID,idx_cd,ticker,SRC_CODE,TRADE_DATE,WEIGHT,AD_OPEN_REF_P,T_SHARES,TRAD_RATIO,W_FACTOR,CLOSE_PRICE,AD_SHARES,T_MKT_V,AD_MKT_V,SRC_CHANNEL,SRC_TABLE,SRC_OPDATE
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

etl.update_unique_id()
wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
