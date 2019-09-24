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

table_name = 'idx_valuation'
file_name = 'idx_valuation'

dodate = '2010-01-01'

sql="""
SELECT '881001' AS idx_cd,a.trade_dt AS est_date,a.pb_lf  AS valuation
 FROM eterminal.AINDEXVALUATION a join ETERMINAL.ASHARECALENDAR c on a.trade_dt = c.TRADE_DAYS and c.S_INFO_EXCHMARKET = 'SSE'
 WHERE a.S_INFO_WINDCODE='881001.WI' AND a.TRADE_DT>= to_char(to_date('%s','yyyy-mm-dd'),'yyyymmdd')

UNION ALL

SELECT 'h11001' AS idx_cd,TRADE_DT AS est_date,B_ANAL_YIELD  AS valuation
 FROM eterminal.CBondCurveCNBD  a join ETERMINAL.ASHARECALENDAR c on a.trade_dt = c.TRADE_DAYS and c.S_INFO_EXCHMARKET = 'SSE'
 where B_ANAL_CURVENUMBER='1232' and B_ANAL_CURVETERM='10' and B_ANAL_CURVETYPE='2' and TRADE_DT>= to_char(to_date('%s','yyyy-mm-dd'),'yyyymmdd')
 
UNION ALL

select 'convBond' AS idx_cd, trade_dt AS est_date
,sum(CB_ANAL_STRBPREMIUMRATIO* B_INFO_OUTSTANDINGBALANCE)/sum(B_INFO_OUTSTANDINGBALANCE)                 AS valuation
from (
SELECT  a.s_info_windcode,a.trade_dt,b.s_info_changedate,b.b_info_outstandingbalance,a.CB_ANAL_STRBPREMIUMRATIO,a.cb_anal_convpremiumratio
 ,row_number() over(partition by a.s_info_windcode,a.trade_dt order by b.s_info_changedate desc) rn
from  eterminal.ccbondvaluation  a 
 join ETERMINAL.ASHARECALENDAR c on a.trade_dt = c.TRADE_DAYS and c.S_INFO_EXCHMARKET = 'SSE'
 left join     eterminal.ccbondamount b
    on a.s_info_windcode = b.s_info_windcode and a.trade_dt >= b.s_info_changedate
  where  a.TRADE_DT>= to_char(to_date('%s','yyyy-mm-dd'),'yyyymmdd')
  and a.TRADE_DT < to_char(sysdate,'yyyymmdd')
    ) 
    where rn = 1
    group by trade_dt  
    
UNION ALL

select 'convStock' AS idx_cd, trade_dt AS est_date
, sum(CB_ANAL_CONVPREMIUMRATIO* B_INFO_OUTSTANDINGBALANCE)/sum(B_INFO_OUTSTANDINGBALANCE) AS valuation
from (
SELECT  a.s_info_windcode,a.trade_dt,b.s_info_changedate,b.b_info_outstandingbalance,a.CB_ANAL_STRBPREMIUMRATIO,a.cb_anal_convpremiumratio
 ,row_number() over(partition by a.s_info_windcode,a.trade_dt order by b.s_info_changedate desc) rn
from  eterminal.ccbondvaluation  a 
 join ETERMINAL.ASHARECALENDAR c on a.trade_dt = c.TRADE_DAYS and c.S_INFO_EXCHMARKET = 'SSE'
 left join     eterminal.ccbondamount b
    on a.s_info_windcode = b.s_info_windcode and a.trade_dt >= b.s_info_changedate 
  where  a.TRADE_DT>= to_char(to_date('%s','yyyy-mm-dd'),'yyyymmdd')
  and a.TRADE_DT < to_char(sysdate,'yyyymmdd')
    ) 
    where rn = 1
    group by trade_dt  
""" % (dodate,dodate,dodate,dodate)

cols = """idx_cd, est_date,valuation
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
