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

table_name = 'custom_mapping_code'
file_name = 'custom_mapping_code'

sql="""
SELECT  s_info_windcode  as src_code, s_info_asharecode  as  ashare_code,s_info_compcode  as comp_code,
s_info_Securitiestypes as securities_types ,s_info_sectypename as  security_type_name,s_info_Countryname as country_name,
s_info_Countrycode as country_code,s_info_exchmarketname  as  exmarket_name,exchmarket as  exmarket
,crncy_name  as crncy_name,crncy_code as crncy_code,s_info_isincode  as isin_code,s_info_code as ticker ,replace(s_info_name,'''','') as sec_short_name,security_status as security_status,
s_info_org_code  as  org_code,s_info_typecode as  type_code,
s_info_min_price_chg_unit as  min_price_chg_unit ,s_info_lot_size as  lot_size ,replace(s_info_ename,'''','') as ename,opdate as src_opdate,'wind' as src_channel,
'1' as src_table,object_id  as src_object_id
FROM  eterminal.WindCustomCode  where to_char(opdate,'YYYY-MM-DD') >= '%s'
""" % (dodate)

cols=""" src_code,  ashare_code,comp_code,
securities_types ,  security_type_name, country_name,
 country_code,  exmarket_name,  exmarket
, crncy_name, crncy_code, isin_code, ticker , sec_short_name, security_status,
  org_code,  type_code,
  min_price_chg_unit , lot_size , ename,src_opdate, src_channel,
src_table, src_object_id """

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
