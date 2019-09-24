#! /usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import warnings
from datetime import datetime


sys.path.append("../")
from utils.conf import  wd,wd_cur,ai,  ai_cur
from utils.etl import ETL

warnings.filterwarnings('ignore')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'



table_name = 'equ_type'
file_name = 'equ_type'

#此表每日全量更新

sql_trun="""truncate table  gjyf.%s  """%(table_name )
print(sql_trun)
ai_cur.execute(sql_trun)
ai_cur.execute('commit')

unique_key = "src_object_id"

cols = """src_code,ticker,isin_code ,list_board,is_shsc, equ_type,src_channel,src_table,src_opdate,src_object_id 
"""

sql="""
select  s_info_windcode  as src_code ,
s_info_code as ticker,
s_info_isincode as isin_code ,
s_info_listboardname as list_board,
is_shsc as is_shsc,
'A'as equ_type,
'wind' as src_channel,
1 as src_table,
opdate as src_opdate，
object_id as src_object_id 
from eterminal.AShareDescription
"""


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
