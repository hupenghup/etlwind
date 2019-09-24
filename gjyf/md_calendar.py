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



table_name = 'md_calendar'
file_name = 'md_calendar'

#此表每日全量更新

sql_trun="""truncate table  gjyf.%s  """%(table_name )
print(sql_trun)
ai_cur.execute(sql_trun)
ai_cur.execute('commit')

unique_key = "src_object_id"

cols = """trade_date,exchange_cd,src_opdate,src_channel,src_table ,src_object_id
"""

sql="""
select  to_date(trade_days,'yyyy-mm-dd')  as trade_date ,s_info_exchmarket as exchange_cd,opdate as src_opdate,'wind' as src_channel,1 as src_table ,object_id  as src_object_id 
from   eterminal.AShareCalendar
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