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


table_name = 'sys_industries_code'
file_name = 'sys_industries_code'

#此表每日全量更新

sql_trun="""truncate table  gjyf.%s  """%(table_name )
print(sql_trun)
ai_cur.execute(sql_trun)
ai_cur.execute('commit')


cols = """industries_code, industries_name, level_num, is_used, industries_alias, industries_def_cn,
industries_name_en, sequence, memo, src_object_id, src_opdate
"""

unique_key = "id"

sql="""
select
INDUSTRIESCODE as industries_code,
INDUSTRIESNAME as industries_name,
LEVELNUM as level_num,
USED as is_used,
INDUSTRIESALIAS as industries_alias,
CHINESEDEFINITION as industries_def_cn,
WIND_NAME_ENG as industries_name_en,
SEQUENCE as sequence,
MEMO as memo,
OBJECT_ID as src_object_id,
OPDATE as src_opdate
from eterminal.Ashareindustriescode 
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