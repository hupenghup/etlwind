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

table_name = 'equ_reginv'
file_name = 'equ_reginv'

# dodate = '2010-01-01'

sql = """
SELECT
replace(regexp_substr(a.s_info_windcode,'.*?\.'),'.') as  ticker,   
COMP_ID,SEC_ID,S_INFO_WINDCODE,SUR_INSTITUTE,SUR_REASONS,STR_ANNDATE,END_ANNDATE,STR_DATE,
a.object_id as    src_object_id,
a.opdate  as    src_opdate 
from   eterminal.ASharereginv  a  
where   to_char(a.opdate,'YYYY-MM-DD') >='%s'
""" % (dodate)

cols = """
ticker,COMP_ID,SEC_ID,S_INFO_WINDCODE,SUR_INSTITUTE,SUR_REASONS,STR_ANNDATE,END_ANNDATE,STR_DATE,src_object_id,src_opdate
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
