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

table_name = 'equ_illegality'
file_name = 'equ_illegality'

#dodate = '2010-01-01'

sql = """
SELECT
replace(regexp_substr(a.s_info_windcode,'.*?\.'),'.') as  ticker,   
S_INFO_WINDCODE,S_INFO_COMPCODE,ANN_DT,ILLEG_TYPE,SUBJECT_TYPE,SUBJECT,RELATION_TYPE,DBMS_LOB.SUBSTR(BEHAVIOR,2000,1)  as BEHAVIOR,DISPOSAL_DT,DISPOSAL_TYPE,METHOD as DISPOSAL_METHOD,PROCESSOR,AMOUNT,BAN_YEAR,REF_RULE,
a.object_id as    src_object_id,
a.opdate  as    src_opdate 
from   eterminal.AShareillegality  a  
where   to_char(a.opdate,'YYYY-MM-DD') >='%s'
""" % (dodate)

cols = """
ticker,S_INFO_WINDCODE,S_INFO_COMPCODE,ANN_DT,ILLEG_TYPE,SUBJECT_TYPE,SUBJECT,RELATION_TYPE,BEHAVIOR,DISPOSAL_DT,DISPOSAL_TYPE,DISPOSAL_METHOD,PROCESSOR,AMOUNT,BAN_YEAR,REF_RULE,src_object_id,src_opdate
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
