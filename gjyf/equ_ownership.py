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



table_name = 'equ_ownership'
file_name = 'equ_ownership'

#此表每日全量更新

sql_trun="""truncate table  gjyf.%s  """%(table_name )
print(sql_trun)
ai_cur.execute(sql_trun)
ai_cur.execute('commit')

unique_key = "src_object_id"

cols = """ticker,sec_code,sec_name,into_date,out_date,is_new,src_code,src_channel,src_object_id,src_opdate
"""

sql="""
select
replace(regexp_substr(b.s_info_windcode ,'.*?\.'),'.') as ticker,
a.wind_sec_code as sec_code,
c.industriesname as sec_name,
to_date(case when a.entry_dt is null then '19000101' else a.entry_dt end,'yyyy-mm-dd') as into_date,
to_date(a.remove_dt ,'yyyy-mm-dd') as out_date,
a.cur_sign as is_new,
b.s_info_windcode as src_code,
'Wind' as src_channel,
a.object_id as src_object_id,
a.opdate as src_opdate
from eterminal.AShareOwnership a
join eterminal.AShareDescription b
on a.s_info_compname = b.s_info_compname
and substr(s_info_code, 1, 1) in ('3', '6', '0')
left join (select industriescode, industriesname from eterminal.ashareindustriescode where industriescode like '0805%') c
on a.wind_sec_code = c.industriescode 
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