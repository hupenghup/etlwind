#! /usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import datetime

sys.path.append("../")
from utils.conf import wd, wd_cur, ai, ai_cur
from utils.etl import ETL

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


table_name = 'md_security'
file_name = 'md_security'
columns = """
    src_code,ticker,ASSET_CLASS,SEC_SHORT_NAME,SEC_FULL_NAME,SEC_FULL_NAME_EN,CN_SPELL,LIST_DATE,DELIST_DATE,TRANS_CURR_CD,src_channel,src_table,src_object_id,src_opdate
    """
unique_key = "security_id"

#此表每日全量更新

sql_trun="""truncate table  gjyf.%s  """%(table_name )
print(sql_trun)
ai_cur.execute(sql_trun)
ai_cur.execute('commit')


sql = """ 
	select  s_info_windcode as src_code,s_info_code as ticker,'EQU' as ASSET_CLASS,s_info_name as SEC_SHORT_NAME,s_info_compname  as SEC_FULL_NAME
	,s_info_compnameeng  as SEC_FULL_NAME_EN,  s_info_pinyin  as CN_SPELL,TO_DATE(s_info_listdate,'YYYY-MM-DD') as LIST_DATE 
	,TO_DATE(s_info_delistdate,'YYYY-MM-DD')  as DELIST_DATE ,crncy_code as   TRANS_CURR_CD  ,
	'wind' as  src_channel, 1 AS src_table, object_id as src_object_id ,opdate as src_opdate from eterminal.AShareDescription 
      """
print(sql)
etl = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql,
          table_name=table_name,
          columns=columns,
          unique_key=unique_key)

# 加载数据

etl.dump_data(file_name)

# 写入数据
etl.import_data(file_name)
# 更新unique_id字段值
#etl.update_unique_id()

wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
