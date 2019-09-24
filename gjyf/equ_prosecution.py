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

table_name = 'equ_prosecution'
file_name = 'equ_prosecution'

# dodate = '2010-01-01'

sql = """
SELECT
replace(regexp_substr(a.s_info_windcode,'.*?\.'),'.') as  ticker,   
to_date(ann_dt ,'yyyy-mm-dd')  AS  anno_date , 
    title  AS title ,
   accuser   AS accuser ,
   defendant AS defendant,
   pro_type  AS pro_type,
  amount  AS amount ,
 a.CRNCY_CODE   AS  curreny_cd ,
PROSECUTE_DT    AS  prosecute_dt ,
 COURT AS  court  ,
 JUDGE_DT AS  judge_dt ,
DBMS_LOB.SUBSTR(result,4000,1)  as result,
 IS_APPEAL AS   is_appeal ,
 APPELLANT AS  appellant ,
 COURT2 AS  court2 ,
JUDGE_DT2  AS judge_dt2 ,
DBMS_LOB.SUBSTR(result2,4000,1)  as result2,
 RESULTAMOUNT AS  result_amount,
 DBMS_LOB.SUBSTR(BRIEFRESULT,4000,1)AS brief_result ,
  SUBSTR(EXECUTION,4000,1) AS  execution ,
 SUBSTR(INTRODUCTION,4000,1)||DBMS_LOB.SUBSTR(INTRODUCTION,4000,4001)||DBMS_LOB.SUBSTR(INTRODUCTION,4000,8001) as introduction,

'wind' as    src_channel,
1 as    src_table,
a.object_id as    src_object_id,
a.s_info_windcode  as         src_code,
a.opdate  as    src_opdate 
from   eterminal.AShareProsecution  a  
where  
 to_char(a.opdate,'YYYY-MM-DD') >='%s'and a.opmode=0
""" % (dodate)

cols = """
ticker,anno_date ,title ,accuser ,defendant,pro_type,amount ,curreny_cd ,prosecute_dt ,court  ,judge_dt ,result ,is_appeal 
,appellant ,court2 ,judge_dt2 ,result2 ,result_amount,brief_result ,execution ,introduction , src_channel,src_table,src_object_id,src_code,src_opdate
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
