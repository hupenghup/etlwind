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



table_name = 'deposit_interest'
file_name = 'deposit_interest'

#此表每日全量更新

sql_trun="""truncate table  gjyf.%s  """%(table_name )
print(sql_trun)
ai_cur.execute(sql_trun)
ai_cur.execute('commit')

sql="""
select to_date(TRADE_DAYS,'yyyy-mm-dd')  as  cal_date,
       B_INFO_RATE as year_interest,
       src_opdate
from 
(
select b.TRADE_DAYS,
       a.B_INFO_RATE,
       a.opdate  as src_opdate,

       row_number() over (partition by substr(b.TRADE_DAYS,1,6) order by TRADE_DAYS desc) rn

from eterminal.CBondBenchmark a,

     eterminal.CBondCalendar b    

where a.B_INFO_BENCHMARK='01010203'

  and b.S_INFO_EXCHMARKET='NIB'

  and b.TRADE_DAYS>='20100101'

  and a.TRADE_DT=

        (select max(c.TRADE_DT) from eterminal.CBondBenchmark c

         where a.B_INFO_BENCHMARK=c.B_INFO_BENCHMARK

           and c.TRADE_DT<=b.TRADE_DAYS)

)
where rn=1 
"""

cols="""cal_date,year_interest ,src_opdate """

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
