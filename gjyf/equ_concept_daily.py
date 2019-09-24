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

if len(sys.argv) == 1:
    dodate = (datetime.datetime.now()  - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
else:
    dodate = sys.argv[1]

table_name = 'equ_concept_daily'
file_name = 'equ_concept_daily'

sql="""
select TO_DATE(e.trade_dt,'YYYY-MM-DD'  )  as trade_date, a.s_info_windcode as src_code,s_info_code as ticker,
  case when a.s_info_listdate>=  e.trade_dt  then 1
        else  0  end  as  is_new_stock, 
         case  when  s_dq_open=s_dq_high and s_dq_high=s_dq_low  and  s_dq_low=s_dq_close 
                     and  s_dq_open=s_dq_close  and s_dq_open=s_dq_close  and  s_dq_close>s_dq_preclose then 1
                     else 0  end as is_up_1,
case  when  s_dq_open=s_dq_high and s_dq_high=s_dq_low  and  s_dq_low=s_dq_close 
                     and  s_dq_open=s_dq_close  and s_dq_open=s_dq_close  and  s_dq_close<s_dq_preclose then 1
                     else 0  end as is_down_1,
        case when  s_dq_amount=0 then 1 else 0  end as is_stop_stock,
case when f.s_dq_resumpdate is not null then 1 else 0 end as is_resump,
 s_info_name as SEC_SHORT_NAME,
case when   b.wind_sec_code='0805010000'  then '国有企业'
     when   b.wind_sec_code='0805020000'    then '民营企业'
      when   b.wind_sec_code='0805030000'   then '外资企业'
         when   b.wind_sec_code='0805040000'  then '集体企业'
             when   b.wind_sec_code='0805050000'  then '公众企业'
               when   b.wind_sec_code='0805060000'  then '其他企业' end  as comp_lv_1,
             d.industriesname as comp_region  ,
'wind' as  src_channel, 1 AS src_table, a.object_id as src_object_id ,e.opdate as src_opdate 
from eterminal.AShareDescription  a left join  eterminal.AShareOwnership b  on 
 a.s_info_compname=b.s_info_compname  and   b.wind_sec_code in('0805010000','0805020000','0805030000'， 
 '0805040000','0805050000'，'0805060000')  and b.cur_sign=1 
 left join 
  eterminal.AShareRegional  c on  a.s_info_compname=c.s_info_compname   and c.cur_sign=1 
 left  join  eterminal.ashareindustriescode d
  on  c.wind_sec_code=d.industriescode
  left join eterminal.ASHAREEODPRICES e
on  a.s_info_windcode=e.s_info_windcode      
left join eterminal.AShareTradingSuspension f
on a.s_info_windcode = f.s_info_windcode and e.trade_dt = f.s_dq_resumpdate
where  to_char(e.opdate,'YYYY-MM-DD') >= '%s'
""" % (dodate)

cols=""" trade_date, src_code,ticker,is_new_stock,is_up_1,is_down_1,is_stop_stock,is_resump,SEC_SHORT_NAME,comp_lv_1,comp_region,src_channel,src_table,src_object_id, src_opdate"""

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