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



table_name = 'equ_industry'
file_name = 'equ_industry'

#此表每日全量更新

sql_trun="""truncate table  gjyf.%s  """%(table_name )
print(sql_trun)
ai_cur.execute(sql_trun)
ai_cur.execute('commit')

unique_key = "src_object_id"

cols="""
src_code,ticker,src_object_id,src_channel,src_table,src_opdate,into_date,out_date,is_new,industry_name_cd,industry_name,level1_cd,level2_cd,level3_cd,level4_cd,level1_name,level2_name,level3_name,level4_name
"""

#zjhx
sql_zjhx="""
select distinct a.* ,f.industriesname as level4_name from 
(select a.* ,e.industriesname  as level3_name from 
(select a.* ,d.industriesname  as level2_name
from (select a.* ,c.industriesname as level1_name

from (select s_info_windcode  as src_code  ,replace(regexp_substr(s_info_windcode ,'.*?\.'),'.')  as  ticker, a.object_id as src_object_id,
'WIND' as src_channel,3 as src_table,a.opdate
as src_opdate,to_date(entry_dt,'yyyy-mm-dd') as into_date,to_date(remove_dt,'yyyy-mm-dd')as out_date ,cur_sign as is_new,'ZJHX' AS industry_name_cd,
'证监会行业分类'  AS industry_name,
substr(industriescode,1,4)||'000000' as level1_cd,

substr(industriescode,1,6)||'0000' as level2_cd,

substr(industriescode,1,8)||'00' as level3_cd,

industriescode as level4_cd
from eterminal.AShareIndustriesCode a  join eterminal.AShareSECIndustriesClass b
on  a.industriescode=b.sec_ind_code)a LEFT  join eterminal.AShareIndustriesCode c

on a.level1_cd=c.industriescode   )a LEFT join   eterminal.AShareIndustriesCode d
on a.level2_cd=d.industriescode)a LEFT join eterminal.AShareIndustriesCode e
on a.level3_cd=e.industriescode) a LEFT join eterminal.AShareIndustriesCode f
on a.level4_cd=f.industriescode
"""

print(sql_zjhx)
etl_zjhx = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql_zjhx,
          table_name=table_name,
          columns=cols,
          unique_key=unique_key)

# 加载数据
etl_zjhx.dump_data(file_name)

# 写入数据
etl_zjhx.import_data(file_name)

#zxhyfl
sql_zxhyfl="""
select distinct a.* ,f.industriesname as level4_name from 
(select a.* ,e.industriesname  as level3_name from 
(select a.* ,d.industriesname  as level2_name
from (select a.* ,c.industriesname as level1_name

from (select s_info_windcode  as src_code ,replace(regexp_substr(s_info_windcode ,'.*?\.'),'.')  as  ticker ,a.object_id as src_object_id,'WIND' as src_channel,2 as src_table,a.opdate
as src_opdate,to_date(entry_dt,'yyyy-mm-dd') as into_date,to_date(remove_dt,'yyyy-mm-dd')as out_date ,cur_sign as is_new,'ZXHYFL' AS industry_name_cd,
'中信行业分类'  AS industry_name,
substr(industriescode,1,4)||'000000' as level1_cd,

substr(industriescode,1,6)||'0000' as level2_cd,

substr(industriescode,1,8)||'00' as level3_cd,

industriescode as level4_cd
from eterminal.AShareIndustriesCode a  join eterminal.AShareIndustriesClassCITICS b
on  a.industriescode=b.citics_ind_code)a  LEFT join eterminal.AShareIndustriesCode c

on a.level1_cd=c.industriescode   )a LEFT join   eterminal.AShareIndustriesCode d
on a.level2_cd=d.industriescode)a LEFT join eterminal.AShareIndustriesCode e
on a.level3_cd=e.industriescode) a LEFT join eterminal.AShareIndustriesCode f
on a.level4_cd=f.industriescode 
"""


print(sql_zxhyfl)
etl_zxhyfl = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql_zxhyfl,
          table_name=table_name,
          columns=cols,
          unique_key=unique_key)

# 加载数据
etl_zxhyfl.dump_data(file_name)

# 写入数据
etl_zxhyfl.import_data(file_name)

#zjhhy2012
sql_zjhhy2012="""
select distinct a.* ,f.industriesname as level4_name from 
(select a.* ,e.industriesname  as level3_name from 
(select a.* ,d.industriesname  as level2_name
from (select a.* ,c.industriesname as level1_name

from (select s_info_windcode  as src_code ,replace(regexp_substr(s_info_windcode ,'.*?\.'),'.')  as  ticker ,a.object_id as src_object_id,'WIND' as src_channel,4 as src_table,a.opdate
as src_opdate,to_date(entry_dt,'yyyy-mm-dd') as into_date,to_date(remove_dt,'yyyy-mm-dd') as out_date,cur_sign as is_new,'ZJHHY2012' AS industry_name_cd,
'证监会行业分类(2012版)'  AS industry_name,
substr(industriescode,1,4)||'000000' as level1_cd,

substr(industriescode,1,6)||'0000' as level2_cd,

substr(industriescode,1,8)||'00' as level3_cd,

industriescode as level4_cd
from eterminal.AShareIndustriesCode a  join eterminal.AShareSECNIndustriesClass b
on  a.industriescode=b.sec_ind_code)a  LEFT join eterminal.AShareIndustriesCode c

on a.level1_cd=c.industriescode   )a LEFT join   eterminal.AShareIndustriesCode d
on a.level2_cd=d.industriescode)a LEFT join eterminal.AShareIndustriesCode e
on a.level3_cd=e.industriescode) a LEFT join eterminal.AShareIndustriesCode f
on a.level4_cd=f.industriescode
"""


print(sql_zjhhy2012)
etl2012 = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql_zjhhy2012,
          table_name=table_name,
          columns=cols,
          unique_key=unique_key)

# 加载数据
etl2012.dump_data(file_name)

# 写入数据
etl2012.import_data(file_name)

#申万
sql_sw="""
select  distinct a.* ,f.industriesname as level4_name from 
(select a.* ,e.industriesname  as level3_name from 
(select a.* ,d.industriesname  as level2_name
from (select a.* ,c.industriesname as level1_name
from (select s_info_windcode  as src_code ,replace(regexp_substr(s_info_windcode ,'.*?\.'),'.')  as  ticker ,a.object_id as src_object_id,'WIND' as src_channel,5 as src_table,a.opdate
as src_opdate,to_char(to_date(entry_dt,'yyyy-mm-dd'), 'yyyy-mm-dd') as into_date,to_date(remove_dt,'yyyy-mm-dd') as out_date,cur_sign as is_new,'SW' AS industry_name_cd,
'申万行业分类'  AS industry_name,
substr(industriescode,1,4)||'000000' as level1_cd,
substr(industriescode,1,6)||'0000' as level2_cd,
substr(industriescode,1,8)||'00' as level3_cd,
industriescode as level4_cd
from eterminal.AShareIndustriesCode a  join eterminal.AShareSWIndustriesClass b
on  a.industriescode=b.sw_ind_code)a  LEFT join eterminal.AShareIndustriesCode c
on a.level1_cd=c.industriescode   )a LEFT join   eterminal.AShareIndustriesCode d
on a.level2_cd=d.industriescode)a LEFT join eterminal.AShareIndustriesCode e
on a.level3_cd=e.industriescode) a LEFT  join eterminal.AShareIndustriesCode f
on a.level4_cd=f.industriescode
"""


print(sql_sw)
etl = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql_sw,
          table_name=table_name,
          columns=cols,
          unique_key=unique_key)

# 加载数据
etl.dump_data(file_name)

# 写入数据
etl.import_data(file_name)

#wind
sql_wind="""
select distinct a.* ,f.industriesname as level4_name from 
(select a.* ,e.industriesname  as level3_name from 
(select a.* ,d.industriesname  as level2_name
from (select a.* ,c.industriesname as level1_name

from (select s_info_windcode  as src_code ,replace(regexp_substr(s_info_windcode ,'.*?\.'),'.')  as ticker,a.object_id as src_object_id,'WIND' as src_channel,1 as src_table,a.opdate
as src_opdate,to_date(entry_dt,'yyyy-mm-dd') as into_date,to_date(remove_dt,'yyyy-mm-dd') as out_date,cur_sign as is_new,'WD' AS industry_name_cd,
'Wind行业分类'  AS industry_name,
substr(industriescode,1,4)||'000000' as level1_cd,

substr(industriescode,1,6)||'0000' as level2_cd,

substr(industriescode,1,8)||'00' as level3_cd,

industriescode as level4_cd
from eterminal.AShareIndustriesCode a  join eterminal.AShareIndustriesClass b
on  a.industriescode=b.wind_ind_code)a LEFT  join eterminal.AShareIndustriesCode c

on a.level1_cd=c.industriescode   )a LEFT join   eterminal.AShareIndustriesCode d
on a.level2_cd=d.industriescode)a LEFT join eterminal.AShareIndustriesCode e
on a.level3_cd=e.industriescode) a LEFT join eterminal.AShareIndustriesCode f
on a.level4_cd=f.industriescode  
"""


print(sql_wind)
etl2 = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql_wind,
          table_name=table_name,
          columns=cols,
          unique_key=unique_key)

# 加载数据
etl2.dump_data(file_name)

# 写入数据
etl2.import_data(file_name)

#etl.update_unique_id()
wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
