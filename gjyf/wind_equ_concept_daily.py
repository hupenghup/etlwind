#! /usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import warnings
import datetime


sys.path.append("../")
from utils.conf import  ai,  ai_cur
from utils.data_proc import dataProc

warnings.filterwarnings('ignore')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

if len(sys.argv) == 1:
    dodate = (datetime.datetime.now()  - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
else:
    dodate = sys.argv[1]

dpc = dataProc(
          tgt_cur=ai_cur,
          tgt_conn=ai
          )

table_name = 'wind_equ_concept_daily'
dpc.exec_delete(table_name,"create_time",dodate)

sql="""
insert into wind_equ_concept_daily(id,trade_date,security_id,ticker,sec_short_name,is_new_stock,is_stop_stock,is_resump,is_up_1,is_down_1,mkt_value_dis,pe_dis,pb_dis,board_dis,exchange_cd
                    ,comp_lv_1,comp_region,unique_id,is_shsz,is_szsc,create_time,update_time)
select
    `a`.`id` AS `id`,
    `a`.`trade_date` AS `trade_date`,
    `a`.`security_id_b` AS `security_id`,
    `a`.`ticker` AS `ticker`,
    `a`.`sec_short_name` AS `sec_short_name`,
    `a`.`is_new_stock` AS `is_new_stock`,
    `a`.`is_stop_stock` AS `is_stop_stock`,
    `a`.`is_resump` AS `is_resump`,
    `a`.`is_up_1` AS `is_up_1`,
    `a`.`is_down_1` AS `is_down_1`,
    `a`.`mkt_value_dis` AS `mkt_value_dis`,
    `a`.`pe_dis` AS `pe_dis`,
    `a`.`pb_dis` AS `pb_dis`,
    `a`.`board_dis` AS `board_dis`,
    `a`.`exchange_cd` AS `exchange_cd`,
    `a`.`comp_lv_1` AS `comp_lv_1`,
    `a`.`comp_region` AS `comp_region`,
    `a`.`unique_id` AS `unique_id`,
    (case
        when (`b`.`is_shsc` = 1) then 1
        else 0
    end) AS `is_shsz`,
    (case
        when (`b`.`is_shsc` = 2) then 1
        else 0
    end) AS `is_szsc`,
    `a`.`create_time` AS `create_time`,
    `a`.`update_time` AS `update_time`
from
    (`gjyf`.`equ_concept_daily` `a`
join `gjyf`.`equ_type` `b` on
    ((`a`.`ticker` = `b`.`ticker`)))
where a.create_time >= '%s'
"""% dodate

dpc.exec_sql(sql)

ai_cur.close()

ai.close()