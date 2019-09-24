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

table_name = 'wind_vjx_consult_info'
dpc.exec_delete(table_name,"create_time",dodate)

sql="""
insert into wind_vjx_consult_info(id,wind_code,code,trade_date,date,open_price,high_price,low_price,close_price,avg_price,volume_transaction,transaction_number,turnover_rate,up_down_limit_status
                        ,pe_ratio,pb_ratio,market_value,closing_price,adj_factor,nofq_closing_price,dividend_yield,total_capital,circulation_capital,unique_id,create_time,update_time)
select
    a.`ID` as `id`,
    a.`wind_code` as `wind_code`,
    a.`code` as `code`,
    a.`trade_dt` as `trade_date`,
    a.`trade_dt` as `date`,
    `b`.`open_price` as `open_price`,
    `b`.`high_price` as `high_price`,
    `b`.`low_price` as `low_price`,
    `b`.`close_price` as `close_price`,
    `b`.`avg_price` as `avg_price`,
    a.`volume_transaction` as `volume_transaction`,
    a.`transaction_number` as `transaction_number`,
    `c`.`turnover_rate` as `turnover_rate`,
    `c`.`up_down_limit_status` as `up_down_limit_status`,
    a.`pe_ratio` as `pe_ratio`,
    a.`pb_ratio` as `pb_ratio`,
    a.`market_value` as `market_value`,
    a.`closing_price` as `closing_price`,
    a.`adj_factor` as `adj_factor`,
    a.`nofq_closing_price` as `nofq_closing_price`,
    (a.`dividend_yield` * 100) as `dividend_yield`,
    a.`total_capital` as `total_capital`,
    a.`circulation_capital` as `circulation_capital`,
    a.`unique_id` as `unique_id`,
    a.`create_time` as `create_time`,
    a.`update_time` as `update_time`
from
    ((`gjyf`.`jx_consult_info` a
join `gjyf`.`equ_daily_mkt` `b` on
    (((a.`wind_code` = `b`.`src_code`)
    and (a.`trade_dt` = `b`.`trade_date`))))
join `gjyf`.`equ_indicator_mkt` `c` on
    (((a.`wind_code` = `c`.`src_code`)
    and (a.`trade_dt` = `c`.`trade_date`))))
where    a.create_time >= '%s'
"""% dodate

dpc.exec_sql(sql)

ai_cur.close()

ai.close()