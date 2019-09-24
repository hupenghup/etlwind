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

table_name = 'wind_vjx_hk_consult_info'
dpc.exec_delete(table_name,"create_time",dodate)

sql="""
insert into wind_vjx_hk_consult_info(id,jy_code,code,trade_dt,volume_transaction,transaction_number,avg_price,pe_ratio,pb_ratio,market_value,closing_price,nofq_closing_price,adj_factor
                ,dividend_yield,total_capital,circulation_capital,min_price_chg,not_hk_total_capital,ashares,bshares,turnover_rate,f_pe,ps,pcf,unique_id,create_time,update_time)
select
    distinct `a`.`ID` as `id`,
    `a`.`jy_code` as `jy_code`,
    `a`.`code` as `code`,
    `a`.`trade_dt` as `trade_dt`,
    `a`.`volume_transaction` as `volume_transaction`,
    (((`a`.`transaction_number` * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0) as `transaction_number`,
    case when ifnull(`a`.`volume_transaction`,0) = 0 then null else 
		(((((`a`.`transaction_number` / `a`.`volume_transaction`) * 1.0) * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0)  end as `avg_price`,
    `a`.`pe_ratio` as `pe_ratio`,
    `a`.`pb_ratio` as `pb_ratio`,
    ((((`a`.`market_value` * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0) + ((((`a`.`nofq_closing_price` * ifnull(`a`.`not_hk_total_capital`,
    0)) * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0)) as `market_value`,
    (((`a`.`closing_price` * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0) as `closing_price`,
    (((`a`.`nofq_closing_price` * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0) as `nofq_closing_price`,
    `a`.`adj_factor` as `adj_factor`,
    `a`.`dividend_yield` as `dividend_yield`,
    `a`.`total_capital` as `total_capital`,
    `a`.`circulation_capital` as `circulation_capital`,
    `a`.`min_price_chg` as `min_price_chg`,
    `a`.`not_hk_total_capital` as `not_hk_total_capital`,
    `a`.`ashares` as `ashares`,
    `a`.`bshares` as `bshares`,
    `a`.`turnover_rate` as `turnover_rate`,
    `a`.`f_pe` as `f_pe`,
    `a`.`ps` as `ps`,
    `a`.`pcf` as `pcf`,
    `a`.`unique_id` as `unique_id`,
    `a`.`create_time` as `create_time`,
    `a`.`update_time` as `update_time`
from
    ((`gjyf`.`jx_hk_consult_info` `a`
join `gjyf`.`hk_equ` `b` on
    ((`a`.`jy_code` = `b`.`src_code`)))
join `gjyf`.`lc_shscforex` `c` on
    ((`a`.`trade_dt` = `c`.`trade_date`)))
where
    ((`c`.`refbid` is not null)
    and (`c`.`refask` is not null)
    and (`c`.`settlebid` is not null)
    and (`c`.`settleask` is not null))
and    a.create_time >= '%s'
union all select
    distinct `a`.`ID` as `id`,
    `a`.`jy_code` as `jy_code`,
    `a`.`code` as `code`,
    `a`.`trade_dt` as `trade_dt`,
    `a`.`volume_transaction` as `volume_transaction`,
    (((`a`.`transaction_number` * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0) as `transaction_number`,
    case when ifnull(`a`.`volume_transaction`,0) = 0 then null else 
    	(((((`a`.`transaction_number` / `a`.`volume_transaction`) * 1.0) * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0) end  as `avg_price`,
    `a`.`pe_ratio` as `pe_ratio`,
    `a`.`pb_ratio` as `pb_ratio`,
    ((((`a`.`market_value` * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0) + ((((`a`.`nofq_closing_price` * ifnull(`a`.`not_hk_total_capital`,
    0)) * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0)) as `market_value`,
    (((`a`.`closing_price` * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0) as `closing_price`,
    (((`a`.`nofq_closing_price` * (`c`.`settlebid` + `c`.`settleask`)) / 2) * 1.0) as `nofq_closing_price`,
    `a`.`adj_factor` as `adj_factor`,
    `a`.`dividend_yield` as `dividend_yield`,
    `a`.`total_capital` as `total_capital`,
    `a`.`circulation_capital` as `circulation_capital`,
    `a`.`min_price_chg` as `min_price_chg`,
    `a`.`not_hk_total_capital` as `not_hk_total_capital`,
    `a`.`ashares` as `ashares`,
    `a`.`bshares` as `bshares`,
    `a`.`turnover_rate` as `turnover_rate`,
    `a`.`f_pe` as `f_pe`,
    `a`.`ps` as `ps`,
    `a`.`pcf` as `pcf`,
    `a`.`unique_id` as `unique_id`,
    `a`.`create_time` as `create_time`,
    `a`.`update_time` as `update_time`
from
    ((`gjyf`.`jx_hk_consult_info_supply` `a`
join `gjyf`.`hk_equ` `b` on
    ((`a`.`jy_code` = `b`.`src_code`)))
join `gjyf`.`lc_shscforex_supply` `c` on
    ((`a`.`trade_dt` = `c`.`trade_dt`)))
where
    ((`c`.`refbid` is not null)
    and (`c`.`refask` is not null)
    and (`c`.`settlebid` is not null)
    and (`c`.`settleask` is not null))
and    a.create_time >= '%s'
"""% (dodate,dodate)

dpc.exec_sql(sql)

ai_cur.close()

ai.close()
