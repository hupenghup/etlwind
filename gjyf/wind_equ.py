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

table_name = 'wind_equ'
dpc.exec_trun(table_name)

sql="""
insert into wind_equ(id,security_id,ticker,exchange_cd,exchange_cd_cn,sec_full_name,sec_short_name,cn_spell,sec_short_name_en,trans_curr_cd,list_date,delist_date,list_board
        ,equ_type,equ_style,isin_code,is_shsc,credit_code,is_high_tech,ownership_cd,ownership,zjh_cd,zjh_name,unique_id,create_time,update_time)
select
    distinct `a`.`id` as `id`,
    `a`.`security_id_b` as `security_id`,
    `a`.`ticker` as `ticker`,
    `a`.`exchange_cd` as `exchange_cd`,
    (case
        when (`a`.`exchange_cd` = 'XSHG') then '上交所'
        when (`a`.`exchange_cd` = 'XSHE') then '深交所'
    end) as `exchange_cd_cn`,
    `a`.`sec_full_name` as `sec_full_name`,
    `a`.`sec_short_name` as `sec_short_name`,
    `a`.`cn_spell` as `cn_spell`,
    `a`.`sec_short_name_en` as `sec_short_name_en`,
    `a`.`trans_curr_cd` as `trans_curr_cd`,
    `a`.`list_date` as `list_date`,
    `a`.`delist_date` as `delist_date`,
    `b`.`list_board` as `list_board`,
    `b`.`equ_type` as `equ_type`,
    '普通股' as `equ_style`,
    `b`.`isin_code` as `isin_code`,
    `b`.`is_shsc` as `is_shsc`,
    `a`.`credit_code` as `credit_code`,
    (case
        when isnull(`c`.`credit_code`) then 0
        else 1
    end) as `is_high_tech`,
    `d`.`sec_code` as `ownership_cd`,
    (case
        when (`d`.`sec_code` like '080501%') then '国有企业'
        when (`d`.`sec_code` like '080504%') then '集体企业'
        when (`d`.`sec_code` like '080502%') then '私营企业'
        else '其他'
    end) as `ownership`,
    `f`.`industries_alias` as `zjh_cd`,
    `e`.`level2_name` as `zjh_name`,
    `a`.`unique_id` as `unique_id`,
    `a`.`create_time` as `create_time`,
    `a`.`update_time` as `update_time`
from
    (((((`gjyf`.`md_security` `a`
join `gjyf`.`equ_type` `b` on
    (((`a`.`src_code` = `b`.`src_code`)
    and (`a`.`asset_class` = 'equ'))))
left join `gjyf`.`equ_high_tech` `c` on
    (((`a`.`credit_code` = `c`.`credit_code`)
    and (`c`.`end_date` >= curdate()))))
left join `gjyf`.`equ_ownership` `d` on
    (((`a`.`ticker` = `d`.`ticker`)
    and (`d`.`is_new` = 1))))
left join `gjyf`.`equ_industry` `e` on
    (((`a`.`ticker` = `e`.`ticker`)
    and (`e`.`industry_name_cd` = 'ZJHHY2012')
    and (`e`.`is_new` = 1))))
left join `gjyf`.`sys_industries_code` `f` on
    (((`e`.`level2_cd` = `f`.`industries_code`)
    and (`f`.`industries_code` like '12%'))))
where
    (substr(`a`.`ticker`,
    1,
    1) in ('0',
    '3',
    '6'))
union all select
    distinct `z`.`id` as `id`,
    `z`.`security_id_b` as `security_id`,
    `z`.`ticker` as `ticker`,
    `z`.`exchange_cd` as `exchange_cd`,
    (case
        when (`a`.`exchange_cd` = 'XSHG') then '上交所'
        when (`a`.`exchange_cd` = 'XSHE') then '深交所'
    end) as `exchange_cd_cn`,
    `z`.`comp_full_name` as `sec_full_name`,
    `z`.`sec_short_name` as `sec_short_name`,
    null as `cn_spell`,
    `z`.`sec_short_name_en` as `sec_short_name_en`,
    `a`.`trans_curr_cd` as `trans_curr_cd`,
    `z`.`listed_date` as `list_date`,
    null as `delist_date`,
    null as `list_board`,
    null as `equ_type`,
    '优先股' as `equ_style`,
    null as `isin_code`,
    null as `is_shsc`,
    `a`.`credit_code` as `credit_code`,
    (case
        when isnull(`c`.`credit_code`) then 0
        else 1
    end) as `is_high_tech`,
    `d`.`sec_code` as `ownership_cd`,
    (case
        when (`d`.`sec_code` like '080501%') then '国有企业'
        when (`d`.`sec_code` like '080504%') then '集体企业'
        when (`d`.`sec_code` like '080502%') then '私营企业'
        else '其他'
    end) as `ownership`,
    `f`.`industries_alias` as `zjh_cd`,
    `e`.`level2_name` as `zjh_name`,
    `z`.`unique_id` as `unique_id`,
    `z`.`create_time` as `create_time`,
    `z`.`update_time` as `update_time`
from
    ((((((`gjyf`.`equ_prefer` `z`
join `gjyf`.`md_security` `a` on
    (((`z`.`a_ticker` = `a`.`ticker`)
    and (`a`.`asset_class` = 'equ'))))
join `gjyf`.`equ_type` `b` on
    ((`a`.`src_code` = `b`.`src_code`)))
left join `gjyf`.`equ_high_tech` `c` on
    (((`a`.`credit_code` = `c`.`credit_code`)
    and (`c`.`end_date` >= curdate()))))
left join `gjyf`.`equ_ownership` `d` on
    (((`a`.`ticker` = `d`.`ticker`)
    and (`d`.`is_new` = 1))))
left join `gjyf`.`equ_industry` `e` on
    (((`a`.`ticker` = `e`.`ticker`)
    and (`e`.`industry_name_cd` = 'ZJHHY2012')
    and (`e`.`is_new` = 1))))
left join `gjyf`.`sys_industries_code` `f` on
    (((`e`.`level2_cd` = `f`.`industries_code`)
    and (`f`.`industries_code` like '12%'))))
where
    (substr(`a`.`ticker`,
    1,
    1) in ('0',
    '3',
    '6'))
"""

dpc.exec_sql(sql)

ai_cur.close()

ai.close()