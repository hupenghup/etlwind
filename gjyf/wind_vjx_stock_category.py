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


dpc = dataProc(
          tgt_cur=ai_cur,
          tgt_conn=ai
          )

table_name = 'wind_vjx_stock_category'
dpc.exec_trun(table_name)

sql="""
insert into wind_vjx_stock_category(id,security_id,ticker,exchange_cd,into_date,out_date,industry_name_cd,industry_name,industry_version,level1_cd,level1_name,level2_cd,level2_name
                ,level3_cd,level3_name,level4_cd,level4_name,is_new,unique_id,create_time,update_time)
                
select
    distinct `a`.`id` as `id`,
    `a`.`security_id_b` as `security_id`,
    `a`.`ticker` as `ticker`,
    `a`.`exchange_cd` as `exchange_cd`,
    `a`.`into_date` as `into_date`,
    `a`.`out_date` as `out_date`,
    a.`industry_name_cd`,
    `a`.`industry_name` as `industry_name`,
    `a`.`industry_version` as `industry_version`,
    `b`.`level1_cd` as `level1_cd`,
    (case
        when (`b`.`level1_name` = '日常消费') then '日常消费品'
        when (`b`.`level1_name` = '可选消费') then '非日常生活消费品'
        when (`b`.`level1_name` = '材料') then '原材料'
        when (`b`.`level1_name` = '电信服务') then '电信业务'
        when isnull(`b`.`level1_name`) then `a`.`level1_name`
        else `b`.`level1_name`
    end) as `level1_name`,
    `a`.`level2_cd` as `level2_cd`,
    `a`.`level2_name` as `level2_name`,
    `a`.`level3_cd` as `level3_cd`,
    `a`.`level3_name` as `level3_name`,
    `a`.`level4_cd` as `level4_cd`,
    `a`.`level4_name` as `level4_name`,
    `a`.`is_new` as `is_new`,
    `a`.`unique_id` as `unique_id`,
    `a`.`create_time` as `create_time`,
    `a`.`update_time` as `update_time`
from
    (`gjyf`.`hk_equ_industry` `a`
left join gjyf.wind_equ_industry_level1 `b` on
    ((`a`.`level1_name` = `b`.`level1_name`)))
where
    (isnull(`a`.`out_date`)
    and (`a`.`industry_name_cd` = 'GICS') )
union

select
    `gjyf`.`equ_industry`.`id` as `id`,
    `gjyf`.`equ_industry`.`security_id_b` as `security_id`,
    `gjyf`.`equ_industry`.`ticker` as `ticker`,
    `gjyf`.`equ_industry`.`exchange_cd` as `exchange_cd`,
    `gjyf`.`equ_industry`.`into_date` as `into_date`,
    `gjyf`.`equ_industry`.`out_date` as `out_date`,
    `gjyf`.`equ_industry`.`industry_name_cd` as `industry_name_cd`,
    `gjyf`.`equ_industry`.`industry_name` as `industry_name`,
    `gjyf`.`equ_industry`.`industry_version` as `industry_version`,
    `gjyf`.`equ_industry`.`level1_cd` as `level1_cd`,
    (case
        when (`gjyf`.`equ_industry`.`level1_name` = '日常消费') then '日常消费品'
        when (`gjyf`.`equ_industry`.`level1_name` = '可选消费') then '非日常生活消费品'
        when (`gjyf`.`equ_industry`.`level1_name` = '材料') then '原材料'
        when (`gjyf`.`equ_industry`.`level1_name` = '电信服务') then '电信业务'
        else `gjyf`.`equ_industry`.`level1_name`
    end) as `level1_name`,
    `gjyf`.`equ_industry`.`level2_cd` as `level2_cd`,
    `gjyf`.`equ_industry`.`level2_name` as `level2_name`,
    `gjyf`.`equ_industry`.`level3_cd` as `level3_cd`,
    `gjyf`.`equ_industry`.`level3_name` as `level3_name`,
    `gjyf`.`equ_industry`.`level4_cd` as `level4_cd`,
    `gjyf`.`equ_industry`.`level4_name` as `level4_name`,
    `gjyf`.`equ_industry`.`is_new` as `is_new`,
    `gjyf`.`equ_industry`.`unique_id` as `unique_id`,
    `gjyf`.`equ_industry`.`create_time` as `create_time`,
    `gjyf`.`equ_industry`.`update_time` as `update_time`
from
    `gjyf`.`equ_industry`
where
    (isnull(`gjyf`.`equ_industry`.`out_date`)
    and (`gjyf`.`equ_industry`.`industry_name_cd` <> '6'))
union 

select
    `gjyf`.`equ_industry`.`id` as `id`,
    `gjyf`.`equ_industry`.`security_id_b` as `security_id`,
    `gjyf`.`equ_industry`.`ticker` as `ticker`,
    `gjyf`.`equ_industry`.`exchange_cd` as `exchange_cd`,
    `gjyf`.`equ_industry`.`into_date` as `into_date`,
    `gjyf`.`equ_industry`.`out_date` as `out_date`,
    'GICS' as `industry_name_cd`,
    '全球行业分类标准' as `industry_name`,
    `gjyf`.`equ_industry`.`industry_version` as `industry_version`,
    `gjyf`.`equ_industry`.`level1_cd` as `level1_cd`,
    (case
        when (`gjyf`.`equ_industry`.`level1_name` = '日常消费') then '日常消费品'
        when (`gjyf`.`equ_industry`.`level1_name` = '可选消费') then '非日常生活消费品'
        when (`gjyf`.`equ_industry`.`level1_name` = '材料') then '原材料'
        when (`gjyf`.`equ_industry`.`level1_name` = '电信服务') then '电信业务'
        else `gjyf`.`equ_industry`.`level1_name`
    end) as `level1_name`,
    `gjyf`.`equ_industry`.`level2_cd` as `level2_cd`,
    `gjyf`.`equ_industry`.`level2_name` as `level2_name`,
    `gjyf`.`equ_industry`.`level3_cd` as `level3_cd`,
    `gjyf`.`equ_industry`.`level3_name` as `level3_name`,
    `gjyf`.`equ_industry`.`level4_cd` as `level4_cd`,
    `gjyf`.`equ_industry`.`level4_name` as `level4_name`,
    `gjyf`.`equ_industry`.`is_new` as `is_new`,
    md5(concat(`gjyf`.`equ_industry`.`unique_id`, 'GICS')) as `unique_id`,
    `gjyf`.`equ_industry`.`create_time` as `create_time`,
    `gjyf`.`equ_industry`.`update_time` as `update_time`
from
    `gjyf`.`equ_industry`
where
    (isnull(`gjyf`.`equ_industry`.`out_date`)
    and (`gjyf`.`equ_industry`.`industry_name_cd` = 'wd'))
 
"""

dpc.exec_sql(sql)

ai_cur.close()

ai.close()