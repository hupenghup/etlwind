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


sql50="""
insert into jx_csi50_industry_perent(code, class_type, trade_dt, name, industries_code, percent)
SELECT  distinct 
  `a`.`idx_cd`              AS `code`,
  `a`.`industry_name_cd`    AS `class_type`,
  `a`.`trade_date`          AS `trade_dt`,
  `b`.`level1_name`         AS `name`,
  `b`.`src_industries_code` AS `industries_code`,
  (`b`.`level_weight` / `a`.`sum_weight`) AS `percent`

FROM ( SELECT `a`.`trade_date`       AS `trade_date`,
  `a`.`idx_cd`           AS `idx_cd`,
  `b`.`industry_name_cd` AS `industry_name_cd`,
  SUM(`a`.`weight`)      AS `sum_weight`

FROM (`gjyf`.`idx_weight` `a`
   JOIN `gjyf`.`equ_industry` `b`
     ON (((`a`.`ticker` = `b`.`ticker`)
          AND (`a`.`exchange_cd` = `b`.`exchange_cd`)
          AND (`a`.`src_table` = 3)
          AND (`b`.`is_new` = 1)  and  a.trade_date>= '%s')))
GROUP BY `a`.`trade_date`,`a`.`idx_cd`,`b`.`industry_name_cd`)a
   JOIN (SELECT
  `a`.`trade_date`       AS `trade_date`,
  `a`.`idx_cd`           AS `idx_cd`,
  `b`.`industry_name_cd` AS `industry_name_cd`,
  `b`.`level1_name`      AS `level1_name`,
  `b`.`level1_cd`        AS `src_industries_code`,
  SUM(`a`.`weight`)      AS `level_weight`
FROM (`gjyf`.`idx_weight` `a`
   JOIN `gjyf`.`equ_industry` `b`
     ON (((`a`.`ticker` = `b`.`ticker`)
          AND (`a`.`exchange_cd` = `b`.`exchange_cd`)
          AND (`b`.`is_new` = 1)
          AND (`a`.`src_table` = 3) )))
GROUP BY `a`.`trade_date`,`b`.`industry_name_cd`,`b`.`level1_cd`)b
     ON `a`.`idx_cd` = `b`.`idx_cd`
          AND `a`.`trade_date` = `b`.`trade_date`
          AND a.industry_name_cd=b.industry_name_cd
""" % (dodate)

dpc.exec_delete("jx_csi50_industry_perent","trade_dt",dodate)
dpc.exec_sql(sql50)


sql300="""
insert into jx_csi300_industry_perent(code, class_type, trade_dt, name, industries_code, percent)
SELECT distinct
 
  `a`.`idx_cd`              AS `code`,
  `a`.`industry_name_cd`    AS `class_type`,
  `a`.`trade_date`          AS `trade_dt`,
  `b`.`level1_name`         AS `name`,
  `b`.`src_industries_code` AS `industries_code`,
  (`b`.`level_weight` / `a`.`sum_weight`) AS `percent`

FROM ( SELECT `a`.`trade_date`       AS `trade_date`,
  `a`.`idx_cd`           AS `idx_cd`,
  `b`.`industry_name_cd` AS `industry_name_cd`,
  SUM(`a`.`weight`)      AS `sum_weight`

FROM (`gjyf`.`idx_weight` `a`
   JOIN `gjyf`.`equ_industry` `b`
     ON (((`a`.`ticker` = `b`.`ticker`)
          AND (`a`.`exchange_cd` = `b`.`exchange_cd`)
          AND (`a`.`src_table` = 1)
          AND (`b`.`is_new` = 1)  and  a.trade_date>= '%s')))
GROUP BY `a`.`trade_date`,`a`.`idx_cd`,`b`.`industry_name_cd`)a
   JOIN (SELECT
  `a`.`trade_date`       AS `trade_date`,
  `a`.`idx_cd`           AS `idx_cd`,
  `b`.`industry_name_cd` AS `industry_name_cd`,
  `b`.`level1_name`      AS `level1_name`,
  `b`.`level1_cd`        AS `src_industries_code`,
  SUM(`a`.`weight`)      AS `level_weight`
FROM (`gjyf`.`idx_weight` `a`
   JOIN `gjyf`.`equ_industry` `b`
     ON (((`a`.`ticker` = `b`.`ticker`)
          AND (`a`.`exchange_cd` = `b`.`exchange_cd`)
          AND (`b`.`is_new` = 1)
          AND (`a`.`src_table` = 1) )))
GROUP BY `a`.`trade_date`,`b`.`industry_name_cd`,`b`.`level1_cd`)b
     ON `a`.`idx_cd` = `b`.`idx_cd`
          AND `a`.`trade_date` = `b`.`trade_date`
          AND a.industry_name_cd=b.industry_name_cd
""" % (dodate)

dpc.exec_delete("jx_csi300_industry_perent","trade_dt",dodate)
dpc.exec_sql(sql300)


sql500="""
insert into jx_csi500_industry_perent(code, class_type, trade_dt, name, industries_code, percent)
SELECT  distinct
  `a`.`idx_cd`              AS `code`,
  `a`.`industry_name_cd`    AS `class_type`,
  `a`.`trade_date`          AS `trade_dt`,
  `b`.`level1_name`         AS `name`,
  `b`.`src_industries_code` AS `industries_code`,
  (`b`.`level_weight` / `a`.`sum_weight`) AS `percent`

FROM ( SELECT `a`.`trade_date`       AS `trade_date`,
  `a`.`idx_cd`           AS `idx_cd`,
  `b`.`industry_name_cd` AS `industry_name_cd`,
  SUM(`a`.`weight`)      AS `sum_weight`

FROM (`gjyf`.`idx_weight` `a`
   JOIN `gjyf`.`equ_industry` `b`
     ON (((`a`.`ticker` = `b`.`ticker`)
          AND (`a`.`exchange_cd` = `b`.`exchange_cd`)
          AND (`a`.`src_table` = 4)
          AND (`b`.`is_new` = 1)  and  a.trade_date>= '%s')))
GROUP BY `a`.`trade_date`,`a`.`idx_cd`,`b`.`industry_name_cd`)a
   JOIN (SELECT
  `a`.`trade_date`       AS `trade_date`,
  `a`.`idx_cd`           AS `idx_cd`,
  `b`.`industry_name_cd` AS `industry_name_cd`,
  `b`.`level1_name`      AS `level1_name`,
  `b`.`level1_cd`        AS `src_industries_code`,
  SUM(`a`.`weight`)      AS `level_weight`
FROM (`gjyf`.`idx_weight` `a`
   JOIN `gjyf`.`equ_industry` `b`
     ON (((`a`.`ticker` = `b`.`ticker`)
          AND (`a`.`exchange_cd` = `b`.`exchange_cd`)
          AND (`b`.`is_new` = 1)
          AND (`a`.`src_table` = 4) )))
GROUP BY `a`.`trade_date`,`b`.`industry_name_cd`,`b`.`level1_cd`)b
     ON `a`.`idx_cd` = `b`.`idx_cd`
          AND `a`.`trade_date` = `b`.`trade_date`
          AND a.industry_name_cd=b.industry_name_cd
""" % (dodate)

dpc.exec_delete("jx_csi500_industry_perent","trade_dt",dodate)
dpc.exec_sql(sql500)

sql800="""
insert into jx_csi800_industry_perent(code, class_type, trade_dt, name, industries_code, percent)
SELECT  distinct 
  `a`.`idx_cd`              AS `code`,
  `a`.`industry_name_cd`    AS `class_type`,
  `a`.`trade_date`          AS `trade_dt`,
  `b`.`level1_name`         AS `name`,
  `b`.`src_industries_code` AS `industries_code`,
  (`b`.`level_weight` / `a`.`sum_weight`) AS `percent`

FROM ( SELECT `a`.`trade_date`       AS `trade_date`,
  `a`.`idx_cd`           AS `idx_cd`,
  `b`.`industry_name_cd` AS `industry_name_cd`,
  SUM(`a`.`weight`)      AS `sum_weight`

FROM (`gjyf`.`idx_weight` `a`
   JOIN `gjyf`.`equ_industry` `b`
     ON (((`a`.`ticker` = `b`.`ticker`)
          AND (`a`.`exchange_cd` = `b`.`exchange_cd`)
          AND (`a`.`src_table` = 2)
          AND (`b`.`is_new` = 1)  and  a.trade_date>= '%s')))
GROUP BY `a`.`trade_date`,`a`.`idx_cd`,`b`.`industry_name_cd`)a
   JOIN (SELECT
  `a`.`trade_date`       AS `trade_date`,
  `a`.`idx_cd`           AS `idx_cd`,
  `b`.`industry_name_cd` AS `industry_name_cd`,
  `b`.`level1_name`      AS `level1_name`,
  `b`.`level1_cd`        AS `src_industries_code`,
  SUM(`a`.`weight`)      AS `level_weight`
FROM (`gjyf`.`idx_weight` `a`
   JOIN `gjyf`.`equ_industry` `b`
     ON (((`a`.`ticker` = `b`.`ticker`)
          AND (`a`.`exchange_cd` = `b`.`exchange_cd`)
          AND (`b`.`is_new` = 1)
          AND (`a`.`src_table` = 2) )))
GROUP BY `a`.`trade_date`,`b`.`industry_name_cd`,`b`.`level1_cd`)b
     ON `a`.`idx_cd` = `b`.`idx_cd`
          AND `a`.`trade_date` = `b`.`trade_date`
          AND a.industry_name_cd=b.industry_name_cd
""" % (dodate)

dpc.exec_delete("jx_csi800_industry_perent","trade_dt",dodate)
dpc.exec_sql(sql800)

ai_cur.close()

ai.close()