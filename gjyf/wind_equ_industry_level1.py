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

table_name = 'wind_equ_industry_level1'
dpc.exec_trun(table_name)

sql="""
insert into wind_equ_industry_level1(level1_name,level1_cd)
select
    distinct `gjyf`.`equ_industry`.`level1_name` AS `level1_name`,
    `gjyf`.`equ_industry`.`level1_cd` AS `level1_cd`
from
    `gjyf`.`equ_industry`
where
    ((`gjyf`.`equ_industry`.`industry_name_cd` = 'wd')
    and isnull(`gjyf`.`equ_industry`.`out_date`))
"""

dpc.exec_sql(sql)

ai_cur.close()

ai.close()