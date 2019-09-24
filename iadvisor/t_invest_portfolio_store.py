#! /usr/local/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import warnings
import datetime


sys.path.append("../")
from utils.conf import wd, wd_cur, ai, ai_cur, ai_en
from utils.etl import ETL

warnings.filterwarnings('ignore')
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

if len(sys.argv) == 1:
    dotime = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
else:
    dotime = sys.argv[1]

table_name = 't_invest_portfolio_store'
file_name = 't_invest_portfolio_store'


# 获取数据映射sql
src_sql = "SELECT src_sql, data_source,columns_list,unique_key FROM iadvisor.etl_src_tgt_rule WHERE tgt_table = '%s' AND is_use = 1" % table_name
ai_cur.execute(src_sql)
rows = ai_cur.fetchall()
sql = "select * from (%s) where to_char(updatetime,'YYYY-MM-DD') >='%s'" % (rows[0][0], dotime)
columns = rows[0][2]
unique_key = rows[0][3]
print(sql)
table_name2 = 'iadvisor.t_invest_portfolio_store'
etl = ETL(src_cur=wd_cur,
          src_conn=wd,
          tgt_cur=ai_cur,
          tgt_conn=ai,
          sql=sql,
          table_name=table_name2,
          columns=columns,
          unique_key=unique_key)

# 加载数据
etl.dump_data(file_name)

# 写入数据
etl.import_data(file_name)


wd_cur.close()
ai_cur.close()
wd.close()
ai.close()
