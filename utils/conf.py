# -*- coding: utf-8 -*-

import os
import sys
import imp
imp.reload(sys)
import pymysql 
import cx_Oracle
from sqlalchemy import create_engine

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

env = 'dev'
cfg = {
    'dev': {
        # wind
        'odc': {
            'host': '192.168.15.149',
            'port': 1521,
            'user': 'QRY_ODS_GJYF',
            'password': 'NewDevODS612'
        },

        # load_data
        'gjyf': {
            'host': '172.28.210.104',
            'port': 3306,
            'db': 'gjyf',
            'user': 'gjyf_dd',
            'password': 'gjyfDD6etl'
        }
    },
    'prd': {
        'odc': {
            'host': '192.168.52.120',
            'port': 1521,
            'user': 'QRY_ODS_ZCPZPT',
            'password': 'Zptsq6B9ygJ!'
        },

        'gjyf': {
            'host': '192.168.52.140',
            'port': 3306,
            'db': 'gjyf',
            'user': 'gjyf_dd',
            'password': 'gjyfDD6etl'
        }
    }
}
env = env.lower()
conn = cfg[env]

# 目标数据库:load_data
host = conn['gjyf']['host']
port = conn['gjyf']['port']
if isinstance(port, str):
    port = int(port)
db = conn['gjyf']['db']
user = conn['gjyf']['user']
password = conn['gjyf']['password']
ai = pymysql.connect(host=host, port=port, db=db, user=user, passwd=password, charset='utf8',local_infile=1)
ai_cur = ai.cursor()
ai_en = create_engine('mysql+pymysql://%s:%s@%s:%s/%s' % (user, password, host, port, db), echo=False)
# 数据源数据库:winddb2
wd_host = conn['odc']['host']
wd_port = conn['odc']['port']
if isinstance(wd_port, str):
    wd_port = int(wd_port)
wd_user = conn['odc']['user']
wd_password = conn['odc']['password']
wd = cx_Oracle.connect('%s/%s@%s:%d/%s' % (wd_user, wd_password, wd_host, wd_port, 'odstest11g'))
wd_cur = wd.cursor()


