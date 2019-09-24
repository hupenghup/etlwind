#! /usr/local/bin/python
#-*- coding:utf-8 -*-

import platform
import datetime

from . import md


class ETL(object):
    def __init__(self,
                 src_cur=None,
                 src_conn=None,
                 tgt_cur=None,
                 tgt_conn=None,
                 sql=None,
                 table_name=None,
                 columns=None,
                 unique_key=None):
        # data source database cursor
        self.src_cur = src_cur
        # data target database connect
        self.src_conn = src_conn
        # data target database cursor
        self.tgt_cur = tgt_cur
        # data target database connect
        self.tgt_conn = tgt_conn
        # data source extract sql, dtype:string
        self.sql = sql
        # target table, dtype:string
        self.table_name = table_name
        # target table's columns, dtype:string
        self.columns = columns
        # target table's unique index, dtype:string
        self.unique_key = unique_key

    def dump_data(self, file_name, remove=1):
        """
        :param file_name: 存储数据文件名
        :param remove: 是否去除sql执行结果最后1列; 1是,0否
        :return:
        """
        with open("%s.txt" % file_name, "w", encoding="utf-8") as f:
            total = 0
            self.src_cur.execute(self.sql)
            for row in self.src_cur.fetchall():
                total += 1
                values = ''
                for i in range(len(row)):
                    if i == 0:
                        values = "'%s'" % (row[i])
                    # 去掉最后1列
                    elif remove == 1 and i == len(row) :
                        values = values
                    else:
                        if row[i] is None:
                            values = "%s|%s" % (values,"\\N")
                        else:
                            values = "%s|'%s'" % (values, row[i])
                f.write("%s\n" % values)
                if total % 1000 == 0:
                    print(total)
            print(total)
            print("%s: %s" % ("dump_data done", datetime.datetime.now()))

    def dump_data2(self, file_name, remove=1, **kwargs):
        """
        :param file_name: 存储数据文件名
        :param remove: 是否去除sql执行结果最后1列; 1是,0否
        :param kwargs: id映射参数，参数值类型:dict; e.g. map=True, security_id={}
        :return:
        """
        if self.columns is None:
            self.columns = ""
        # 查找是否有需要映射的security_id字段
        try:
            if 'map' in kwargs.keys() and kwargs['map'] is True:
                _id = self.columns.replace(" ", "").split(',').index('security_id')
            else:
                _id = -1
        except ValueError:
            _id = -1
        # 加载数据到txt
        with open("%s.txt" % file_name, "w") as f:
            total = 0
            self.src_cur.execute(self.sql)
            for row in self.src_cur.fetchall():
                total += 1
                values = ''
                for i in range(len(row)):
                    try:
                        if i == _id == 0:
                            values = "'%s'" % (kwargs['security_id'][row[i]])
                        elif i == 0:
                            values = "'%s'" % (row[i])
                        # 去掉最后1列
                        elif remove == 1 and i == len(row) - 1:
                            values = values
                        else:
                            if row[i] is None:
                                values = "%s|%s" % (values, "\n")
                            else:
                                if i == _id:
                                    values = "%s|'%s'" % (values, kwargs['security_id'][row[i]])
                                else:
                                    values = "%s|'%s'" % (values, row[i])
                    except KeyError:
                        # 没有对应映射的security_id字段过滤该行
                        values = ""
                        break
                f.write(values if values == "" else "%s\n" % values)
                if total % 1000 == 0:
                    print(total)
            print(total)
            print("%s: %s" % ("dump_data done", datetime.datetime.now()))

    def delete_data(self, tgt_filter=None):
        """
        :param tgt_filter: String, 提取目标表数据sql之过滤条件
        :return:
        """
        if tgt_filter is None:
            tgt_filter = ""
        # 全量比较
        # 获取数据源unique_id之sql
        src_sql = "select %s from (%s)tmp" % (self.unique_key, self.sql)
        print(src_sql)
        self.src_cur.execute(src_sql)
        rows = self.src_cur.fetchall()
        # 获取目标表unique_id之sql
        tgt_sql = "select unique_id from %s %s" % (self.table_name, tgt_filter)
        self.tgt_cur.execute(tgt_sql)
        tgt_rows = self.tgt_cur.fetchall()
        if len(rows) > 0 and len(tgt_rows) > 0:
            # 数据源unique_id
            unique_id_src = []
            for row in rows:
                unique_value = ""
                for _r in row:
                    if isinstance(_r, datetime.datetime):
                        _r = datetime.datetime.strftime(_r, '%Y-%m-%d')
                    unique_value += "%s" % _r
                unique_id_src.append(md.md5(unique_value))
            print(len(unique_id_src))
            # 目标表unique_id
            unique_id_tgt = [row[0] for row in tgt_rows]
            print(len(unique_id_tgt))
            # 比较数据源与目标表,存在于目标表不存在与数据源,判断为删除unique_id
            delete_unique_id = set(unique_id_tgt).difference(set(unique_id_src))
            print(len(delete_unique_id))
            # 处理待删除数据
            if len(delete_unique_id) > 0:
                delete_unique_id = tuple([_id.encode("utf-8") for _id in delete_unique_id])
                del_sql = "delete from %s where unique_id in %s" % (self.table_name, tuple(delete_unique_id))
                # 执行数据删除
                # print(del_sql
                self.tgt_cur.execute(del_sql)
                self.tgt_cur.execute("commit")
                print("%s, delete done" % datetime.datetime.now())
            else:
                print("%s, no data to delete!" % datetime.datetime.now())
        else:
            print("%s, not deleted, data source or target has no data" % datetime.datetime.now())

    def import_data(self, file_name):
        """
        :param file_name: dump_data方法存储数据的文件名,注:terminated使用'|'分隔
        :return:
        """
        # 插入/更新数据
        # 使用load data local infile…replace into…方法
        if platform.system() == 'Windows':
            sql = """load data local infile '%s.txt' replace into table %s 
                fields terminated by '|' enclosed by "'" lines terminated by '\r\n'(%s)""" % (
                file_name, self.table_name, self.columns)
        elif platform.system() == 'Mac':
            sql = """load data local infile '%s.txt' replace into table %s 
                fields terminated by '|' enclosed by "'" lines terminated by '\r'(%s) """ % (
                file_name, self.table_name, self.columns)
        else:
            sql = """load data local infile '%s.txt' replace into table %s 
                fields terminated by '|' enclosed by "'" lines terminated by '\n'(%s) """ % (
                file_name, self.table_name, self.columns)

        print(sql)
        self.tgt_cur.execute(sql)
        self.tgt_cur.execute('commit')
        print("%s: %s" % ("import_data done", datetime.datetime.now()))

    def update_unique_id(self):
        """
        :return: 更新目标表unique_id字段为空的记录
        """
        sql = "UPDATE %s SET unique_id = MD5(CONCAT(%s)) WHERE unique_id IS NULL" % (self.table_name, self.unique_key)
        self.tgt_cur.execute(sql)
        self.tgt_conn.commit()
        print("%s: %s" % ("update_unique_id done", datetime.datetime.now()))

