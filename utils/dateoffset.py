# -*- coding: utf-8 -*-

import datetime
import calendar


def MonthOffset(date, n):
    """
    :param date:
    :param n:
    :return: 获取当前日期前/后n个月的日期
    """
    if isinstance(date, (datetime.date, datetime.datetime)):
        date = date
    y = int(date.year)
    m = int(date.month)
    m = m + n

    if 0 < m < 12:
        y = y
        d = str(calendar.monthrange(y, m)[1])
        m = m
    else:
        i = m / 12
        j = m % 12
        if j == 0:
            i -= 1
            j = 12
        y += int(i)
        d = str(calendar.monthrange(y, j)[1])
        m = j

    if int(date.strftime('%d')) < int(d):
        d = date.strftime('%d')

    return datetime.datetime.strptime('%s%s%s' % (y, '0%s' % m if m < 10 else m, d), '%Y%m%d').date()


