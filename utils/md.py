#! usr/local/bin/python
# -*- coding: utf-8 -*-

import hashlib


def md5(data):
    """
    :param data: string
    :return:
    """
    hs = hashlib.md5()
    hs.update(data)

    return hs.hexdigest()

