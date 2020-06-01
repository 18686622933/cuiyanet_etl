#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import pymysql
from package import config_file


def exception_handling(act, *exception, handling="Error"):
    """

    :param act:
    :param exception:
    :param handling:
    :return:
    """

    if exception:
        try:
            act
        except exception as e:
            print("Error:", e, sep=" ")
    else:
        print(handling)


def db_connect_test(connfig):
    try:
        conn = pymysql.connect(**connfig)
        conn.close()
    except pymysql.err.OperationalError as e:
        print("Error:", e, sep=" ")


