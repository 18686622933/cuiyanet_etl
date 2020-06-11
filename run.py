#!/usr/bin/env python3
# -*- coding:utf-8 -*-


from package import raw_data_import
from package import config_file
from package import sub_function
from package import algo_data_export
import pymysql

while True:
    key = int(input("1.原始数据入库\n2.算法数据输出\n3.算法结果返回\n4.退出\n\n输入序号："))
    try:
        conn = pymysql.connect(**config_file.config)
        conn.close()

        if key == 1:
            # sub_function.db_connect_test(config_file.config)
            raw_data_import.data_import(config_file.config)
        elif key == 2:
            algo_data_export.aglo_data_export(config_file.config)
        elif key == 3:
            print(3)
        elif key == 4:
            break
        else:
            print("xxx 错误输入 xxx")

    except pymysql.err.OperationalError as e:
        print("Error:", e, sep=" ")
        break
