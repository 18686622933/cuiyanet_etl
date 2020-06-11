#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from package import config_file
from package import db_data
import time


# 获取数据对标表列表，并根据表中省份及最新年份创建算法文件夹，完整路径保存在pro_dirs中

def aglo_data_export(config):
    star = time.process_time()
    print("\n" + "-" * 20 + " 算法数据输出 开始执行 " + "-" * 20 + "\n")
    print("算法数据正在输出，请等待。。。")
    tables_dict = db_data.get_table(config)
    pro_dirs = tables_dict

    for k in tables_dict.keys():
        province = k
        pro = province
        year = tables_dict[province][0][-8:-4]
        algo_dir = db_data.create_file(pro, year)
        pro_dirs[province] = [pro_dirs[province], algo_dir]
        for table in tables_dict[province][0]:
            db_data.get_db_data(config_file.config, table, algo_dir, province)
        print("%s算法数据输入已完成，目录为%s " % (province, pro_dirs[province][1]))

    print("\n" + "-" * 20 + " 算法数据输出 执行完成 " + "-" * 20 + "\n")
    end = time.process_time()
    # print(end - star)
