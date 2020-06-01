#!/usr/bin/env python3
# -*- coding:utf-8 -*-


# !/usr/bin/env python3
# -*- coding:utf-8 -*-

from package import mycuiyanet
from package import base_data
from package import execel_data
from package import config_file
import openpyxl
import pymysql
import re


def data_import(config):
    print("\n" + "-" * 20 + " 原始数据入库 开始执行 " + "-" * 20 + "\n")

    gruop_names = execel_data.parser_dir()
    sch_data = base_data.get_sch_data(config)
    major_data = base_data.get_major_data(config)
    province_data = base_data.get_province_data(config)
    batch_data = base_data.get_batch_data(config)
    type = {'综合': 407, '理科': 298, '文科': 299}

    category_data = base_data.get_category_data(config)
    small_class_data = base_data.get_small_class(config)
    # print(gruop_names)
    # print(sch_data)
    # print(major_data)
    # print(province_data)
    # print(batch_data)
    # print(type)
    #
    # print(category_data)
    # print(small_class_data)
    # print("-" * 100)

    print("-" * 20 + " 一分一段表 " + "-" * 20)
    for name in gruop_names['一分一段表']:
        execel_data.to_distribution(name, config, province_data)

    print("-" * 20 + " 录取分数 " + "-" * 20)
    for name in gruop_names['录取分数']:
        execel_data.to_fractional(name, config, sch_data, major_data, batch_data,
                                  subject={'综合': 407, '理科': 298, '文科': 299})

    print("-" * 20 + " 报考书 " + "-" * 20)
    for name in gruop_names['报考书']:
        execel_data.to_guidefra(name, config, province_data, sch_data, major_data, batch_data,
                                subject={'综合': 407, '理科': 298, '文科': 299})

    print("-" * 20 + " 拆分表 " + "-" * 20)
    for name in gruop_names['拆分表']:
        execel_data.to_frac(name, config, sch_data, major_data, batch_data, category_data,
                            small_class_data, subject={'综合': 407, '理科': 298, '文科': 299})

    print("-" * 20 + " 数据对标 " + "-" * 20)
    for name in gruop_names['数据对标']:
        execel_data.to_guidefra_arts_science(name, config, province_data, sch_data, major_data, batch_data,
                                             subject={'综合': 407, '理科': 298, '文科': 299})

    print("\n" + "-" * 20 + " 原始数据入库 执行完成 " + "-" * 20 + "\n")
