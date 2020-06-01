#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import pymysql


def get_sch_data(conn_info):
    """
    获取数据库中的学校信息（id,code,name,pro_id,pro）
    :param conn_info:  目标数据库信息
    :return: school_data
    {'安徽大学': {'id': 1, 'code': 'HS1', 'name': '安徽大学', 'pro_id': 12, 'pro': '安徽省'}, '北京大学': {'id': 2, 'code': 'HS2', 'name': '北京大学', 'pro_id': 1, 'pro': '北京'}, ...}
    """

    config = conn_info

    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    sql_school_count = '''
            select count(*) from hm_school
                    '''
    cursor.execute(sql_school_count)
    school_count = cursor.fetchall()[0][0]
    # print(school_count)

    sql_school_data = '''
            select id,code,name,pro_id,pro from hm_school 
                    '''
    cursor.execute(sql_school_data)
    school_data = {}

    while school_count >= 1:
        data = cursor.fetchone()
        one_school = {}
        one_school['id'] = data[0]
        one_school['code'] = data[1]
        one_school['name'] = data[2]
        one_school['pro_id'] = data[3]
        one_school['pro'] = data[4]
        school_data[data[2]] = one_school
        # print(one_school)
        school_count -= 1

    cursor.close()
    conn.close()

    # print(school_data)
    return school_data


def get_major_data(conn_info):
    """
    获取数据库中的学校信息（id,code,name,pro_id,pro）
    :param conn_info:  目标数据库信息
    :return: major_data

    """

    config = conn_info

    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    sql_major_count = '''
            select count(*) from hm_major
                    '''
    cursor.execute(sql_major_count)
    major_count = cursor.fetchall()[0][0]
    # print(major_count)

    sql_major_data = '''
            select id,code,name,small_class_id,small_class from hm_major 
                    '''
    cursor.execute(sql_major_data)
    major_data = {}

    while major_count >= 1:
        data = cursor.fetchone()
        one_major = {}
        one_major['id'] = data[0]
        one_major['code'] = data[1]
        one_major['name'] = data[2]
        one_major['small_class_id'] = data[3]
        one_major['small_class'] = data[4]
        major_data[data[2]] = one_major
        # print(one_major)
        major_count -= 1

    cursor.close()
    conn.close()

    # print(major_data)
    return major_data


def get_province_data(conn_info):
    """
    获取数据库中的省份信息（id,provincecode,province,short_name）
    :param conn_info:  目标数据库信息
    :return: province_data
    {'北京': {'id': 1, 'provincecode': '110000', 'province': '北京', 'short_name': '北京'}, '天津': {'id': 2, 'provincecode': '120000', 'province': '天津', 'short_name': '天津'}, ...}

    """

    config = conn_info

    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    sql_province_count = '''
            select count(*) from  sys_provinces
                    '''
    cursor.execute(sql_province_count)
    school_count = cursor.fetchall()[0][0]
    # print(school_count)

    sql_province_data = '''
            select id,provincecode,province,short_name from sys_provinces 
                    '''
    cursor.execute(sql_province_data)
    province_data = {}

    while school_count >= 1:
        data = cursor.fetchone()
        one_province = {}
        one_province['id'] = data[0]
        one_province['provincecode'] = data[1]
        one_province['province'] = data[2]
        one_province['short_name'] = data[3]
        province_data[data[3]] = one_province
        # print(one_school)
        school_count -= 1

    cursor.close()
    conn.close()

    # print(province_data)
    return province_data


def get_batch_data(conn_info):
    """
    获取数据库中的省份信息（id,provincecode,province,short_name）
    :param conn_info:  目标数据库信息
    :return: batch_data
    {'吉林省本科第一批A段': {'id': 28, 'code': '28', 'name': '本科第一批A段', 'procode': '220000', 'proname': '吉林省', 'pro_id': 7}, '吉林省本科第二批A段': {'id': 29, 'code': '29', 'name': '本科第二批A段', 'procode': '220000', 'proname': '吉林省', 'pro_id': 7}, '浙江省平行录取一段': {'id': 30, 'code': '30', 'name': '平行录取一段', 'procode': '330000', 'proname': '浙江省', 'pro_id': 11}, '浙江省平行录取二段': {'id': 31, 'code': '31', 'name': '平行录取二段', 'procode': '330000', 'proname': '浙江省', 'pro_id': 11}, '浙江省平行录取三段': {'id': 32, 'code': '32', 'name': '平行录取三段', 'procode': '330000', 'proname': '浙江省', 'pro_id': 11}, '吉林省本科第三批': {'id': 34, 'code': '34', 'name': '本科第三批', 'procode': '220000', 'proname': '吉林省', 'pro_id': 7}, '新疆维吾尔自治区本科第一批': {'id': 35, 'code': '35', 'name': '本科第一批', 'procode': '650000', 'proname': '新疆维吾尔自治区', 'pro_id': 31}, '新疆维吾尔自治区本科第二批': {'id': 36, 'code': '36', 'name': '本科第二批', 'procode': '650000', 'proname': '新疆维吾尔自治区', 'pro_id': 31}}
    """

    config = conn_info

    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    sql_batch_count = '''
            select count(*) from  sys_batch
                    '''
    cursor.execute(sql_batch_count)
    school_count = cursor.fetchall()[0][0]
    # print(school_count)

    sql_province_data = '''
            select * from sys_batch 
                    '''
    cursor.execute(sql_province_data)
    batch_data = {}

    while school_count >= 1:
        data = cursor.fetchone()
        one = {}
        one['id'] = data[0]
        one['code'] = data[1]
        one['name'] = data[2]
        one['procode'] = data[3]
        one['proname'] = data[4]
        one['pro_id'] = data[5]
        batch_data[data[4][:2] + data[2]] = one
        # print(one_school)
        school_count -= 1

    cursor.close()
    conn.close()

    # print(batch_data)
    return batch_data


def get_category_data(conn_info):

    config = conn_info
    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    sql_major_count = '''
            select count(*) from hm_major
                    '''
    cursor.execute(sql_major_count)
    major_count = cursor.fetchall()[0][0]
    # print(major_count)

    sql_major_data = '''
            SELECT category_id,category FROM `hm_major` GROUP by category_id,category
                    '''
    cursor.execute(sql_major_data)
    category_data = {}

    data = cursor.fetchall()
    for id, name in data:
        category_data[name] = id

    cursor.close()
    conn.close()

    # print(category_data)
    return category_data



def get_small_class(conn_info):

    config = conn_info
    conn = pymysql.connect(**config)
    cursor = conn.cursor()

    sql_major_count = '''
            select count(*) from hm_major
                    '''
    cursor.execute(sql_major_count)
    major_count = cursor.fetchall()[0][0]
    # print(major_count)

    sql_major_data = '''
            SELECT small_class_id,small_class FROM `hm_major` GROUP by small_class_id,small_class
                    '''
    cursor.execute(sql_major_data)
    small_class_data = {}

    data = cursor.fetchall()
    for id, name in data:
        small_class_data[name] = id

    cursor.close()
    conn.close()

    # print(small_class_data)
    return small_class_data