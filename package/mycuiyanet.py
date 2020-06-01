#!/usr/bin/env python3
# -*- coding:utf-8 -*-


import pymysql


def get_data_to_mysql(table_name, *max):
    """
    先按来源库创建table，然后运行本函数，将指定table中的数据取到本地库
    要求：来源库与本地库 table结构完全一致
    :param table_name: 要获取数据的table名称
    :param max: 要获取的数据条数，如不填则为获取该表中全部数据
    :return:
    """



    config = {'host': '192.168.110.85',
              'port': 3306,
              'user': 'chenbowen',
              'password': '123456',
              'database': 'predata',
              'charset': 'utf8'}

    myconfig = {'host': '127.0.0.1',
                'port': 3306,
                'user': 'web',
                'password': '123',
                'database': 'cuiyanet',
                'charset': 'utf8'}

    conn = pymysql.connect(**config)
    myconn = pymysql.connect(**myconfig)

    cursor = conn.cursor()
    mycursor = myconn.cursor()

    table = table_name
    sql_column_name = '''
            select COLUMN_NAME from information_schema.COLUMNS where table_name = %s and table_schema = 'predata' 
                    '''
    cursor.execute(sql_column_name, table)
    column_names = cursor.fetchall()
    print(column_names)
    print(len(column_names))

    sql_count = '''
                select count(*) from %s
    ''' % table
    if max:
        count = max[0]
    else:
        cursor.execute(sql_count)
        count = cursor.fetchall()[0][0]
        print(count)

    get_row_data = '''
        select * from %s
    ''' % table
    cursor.execute(get_row_data)

    while count >= 1:
        one_row_data = cursor.fetchone()
        column_s = '(%s' + ',%s' * (len(column_names) - 1) + ')'
        sql_insert = '''insert into {table}() values'''.format(table=table) + column_s
        mycursor.execute(sql_insert, one_row_data)
        print(one_row_data)
        count -= 1

    myconn.commit()

    cursor.close()
    mycursor.close()

    conn.close()
    myconn.close()



