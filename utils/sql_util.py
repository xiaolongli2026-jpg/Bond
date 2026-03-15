# -*- coding: utf-8 -*-

import pandas as pd


def sql_read(sql_str, cur, is_mysql=False, single_return=False) -> pd.DataFrame:
    """
    根据sql语句读取数据

    Args:
        sql_str (str): sql 语句
        cur (cursor):
        is_mysql(bool): sql语句为mysql，如果是False则是oracle，需要将 `sql_str` 中不符合oracle语法的进行替代
        single_return: True-返回单个值，False-返pd.DataFrame

    """

    if not is_mysql:
        trans_ = {'ifnull': 'nvl', 'IFNULL': 'nvl'}
        for key_ in trans_:
            sql_str = sql_str.replace(key_, trans_[key_])

    if single_return:
        cur.execute(sql_str)
        value_ = cur.fetchone()
        return_ = value_[0]

    else:
        cur.execute(sql_str)
        value_ = cur.fetchall()
        columns_ = cur.description
        return_ = pd.DataFrame(value_, columns=[x[0].lower() for x in columns_])

    return return_