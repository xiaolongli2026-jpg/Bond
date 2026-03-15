
"""
快速链接数据库，数据库地址在 ``doc.conf``
"""

import pymysql
import cx_Oracle
from conf.conf import default_db, db_names
from doc.global_var import global_var as glv


def connect_mysql(dt_source=default_db):
    """
    如果没有规定用的数据库，优先连接估值库

    Args:
        dt_source (str): db_bond-估值库 oracle，db_abs-外网数据库 mysql

    Returns: connection

    """
    assert dt_source in db_names.keys()

    conn_fail = False

    is_mysql = False
    info = db_names[dt_source]['info']
    sql_type = db_names[dt_source]['type']
    try:
        if sql_type == "oracle":
            dns = cx_Oracle.makedsn(info["host"], info["port"], sid=info["sid"])
            conn = cx_Oracle.connect(info["user"], info['passwd'], dns)
            is_mysql = False
        elif sql_type == 'mysql':
            conn = pymysql.connect(
                host=info['host'],
                port=info['port'],
                user=info['user'],
                passwd=info['passwd'],
                db=info['db']
            )
            is_mysql = True
    except:
        conn_fail = True

    if conn_fail:
        raise ConnectionError(f"数据库连接错误，地址：{info['host']}")
    else:
        glv().set("is_mysql", is_mysql)

    return conn, is_mysql
