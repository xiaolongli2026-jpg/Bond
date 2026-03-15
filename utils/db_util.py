# -*- coding: utf-8 -*-
"""UPDATE RESULTS TO ORACLE / MYSQL / SQLITE (OR DELETE/SELECT DATA)

"""
import pandas as pd
from conf.conf import db_names, DATABASE_DIR, LOG_DIR
from utils.quick_connect import connect_mysql
import os
import sqlite3
from datetime import datetime
pd.set_option('precision', 4)


class UploadLib:


    def __init__(self, db_name, ban_creation=True):
        """this class is designed to cope with operations of three different types of database, separately mysql,
            oracle and sqlite3, in which the last one stores data in your computer.

        Args:
            db_name (str): name of database. A new file will be created if the file does not exist in 'DATABASE_DIR'
            if 'db_name' is not one of the cloud databases listed on conf.py. On the contrary,
            the structures of cloud databases are not allowed to be changed here in order to prevent introducing mistakes.
            ban_creation (bool): default True, creating a new table is interdicted.

        Notes:
            this class allows operations to rows, unable to update data on specific positions.

        """
        self.db_name = db_name
        self.ban_creation = ban_creation

        today_ = datetime.now().strftime("%Y%m%d")
        self.filename = os.path.join(LOG_DIR, 'log_db_' + today_ + '.txt')


        # data types of the table information below follow the rules of sqlite3, which should be transformed into other strings that obey the regulations of mysql / oracle when you are going to use these two kinds of database.


    def connect(self):
        """connect to the database named 'db_name'

        """

        if self.db_name in db_names:
            self.conn, _ = connect_mysql(self.db_name)
            self.cursor = self.conn.cursor()
            self.db_type = db_names[self.db_name]['type']
        else:
            self.db_type = 'sqlite'
            self.conn = sqlite3.connect(os.path.join(DATABASE_DIR, self.db_name.split(".")[0] + ".db"))
            self.cursor = self.conn.cursor()


    def insert(self, table_name, data):
        """insert data into a table

        Args:
            table_name:
            data (pd.DataFrame):

        Returns:

        """
        if not self.table_exist(table_name):
            self.create_table(table_name=table_name)

        if self.table_exist(table_name):

            insert_ = self.__columns_check(table_name, col_names=list(data.columns))
            if insert_:
                data = self.digital_match(table_name, data)
                cols_n = data.shape[1]
                col_str = ",".join(list(data.columns))

                data_lst = data.apply(lambda x: tuple(x), axis=1).to_list()
                split_n = len(data_lst) // 100000
                if self.db_type == 'sqlite':

                    site_str = ",".join(["?"] * data.shape[1])
                    sql_cmd = "REPLACE INTO %s  ( %s )  VALUES ( %s );" % (table_name, col_str, site_str)

                    try:
                        for i in range(0, split_n + 1):
                            self.cursor.executemany(sql_cmd, data_lst[i * 100000: min((i + 1) * 100000, len(data_lst))])
                            self.conn.commit()
                            msg = "inserted successfully into %s " % table_name
                            print(msg)

                    except sqlite3.Error as e:

                        self.conn.rollback()
                        msg = "fail to insert data into %s ,:" % table_name
                        self.write_log(msg)
                        print(msg, e)

                elif self.db_type == 'oracle' or self.db_type == 'mysql':

                    # site_str = ",".join([" :" + str(int(x + 1)) for x in range(cols_n)])
                    site_str = ",".join([" :" + x for x in data.columns])
                    sql_cmd = "INSERT INTO %s ( %s ) VALUES ( %s ) " % (table_name, col_str, site_str)

                    for i in range(0, split_n + 1):

                        try:
                            self.cursor.executemany(sql_cmd, data_lst[i * 100000: min((i + 1) * 100000, len(data_lst))])
                            self.conn.commit()
                            print("inserted successfully into %s " % table_name)
                        except ValueError as e:
                            self.conn.rollback()
                            msg = "fail to insert data into %s" % table_name
                            self.write_log(msg)
                            print(msg, e)

                else:

                    raise ValueError("unsupported database type %s " % self.db_type)

            else:
                msg = "dataset's columns do not perfectly match the column names in the %s , please check column names." % table_name
                print(msg)
                self.write_log(msg)
        else:
            msg = "table %s not exist" %table_name
            print(msg)
            self.write_log(msg)

    def delete(self, table_name, cond):
        """

        Args:
            table_name:
            cond (str, None): delete rows that conform to the 'cond' that follows the rules of sql.

        """
        if cond is None:
            sql_cmd = "DELETE FROM %s" %table_name
        else:
            sql_cmd = "DELETE FROM %s WHERE %s" % (table_name, cond)

        try:
            self.cursor.execute(sql_cmd)
            self.conn.commit()

        except:
            self.conn.rollback()
            msg = "can not delete data from the database, please check the input condition"
            print(msg)


    def transfer(self, table_source, table_target, delete_, condition=None, cols_reflection=None):
        """transfer data from one table to another.

        Args:
            table_source (str): data source.
            table_target (str): insert data into this table.
            delete_ (bool): will the initial data be deleted?
            condition (str, None): how to filter data from table_source. the condition must follow the rules of sql,
                                   whereas the word 'WHERE' does not need to be in the string. eg. "data >= 20191010"
            cols_reflection (tuple, None): ((column name in the initial table, column name in the target table), ) ,
                                    mention the column names must be in the tables. And when this parameter is a None,
                                    all columns in the former table would be moved to the other one,
                                    so be careful that table2 must have same table structure with table1
                                    (at least include the columns in the table1)
        """

        source_exist = self.table_exist(table_source)
        target_exist = self.table_exist(table_target)

        if not source_exist:
            self.create_table(table_source)
            msg = "the data source is empty"
            print(msg)

        if not target_exist:
            self.create_table(table_target)

        if self.db_type == 'sqlite' or self.db_type == 'oracle':

            if cols_reflection is None:
                sql_cmd = """INSERT INTO %s SELECT * FROM %s WHERE %s;
                          """ % (table_target, table_source, condition)
            else:
                cols_str1 = ",".join([x[0] for x in cols_reflection])
                cols_str2 = ",".join([x[1] for x in cols_reflection])
                sql_cmd = """INSERT INTO %s ( %s ) SELECT %s FROM %s WHERE %s; 
                          """ % (table_target, cols_str2, cols_str1, table_source, condition)

            try:
                self.cursor.execute(sql_cmd)
                self.conn.commit()
            except:
                msg = 'fail to transfer data between : %s , %s' % (table_source, table_target)
                print(msg)
                self.write_log(msg)
                self.conn.rollback()

        else:
            pass

        if delete_:
            self.delete(table_source, condition)

    def select(self, sql_cmd=None) -> pd.DataFrame:
        """

        Args:
            sql_cmd (str): sql command that begins with 'SELECT'

        Returns:
            pd.DataFrame: results
        """

        try:
            if self.db_type == 'sqlite':

                results = pd.read_sql(sql=sql_cmd, con=self.conn)

            else:
                results = pd.read_sql(sql=sql_cmd, con=self.conn)

            return results

        except Exception as e:
            self.conn.rollback()
            msg = "a sql string error leads to a mistake in data selection process, please check your grammar:"
            print(msg, e)
            return None

    def update(self, df, table_, keys_, names_):
        """

        Args:
            df(pd.DataFrame): the column names should be same as the selected database table
            table_ (str): table name
            keys_ (list): the primary keys of the table
            names_(list): the columns of the DataFrame waiting for upload.

        Returns:

        """
        copy_ = df[names_ + keys_].copy() # drop duplicates
        sql_ = "UPDATE " + table_ + " SET " + " and ".join([ x + " = ?" for x in names_]) + \
            "WHERE " + " and ".join(x + " = ? " for x in keys_)
        commit_lst = copy_.apply(lambda row: tuple(row), axis=1).to_list()
        try:
            self.cursor.executemany(sql_, commit_lst)
            self.conn.commit()
        except:
            msg = "fail to update columns : %s on the table %s"%(names_, table_)
            print(msg)
            self.write_log(msg)
            self.conn.rollback()

    def __columns_check(self, table_name, col_names):
        """
        confirm that column names in 'col_names' are listed in the table named 'table_name'

        Returns:
            bool:
        """

        table_cols = self.get_columns(table_name)
        if table_cols is not None or len(table_cols) > 0:
            col_names = [x.upper() for x in col_names]
            table_cols = [x.upper() for x in table_cols]
            cols_exist = set(col_names).issubset(set(table_cols))
            if not cols_exist:
                print(list(set(col_names).difference(set(table_cols))))
            return cols_exist
        else:
            return False


    def get_columns(self, table_name):
        """get column names

        Args:
            table_name (str):

        Returns:
            tuple: column names

        """
        user_name = None
        table_ = table_name
        if "." in table_name:
            user_name = table_name.split(".")[0]
            table_ = table_name.split(".")[1]

        cols = tuple([])
        if self.db_type == 'sqlite':

            sql_cmd = "PRAGMA table_info( %s )" % table_
            self.cursor.execute(sql_cmd)
            self.conn.commit()
            table_info = self.cursor.fetchall()
            cols = tuple([x[1] for x in table_info])

        elif self.db_type == 'mysql':

            sql_cmd = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' " % table_
            self.cursor.execute(sql_cmd)
            self.conn.commit()
            table_info = self.cursor.fetchall()
            cols = tuple([x[0] for x in table_info])

        elif self.db_type == 'oracle':

            if not self.table_exist(table_name):
                return None
            else:
                sql_cmd = "SELECT OWNER, TABLE_NAME, COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME =  '%s'" % table_
                self.cursor.execute(sql_cmd)
                self.conn.commit()
                table_info = self.cursor.fetchall()

                if user_name is None:
                    user_name = self.conn.username
                    user_name = user_name.upper()

                user_match = [x for x in table_info if x[0].upper() == user_name.upper()]
                if len(user_match) == 0:
                    msg = "no such a table '%s' under user '%s'; fail to return column names of the table" % (table_name, user_name)
                    print(msg)
                    cols = tuple([])
                else:
                    print("table exists; return column names.")
                    cols = tuple([x[2] for x in table_info])
                    cols = tuple(set(cols))

        return cols


    def table_exist(self, table_name):
        """judge the existence of a specific table.

        Args:
            table_name (str):

        Returns:
            bool: does the table exist or not
        """
        user_name = None
        if "." in table_name:
            user_name = table_name.split(".")[0]
            table_name = table_name.split(".")[1]

        if self.db_type == 'sqlite':
            sql_table_check = "PRAGMA TABLE_INFO( %s ) ;" % table_name
            self.cursor.execute(sql_table_check)
            self.conn.commit()
            result_ = self.cursor.fetchone()

            if (result_ is None) or len(result_) < 1:
                return False
            else:
                return True

        elif self.db_type == 'oracle':
            if user_name is not None:

                sql_table_check = "SELECT '%s' FROM ALL_TABLES WHERE OWNER = '%s'" % (table_name, user_name)
                self.cursor.execute(sql_table_check)
                self.conn.commit()
                result_ = self.cursor.fetchall()
                if len(result_) > 0:
                    print("find successfully a table '%s' under user '%s'" % (table_name, user_name))
                    return True
                else:
                    print("no table named '%s' under user '%s'" % (table_name, user_name))
                    return False

            else:

                sql_table_check = "SELECT '%s' FROM USER_TABLES" % (table_name)
                self.cursor.execute(sql_table_check)
                self.conn.commit()
                result_ = self.cursor.fetchall()

                if result_ is None or len(result_) < 1:
                    sql_table_check = "SELECT OWNER, TABLE_NAME FROM ALL_TABLES WHERE TABLE_NAME LIKE '%s'" % ('CSI_BOND_GZ_CF')
                    self.cursor.execute(sql_table_check)
                    self.conn.commit()
                    result_ = self.cursor.fetchall()

                    if len(result_) == 0 or result_ is None:
                        msg = "table %s does not exist in this database" % table_name
                    elif len(result_) == 1:
                        msg = "find a homonymous table '%s' under user '%s', please check if it's the one you want and input a new table name %s . %s" % (table_name, result_[0][0], result_[0][0], table_name)
                    else:
                        msg = f"at least 2 users in this database own a table named '{table_name}'; please input a more specific command by using a more clear 'table_name'. eg. user_name.table_name. available users: {user_name}"

                    print(msg)
                    return False

                else:
                    msg = "find '%s' under default user '%s'; accept it as your target table" % (table_name, self.conn.username)
                    print(msg)
                    return True

        elif self.db_type == 'mysql':
            pass


    def create_table(self, table_name, key_value=None):
        """

        Args:
            table_name (str): when creating a table in an oracle database, the table name can not be longer than 30 characters.
            key_value (tuple, None): form -> ((column_name, data_type, allow_NUll, is_primary_key, comment),)

        Returns:

        """

        try:
            if self.db_type == 'sqlite':
                if self.table_exist(table_name):
                    pass
                else:
                    column_info = ",".join(["  ".join([x[0], x[1], "" if x[2] else 'NOT NULL']) for x in key_value])
                    key_cols = [x[0] for x in key_value if x[3]]
                    primary_key_str = "PRIMARY KEY" + "(" + ",".join(key_cols) + ")" if len(key_cols) > 0 else ""
                    if len(primary_key_str) > 0:
                        sql_cmd = f"""CREATE TABLE {table_name} ({column_info} ,  {primary_key_str});"""
                    else:
                        sql_cmd = f"""CREATE TABLE {table_name} ({column_info});"""
                    self.cursor.execute(sql_cmd)
                    self.conn.commit()
                    msg = f'successfully built the new table {table_name}'
                    print(msg)

            else:
                ValueError()
        except ValueError:
            msg = "only accept 'sqlite'"
            print(msg)

        except sqlite3.OperationalError as e:
            msg = f"unable to create table {table_name} in database '{self.db_name}', error : {e}"
            print(msg)

    def digital_match(self, table_name, df_data):
        """before inserting data into table, use this method to transform digital into the form that corresponds with the table

        """

        if self.db_type == 'oracle':
            df_infos = self.columns_info(table_name)
            df_infos = df_infos[df_infos[3]=='NUMBER'].reset_index(drop=True)
            columns_info = dict(zip(df_infos[2], df_infos[6]))
            df_data.columns = [x.upper() for x in df_data.columns]
            precisions = {x: int(columns_info[x]) for x in columns_info if (x in df_data.columns)}
            df_data = df_data.round(precisions)

            # for x in precisions:
            #     df_data[x] = df_data[x].astype(float)
            #     df_data[x] = round(df_data[x], precisions[x])
        else:
            pass

        return df_data


    def columns_info(self, table_name):


        if self.db_type == 'sqlite':

            sql_table_info = """PRAGMA TABLE_INFO ( %s )""" % table_name
            self.cursor.execute(sql_table_info)
            self.conn.commit()
            df_infos = pd.DataFrame(self.cursor.fetchall())

        elif self.db_type == 'oracle':
            if len(table_name.split(".")) > 1:
                table_name_1 = table_name.split(".")[1]
                user_ = table_name.split(".")[0]
            else:
                table_name_1 = table_name
                user_ = None
            sql_table_info = """SELECT OWNER, TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '%s' """ % table_name_1
            self.cursor.execute(sql_table_info)
            self.conn.commit()
            df_infos = pd.DataFrame(self.cursor.fetchall())
            df_infos = df_infos[df_infos[0] == user_] if user_ is not None else df_infos

        else:
            df_infos = None

        return df_infos

    def drop(self, table_name):
        """delete a table

        """

        if self.db_type == 'sqlite':
            sql_cmd = """DROP TABLE %s ; """ % table_name
            try:
                self.cursor.execute(sql_cmd)
                self.conn.commit()
            except:
                msg = "unable to delete the table '%s'" % table_name
                print(msg)
                self.conn.rollback()

    def clear(self, table_name):
        """clear data

        """

        sql_cmd = "DELETE * FROM %s" % table_name
        self.cursor.execute(sql_cmd)
        self.conn.commit()

    def write_log(self, msg):
        str_ = datetime.now().strftime("%Y%m%d%H%M%S") + "|" + msg + " ( database: " + self.db_name + ")\n"

        with open(self.filename, "w") as f:
            f.write(str_)

    def close(self):
        """close the connection

        """
        self.conn.close()


