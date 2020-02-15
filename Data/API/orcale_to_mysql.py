#!/usr/bin/env python
# encoding: utf-8
import pandas as pd
from Data.API.database_accessor import OracleDataAccessor, MysqlDataAccessor

__author__ = "han"


class OracleDB(OracleDataAccessor):
    """
    功能：oracle数据存储器
    配置：数据库连接串在data_config.ini中更新
    """
    def __init__(self):
        super(OracleDB, self).__init__("oracle")

    def select_full_data(self, table_name):
        """
        取全量数据
        :param table_name:
        :return:
        """
        sql_comand = "select * from {table_name}".format(table_name=table_name)
        select_data = self.select_data(sql_comand)
        return select_data

    def select_increase_data(self, table_name, c_timestamp):
        """
        取时间大于某时间戳的增量数据
        :param table_name:
        :param c_timestamp:
        :return:
        """
        sql_command = "select * from {table_name} where c_timestamp > '{c_timestamp}'"
        sql_command = sql_command.format(table_name=table_name, c_timestamp=c_timestamp)
        select_data = self.select_data(sql_command)
        return select_data


class MysqlDB(MysqlDataAccessor):
    """
    功能：mysql数据库的数据存储器
    配置：数据库连接串在data_config.ini中更新
    """

    def __init__(self):
        super(MysqlDB, self).__init__("mysql")

    def get_data_count(self, table_name):
        """
        获取表总数据总量
        :param table_name:
        :return:
        """
        sql_command = "select count(*) from {table_name}".format(table_name=table_name)
        select_data = self.select_data(sql_command)
        return select_data.values[0][0]

    def insert_data(self, table_name, data:pd.DataFrame):
        """
        向数据表中插入DataFrame
        :param table_name:
        :param data:
        :return:
        """
        try:
            for i in range(0, data.shape[0]+1, 2000):
                _data = data.iloc[i:min(i+2000, data.shape[0]+1)]
                _data.to_sql(name=table_name, con=self.engine, if_exist="append", index=False)
        except Exception as e:
            self.connection.rollback()
            print("something wrong, roolback")
            raise e
        else:
            self.connection.commit()
        return True

    def truncate(self, table_name):
        """
        删除表中所有数据
        :param table:
        :return:
        """
        sql_command = "truncate table {table_name}".format(table_name=table_name)
        self.cursor.execute(sql_command)
        results = self.cursor.fetchall()
        return results

    def delete(self, table_name, rows=0):
        """
        删除表中后n条数据
        :param table_name:
        :return:
        """
        sql_command = "delete from {table_name} order by id desc limit {rows}".format(table_name=table_name, rows=rows)
        self.cursor.execute(sql_command)
        results = self.cursor.fetchall()
        return results
