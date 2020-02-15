#!/usr/bin/env python
# encoding: utf-8

import psycopg2
import cx_Oracle
import pymysql
import pandas as pd
from sqlalchemy import create_engine

from Data.Configuration import data_config_settings

__author__ = "han"


class MysqlDataAccessor(object):
    """
    功能：mysql数据库的数据存取器
    描述：本类作为其它数据访问的工具类或者其他数据库交互类的基类使用
    配置：数据库连接串在data_config.ini中更新
    """
    def __init__(self, database_tag):
        self.db = data_config_settings.read_config_section_option(database_tag, "database")
        self.user = data_config_settings.read_config_section_option(database_tag, "user")
        self.password = data_config_settings.read_config_section_option(database_tag, "password")
        self.host = data_config_settings.read_config_section_option(database_tag, "host")
        self.port = data_config_settings.read_config_section_option(database_tag, "port")
        self.engine = self.create_engine()
        self.connection = pymysql.connect(host=self.host,
                                          port=int(self.port),
                                          user=self.user,
                                          password=self.password,
                                          db=self.db)
        self.cursor = self.connection.cursor()

    def close_client(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def select_data(self, sql_command, data_frame_column_names=None):
        self.cursor.execute(sql_command)
        results = self.cursor.fetchall()
        if not data_frame_column_names:
            col_des = self.cursor.description
            data_frame_column_names = [col_des[i][0] for i in range(len(col_des))]
        news_data_frame = pd.DataFrame(results, columns=data_frame_column_names)
        return news_data_frame

    def create_engine(self):
        return create_engine("mysql+pymysql://{}:{}@{}：{}/{}?charset=utf8".format(self.user, self.password, self.host,
                                                                                  self.port, self.db))


class OracleDataAccessor(object):
    """
    功能：oracle数据库的数据存取器
    描述：本类作为其它数据访问的工具类或者其他数据库交互类的基类使用
    配置：数据库连接串在data_config.ini中更新
    """
    def __init__(self, database_tag):
        self.db = data_config_settings.read_config_section_option(database_tag, "database")
        self.user = data_config_settings.read_config_section_option(database_tag, "user")
        self.password = data_config_settings.read_config_section_option(database_tag, "password")
        self.host = data_config_settings.read_config_section_option(database_tag, "host")
        self.port = data_config_settings.read_config_section_option(database_tag, "port")
        self.engine = self.create_engine()
        self.connection = cx_Oracle.connect("{}:{}@{}：{}/{}".format(self.user, self.password, self.host,
                                                                    self.port, self.db))
        self.cursor = self.connection.cursor()

    def close_client(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def select_data(self, sql_command, data_frame_column_names=None):
        self.cursor.execute(sql_command)
        results = self.cursor.fetchall()
        if not data_frame_column_names:
            col_des = self.cursor.description
            data_frame_column_names = [col_des[i][0] for i in range(len(col_des))]
        news_data_frame = pd.DataFrame(results, columns=data_frame_column_names)
        return news_data_frame

    def create_engine(self):
        return create_engine("oracle://{}:{}@{}：{}/{}".format(self.user, self.password, self.host,
                                                              self.port, self.db))


class PostgreDataAccessor(object):
    """
    功能：postgre数据库的数据存取器
    描述：本类作为其它数据访问的工具类或者其他数据库交互类的基类使用
    配置：数据库连接串在data_config.ini中更新
    """

    def __init__(self, database_tag):
        self.db = data_config_settings.read_config_section_option(database_tag, "database")
        self.user = data_config_settings.read_config_section_option(database_tag, "user")
        self.password = data_config_settings.read_config_section_option(database_tag, "password")
        self.host = data_config_settings.read_config_section_option(database_tag, "host")
        self.port = data_config_settings.read_config_section_option(database_tag, "port")
        self.engine = self.create_engine()
        self.connection = psycopg2.connect(database=self.db, user=self.user, password=self.password, host=self.host,
                                           port=self.port)
        self.cursor = self.connection.cursor()

    def close_client(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

    def select_data(self, sql_command, data_frame_column_names=None):
        self.cursor.execute(sql_command)
        results = self.cursor.fetchall()
        if not data_frame_column_names:
            col_des = self.cursor.description
            data_frame_column_names = [col_des[i][0] for i in range(len(col_des))]
        news_data_frame = pd.DataFrame(results, columns=data_frame_column_names)
        return news_data_frame

    def create_engine(self):
        return create_engine("postgresql+psycopg2://{}:{}@{}：{}/{}?charset=utf8".format(self.user, self.password,
                                                                                        self.host, self.port, self.db))
