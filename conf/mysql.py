# coding:utf-8

import pymysql
import logging
from conf.config import *

logger = logging.getLogger('MySQL')


class MySQL:
    def __init__(self):
        self.host = OASIS_DB_HOST
        self.port = OASIS_DB_PORT
        self.user = OASIS_DB_USERNAME
        self.password = OASIS_DB_PASSWORD
        self.database = OASIS_DB_DATABASE
        self.cursor = None
        self.conn = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database)

    def begin(self):
        se
        self.conn.begin()

    def rollback(self):
        self.conn.rollback()

    def commit(self):
        self.conn.commit()

    # 连接数据库
    def connect(self):
        try:
            self.conn.ping()
        except self.conn.Error:
            self.conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database)
        self.cursor = self.conn.cursor()

    # 关闭连接
    def close(self):
        self.conn.close()

    # 查询数据
    def query_one(self, sql):
        try:
            self.connect()
            self.cursor.execute(sql)
            return self.cursor.fetchone(), True
        except self.conn.Error:
            logger.error("select failed, sql: {}, err: {}".format(sql, self.conn.Error.with_traceback()))
            return None, False

    # 查询数据
    def query_all(self, sql):
        try:
            self.connect()
            self.cursor.execute(sql)
            return self.cursor.fetchall(), True
        except self.conn.Error:
            logger.error("select failed, sql: {}, err: {}".format(sql, self.conn.Error.with_traceback()))
            return None, False

    # 插入数据
    def insert(self, sql):
        return self.__edit(sql)

    # 修改数据
    def update(self, sql):
        return self.__edit(sql)

    # 删除数据
    def delete(self, sql):
        return self.__edit(sql)

    def __edit(self, sql):
        try:
            self.connect()
            count = self.cursor.execute(sql)
            self.conn.commit()
            self.close()
            return count, True
        except self.conn.Error:
            logger.error("modify failed, sql: {}, err: {}".format(sql, self.conn.Error.with_traceback()))
            self.conn.rollback()
            return 0, False


Connection = MySQL().conn
Cursor = Connection.cursor()
