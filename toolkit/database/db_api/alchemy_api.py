# coding=gbk

from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from tookit.database.configure import models
from tookit.analysis_package import ap
import pandas as pd


class AlchemyAPI(object):
    def __init__(self, engine):
        self.engine = engine
        session = sessionmaker(bind=self.engine)
        self.session = session()

    def mapping_tb(self, tb_name):
        from sqlalchemy.ext.declarative import declarative_base
        base = declarative_base()
        from sqlalchemy import MetaData, Table
        metadata = MetaData(self.engine)

        class MapTable(base):
            __table__ = Table(tb_name, metadata, autoload=True)

        return MapTable

    def add_one(self, tb_name, map_tb, data_dict, con_log=models.conn_logs, tb_name_log='log'):
        """
        向数据库添加一条信息, 过程:
            --> 添加信息
            --> 提交

        :param tb_name: str, 表名
        :param map_tb: 映射表, 需要调用 map_tb = mapping_tb()方法, 得到映射表, 再传进来
        :param data_dict: dict, 要添加的信息, 例如: data_dict = {'username': 'ww', 'password': '123456', 'email': 'ww@163.com'}
        :param con_log: 输出日期的 sql 连接
        :param tb_name_log: 输出日志的表名
        :return: 要添加的信息
        """

        objs = map_tb(**data_dict)

        # noinspection PyBroadException
        try:
            self.session.add(objs)
            self.session.commit()
            ap.sound(notes=f'add_one 完成: 向 {tb_name} 添加信息',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        except Exception as error:
            self.session.rollback()
            ap.sound(notes=f'add_one 错误: {error}',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        return data_dict

    def add_all(self, tb_name, map_tb, data_list, con_log=models.conn_logs, tb_name_log='log'):
        """
        向数据库批量添加信息, 过程:
            --> 添加信息
            --> 提交

        :param tb_name: str, 表名
        :param map_tb: 映射表, 需要调用 map_tb = mapping_tb()方法, 得到映射表, 再传进来
        :param data_list: list, 要添加的信息,
            例如: data_list = [{'username': 'hello', 'password': '1234'}, {'q': 'hello2', 'password': '1234'}]
        :param con_log: 输出日期的 sql 连接
        :param tb_name_log: 输出日志的表名
        :return: 要添加的信息
        """

        # noinspection PyBroadException
        try:
            self.engine.execute(map_tb.__table__.insert(), data_list)
            self.session.commit()
            ap.sound(notes=f'add_all 完成: 向 {tb_name} 添加信息',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        except Exception as error:
            self.session.rollback()
            ap.sound(notes=f'add_all 错误: {error}',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        return data_list

    def physical_del(self, tb_name, map_tb, select_sql, con_log=models.conn_logs, tb_name_log='log'):
        """
        物理删除数据库中的指定内容, 过程:
            --> 查找指定内容
            --> 判断查找内容不为空
            --> 逐行删除查找到的内容, 但不提交
            --> 提交

        :param tb_name: str, 表名
        :param map_tb: 映射表, 需要调用 map_tb = mapping_tb()方法, 得到映射表, 再传进来
        :param select_sql: dict, sqlalchemy 查询语句, 例如: {maptable.Id > 5, maptable.username == 'sd'}
        :param con_log: 输出日期的 sql 连接
        :param tb_name_log: 输出日志的表名
        :return: 查询的内容
        """

        res = self.session.query(map_tb).filter(*select_sql).all()
        if res and res is not None:
            df = ap.module_to_df(res)
            # noinspection PyBroadException
            try:
                [self.session.delete(row) for row in res]
                self.session.commit()
                ap.sound(notes=f'physical_del 完成: 从 {tb_name} 数据库, 删除了 {df.shape[0]} 行',
                         write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

            except Exception as error:
                self.session.rollback()
                ap.sound(notes=f'physical_del 错误: {error}',
                         write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        else:
            df = pd.DataFrame()
            ap.sound(notes=f'physical_del 错误: 筛选结果为空',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)
        return df

    def logical_del(self, tb_name, map_tb, select_sql, con_log=models.conn_logs, tb_name_log='log'):
        """
        逻辑删除数据库中的指定内容, 过程:
            --> 调用 modify() 方法, 将要删除内容 is_valid 字段的值改为 0

        :param tb_name: str, 表名
        :param map_tb: 映射表, 需要调用 map_tb = mapping_tb()方法, 得到映射表, 再传进来
        :param select_sql: dict, sqlalchemy 查询语句, 例如: {maptable.Id > 5, maptable.username == 'sd'}
        :param con_log: 输出日期的 sql 连接
        :param tb_name_log: 输出日志的表名
        :return: 转换为 df 的查询内容
        """

        df = self.modify(tb_name, map_tb, select_sql, {'is_valid': 0})
        ap.sound(notes=f'logical_del 完成: from {tb_name} 逻辑删除 {df.shape[0]} 行',
                 write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)
        return df

    def modify(self, tb_name, map_tb, select_sql, modify_dict, con_log=models.conn_logs, tb_name_log='log'):
        """
        修改数据库中的内容, 过程:
            --> 查找指定内容
            --> 判断查找内容不为空
            --> 逐行、逐个字段, 修改被查找的内容, 但不提交
            --> 提交

        :param tb_name: str, 表名
        :param map_tb: 映射表, 需要调用 map_tb = mapping_tb()方法, 得到映射表, 再传进来
        :param select_sql: dict, sqlalchemy 查询语句, 例如: {maptable.Id > 5, maptable.username == 'sd'}
        :param modify_dict: dict, 将要修改的字段和修改后的内容组成字典, 例如: {'username': 'ok', 'password': '678'}
        :param con_log: 输出日期的 sql 连接
        :param tb_name_log: 输出日志的表名
        :return: 转换为 df 的查询内容
        """

        res = self.session.query(map_tb).filter(*select_sql).all()
        if res and res is not None:
            df = ap.module_to_df(res)
            # noinspection PyBroadException
            try:
                for row in res:
                    for keys in modify_dict:
                        setattr(row, keys, modify_dict[keys])
                self.session.commit()
                ap.sound(notes=f'modify 完成: 修改 {tb_name}',
                         write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

            except Exception as error:
                self.session.rollback()
                ap.sound(notes=f'modify 错误: {error}',
                         write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        else:
            df = pd.DataFrame()
            ap.sound(notes=f'modify 错误: 筛选结果为空',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        return df

    def query(self, tb_name, map_tb, select_sql):
        """
        查询, 并将结果转换为 df

        :param tb_name: str, 表名
        :param map_tb: 映射表, 需要调用 map_tb = mapping_tb()方法, 得到映射表, 再传进来
        :param select_sql: dict, sqlalchemy 查询语句, 例如: {maptable.Id > 5, maptable.username == 'sd'}
        :return: 转换为 df 的查询内容
        """

        # 将查询结果 res 转换为 df
        res = self.session.query(map_tb).filter(*select_sql).all()
        df = ap.module_to_df(res)
        ap.sound(notes=f'query 完成: 查询: {tb_name}, 行数: {df.shape[0]}')

        return df

    def statistic(self, tb_name, map_tb, select_sql=None, field=None, func_prop='sum'):
        """
        对筛选内容的某个字段进行统计

        :param tb_name: str, 表名
        :param map_tb: 映射表, 需要调用 map_tb = mapping_tb()方法, 得到映射表, 再传进来
        :param select_sql: dict, default = None, 如果为默认值, 则返回表所有内容,
            sqlalchemy 查询语句, 例如: {maptable.Id > 5, maptable.username == 'sd'}
        :param field: str, default = None, 要进行统计的字段名
        :param func_prop: str, default = 'sum', 统计方式
        :return: int, 统计结果
        """

        funcs = getattr(func, func_prop)

        if select_sql is None:
            statistic_res = self.session.query(funcs(getattr(map_tb, field))).filter().scalar()
        else:
            statistic_res = self.session.query(funcs(getattr(map_tb, field))).filter(*select_sql).scalar()
        ap.sound(notes=f'statistic 完成: 统计表: {tb_name}, 字段: {field}, 统计方式: {func_prop}, 统计结果: {statistic_res}')

        return statistic_res
