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
        �����ݿ����һ����Ϣ, ����:
            --> �����Ϣ
            --> �ύ

        :param tb_name: str, ����
        :param map_tb: ӳ���, ��Ҫ���� map_tb = mapping_tb()����, �õ�ӳ���, �ٴ�����
        :param data_dict: dict, Ҫ��ӵ���Ϣ, ����: data_dict = {'username': 'ww', 'password': '123456', 'email': 'ww@163.com'}
        :param con_log: ������ڵ� sql ����
        :param tb_name_log: �����־�ı���
        :return: Ҫ��ӵ���Ϣ
        """

        objs = map_tb(**data_dict)

        # noinspection PyBroadException
        try:
            self.session.add(objs)
            self.session.commit()
            ap.sound(notes=f'add_one ���: �� {tb_name} �����Ϣ',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        except Exception as error:
            self.session.rollback()
            ap.sound(notes=f'add_one ����: {error}',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        return data_dict

    def add_all(self, tb_name, map_tb, data_list, con_log=models.conn_logs, tb_name_log='log'):
        """
        �����ݿ����������Ϣ, ����:
            --> �����Ϣ
            --> �ύ

        :param tb_name: str, ����
        :param map_tb: ӳ���, ��Ҫ���� map_tb = mapping_tb()����, �õ�ӳ���, �ٴ�����
        :param data_list: list, Ҫ��ӵ���Ϣ,
            ����: data_list = [{'username': 'hello', 'password': '1234'}, {'q': 'hello2', 'password': '1234'}]
        :param con_log: ������ڵ� sql ����
        :param tb_name_log: �����־�ı���
        :return: Ҫ��ӵ���Ϣ
        """

        # noinspection PyBroadException
        try:
            self.engine.execute(map_tb.__table__.insert(), data_list)
            self.session.commit()
            ap.sound(notes=f'add_all ���: �� {tb_name} �����Ϣ',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        except Exception as error:
            self.session.rollback()
            ap.sound(notes=f'add_all ����: {error}',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        return data_list

    def physical_del(self, tb_name, map_tb, select_sql, con_log=models.conn_logs, tb_name_log='log'):
        """
        ����ɾ�����ݿ��е�ָ������, ����:
            --> ����ָ������
            --> �жϲ������ݲ�Ϊ��
            --> ����ɾ�����ҵ�������, �����ύ
            --> �ύ

        :param tb_name: str, ����
        :param map_tb: ӳ���, ��Ҫ���� map_tb = mapping_tb()����, �õ�ӳ���, �ٴ�����
        :param select_sql: dict, sqlalchemy ��ѯ���, ����: {maptable.Id > 5, maptable.username == 'sd'}
        :param con_log: ������ڵ� sql ����
        :param tb_name_log: �����־�ı���
        :return: ��ѯ������
        """

        res = self.session.query(map_tb).filter(*select_sql).all()
        if res and res is not None:
            df = ap.module_to_df(res)
            # noinspection PyBroadException
            try:
                [self.session.delete(row) for row in res]
                self.session.commit()
                ap.sound(notes=f'physical_del ���: �� {tb_name} ���ݿ�, ɾ���� {df.shape[0]} ��',
                         write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

            except Exception as error:
                self.session.rollback()
                ap.sound(notes=f'physical_del ����: {error}',
                         write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        else:
            df = pd.DataFrame()
            ap.sound(notes=f'physical_del ����: ɸѡ���Ϊ��',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)
        return df

    def logical_del(self, tb_name, map_tb, select_sql, con_log=models.conn_logs, tb_name_log='log'):
        """
        �߼�ɾ�����ݿ��е�ָ������, ����:
            --> ���� modify() ����, ��Ҫɾ������ is_valid �ֶε�ֵ��Ϊ 0

        :param tb_name: str, ����
        :param map_tb: ӳ���, ��Ҫ���� map_tb = mapping_tb()����, �õ�ӳ���, �ٴ�����
        :param select_sql: dict, sqlalchemy ��ѯ���, ����: {maptable.Id > 5, maptable.username == 'sd'}
        :param con_log: ������ڵ� sql ����
        :param tb_name_log: �����־�ı���
        :return: ת��Ϊ df �Ĳ�ѯ����
        """

        df = self.modify(tb_name, map_tb, select_sql, {'is_valid': 0})
        ap.sound(notes=f'logical_del ���: from {tb_name} �߼�ɾ�� {df.shape[0]} ��',
                 write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)
        return df

    def modify(self, tb_name, map_tb, select_sql, modify_dict, con_log=models.conn_logs, tb_name_log='log'):
        """
        �޸����ݿ��е�����, ����:
            --> ����ָ������
            --> �жϲ������ݲ�Ϊ��
            --> ���С�����ֶ�, �޸ı����ҵ�����, �����ύ
            --> �ύ

        :param tb_name: str, ����
        :param map_tb: ӳ���, ��Ҫ���� map_tb = mapping_tb()����, �õ�ӳ���, �ٴ�����
        :param select_sql: dict, sqlalchemy ��ѯ���, ����: {maptable.Id > 5, maptable.username == 'sd'}
        :param modify_dict: dict, ��Ҫ�޸ĵ��ֶκ��޸ĺ����������ֵ�, ����: {'username': 'ok', 'password': '678'}
        :param con_log: ������ڵ� sql ����
        :param tb_name_log: �����־�ı���
        :return: ת��Ϊ df �Ĳ�ѯ����
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
                ap.sound(notes=f'modify ���: �޸� {tb_name}',
                         write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

            except Exception as error:
                self.session.rollback()
                ap.sound(notes=f'modify ����: {error}',
                         write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        else:
            df = pd.DataFrame()
            ap.sound(notes=f'modify ����: ɸѡ���Ϊ��',
                     write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name)

        return df

    def query(self, tb_name, map_tb, select_sql):
        """
        ��ѯ, �������ת��Ϊ df

        :param tb_name: str, ����
        :param map_tb: ӳ���, ��Ҫ���� map_tb = mapping_tb()����, �õ�ӳ���, �ٴ�����
        :param select_sql: dict, sqlalchemy ��ѯ���, ����: {maptable.Id > 5, maptable.username == 'sd'}
        :return: ת��Ϊ df �Ĳ�ѯ����
        """

        # ����ѯ��� res ת��Ϊ df
        res = self.session.query(map_tb).filter(*select_sql).all()
        df = ap.module_to_df(res)
        ap.sound(notes=f'query ���: ��ѯ: {tb_name}, ����: {df.shape[0]}')

        return df

    def statistic(self, tb_name, map_tb, select_sql=None, field=None, func_prop='sum'):
        """
        ��ɸѡ���ݵ�ĳ���ֶν���ͳ��

        :param tb_name: str, ����
        :param map_tb: ӳ���, ��Ҫ���� map_tb = mapping_tb()����, �õ�ӳ���, �ٴ�����
        :param select_sql: dict, default = None, ���ΪĬ��ֵ, �򷵻ر���������,
            sqlalchemy ��ѯ���, ����: {maptable.Id > 5, maptable.username == 'sd'}
        :param field: str, default = None, Ҫ����ͳ�Ƶ��ֶ���
        :param func_prop: str, default = 'sum', ͳ�Ʒ�ʽ
        :return: int, ͳ�ƽ��
        """

        funcs = getattr(func, func_prop)

        if select_sql is None:
            statistic_res = self.session.query(funcs(getattr(map_tb, field))).filter().scalar()
        else:
            statistic_res = self.session.query(funcs(getattr(map_tb, field))).filter(*select_sql).scalar()
        ap.sound(notes=f'statistic ���: ͳ�Ʊ�: {tb_name}, �ֶ�: {field}, ͳ�Ʒ�ʽ: {func_prop}, ͳ�ƽ��: {statistic_res}')

        return statistic_res
