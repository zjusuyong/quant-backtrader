# coding=gbk
# coding=utf-8

from tookit.database.db_api.alchemy_api import AlchemyAPI
from tookit.analysis_package import ap
from tookit.database.configure import models
import pandas as pd


def format_names(package_or_file_name, months=''):
    dict_name = {
        # ҵ��������ʷ
        'package_perf_hty': 'PerfHty',
        'file_perf_hty_sm': 'perf_hty_sm',
        'file_perf_hty_pp': 'perf_hty_pp',

        # ҵ���������¶���
        'package_perf_new_month': 'PerfNewMonth',
        'file_perf_new_month_sm': 'perf_new_month_sm_',
        'file_perf_new_month_pp': 'perf_new_month_pp_',

        # ҵ��������ʱ
        'package_perf_temp': 'PerfTemp',
        'file_perf_temp_pp': 'perf_temp_pp',
        'file_perf_temp_sm': 'perf_temp_sm',

        # ��ʯ������ʷ
        'package_cnst_hty': 'CnstHty',
        'file_cnst_hty': 'cnst_hty',

        # ��ʯ������ʱ
        'package_cnst_temp': 'CnsyTemp',
        'file_cnst_temp': 'cnst_temp'}

    return f'{dict_name[package_or_file_name]}{months}'


def statistic_tb(con, tb_name, map_tb_s, select_sql=None, field=None, func_prop='sum'):
    """
    ͳ�Ʊ��������ĳ���ֶε��ܺ�, ����:
        --> ʵ�����ӿ�
        --> ͳ������
        --> ͳ��ĳ�ֶ��ܺ�

    :param con: ���ݿ�����
    :param tb_name: str, Ҫͳ�Ƶı���
    :param map_tb_s: str, ӳ���
    :param select_sql: dict, default = None, ���ΪĬ��ֵ, �򷵻ر���������,
        sqlalchemy ��ѯ���, ����: {maptable.Id > 5, maptable.username == 'sd'}
    :param field: str, default = None, Ҫ����ͳ�Ƶ��ֶ���
    :param func_prop: str, default = 'sum', ͳ�Ʒ�ʽ
    :return: int, ����, int, ĳ�ֶ��ܺ�
    """

    # ʵ�����ӿ�
    a_api = AlchemyAPI(con)

    # ͳ��
    row_count = a_api.statistic(tb_name, map_tb_s, select_sql=select_sql, func_prop='count', field='is_valid')
    sum_tb = a_api.statistic(tb_name, map_tb_s, select_sql=select_sql, func_prop=func_prop, field=field)

    return row_count, sum_tb


def create_tb(con_w, tb_name_w, sum_field, read_excel=True, path_r=None, df=pd.DataFrame(),
              con_log=models.conn_logs, tb_name_log='log'):
    """
    ��ȡ���� df д�� mysql, ����:
        --> ��ȡ�����ļ�
        --> д�� mysql

    :param con_w: ���ݿ�����
    :param tb_name_w: str, Ҫд��ı���
    :param sum_field: str, ����ֶ�, ���ں˶�����
    :param read_excel: bool, default = True, ��ȡ�ļ���ʽ, Ĭ��Ϊ�� path_r ��ȡ, FalseΪ���մ���� df
    :param path_r: str, �����ļ���ȡ��ַ
    :param df: ����� df
    :param con_log: ������ڵ� sql ����
    :param tb_name_log: �����־�ı���
    :return: д���df
    """

    # ��ȡ����
    if read_excel:
        df = pd.read_excel(path_r)
    else:
        df = df

    for i in ['id_sql', 'createtime', 'updatetime']:
        if i in df.columns:
            df.drop(i, axis=1, inplace=True)

    ap.sound(notes=f'���: ��ȡ {path_r} �� df')

    # д��mysql
    df.to_sql(con=con_w, name=tb_name_w, if_exists='fail', index=False)
    ap.sound(notes=f'���: to_sql {tb_name_w}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    # ����id_col, ����Ϊ����, INT, ����, �ǿ�; ����createtime; ����updatetime;
    with con_w.connect() as conn:
        conn.execute(f'ALTER TABLE `{tb_name_w}` ADD `id_sql` INT(20) UNSIGNED NOT NULL AUTO_INCREMENT, '
                     f'ADD PRIMARY KEY (`id_sql`);')
        conn.execute(f'ALTER TABLE `{tb_name_w}` ADD `createtime` datetime DEFAULT CURRENT_TIMESTAMP;')
        conn.execute(f'ALTER TABLE `{tb_name_w}` ADD `updatetime` datetime '
                     f'DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;')
        ap.sound(notes=f'���: �޸�mysql: ���� id_sql, createtime, updatetime',
                 write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    a_api = AlchemyAPI(con_w)
    map_tb = a_api.mapping_tb(tb_name_w)

    # ͳ��
    row_count_df = df.shape[0]
    sum_df = df[sum_field].sum()
    row_count_tb, sum_tb = statistic_tb(con_w, tb_name_w, map_tb, select_sql={map_tb.is_valid == 1}, field=sum_field,
                                        func_prop='sum')

    # ��ӡͳ�ƽ��
    ap.sound(notes=f'��д��: ���� {row_count_df}, �ܺ� {sum_df}, д���: ���� {row_count_tb}, �ܺ� {sum_tb}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    ap.sound(notes=f'���: create_tb, from {path_r} �� df to {tb_name_w}')
    return df


def add_sth_to_tb(con_w, tb_name_w, map_tb_w, sum_field, read_from='tb', con_r=None, tb_name_r=None, path_r=None,
                  df=None, con_log=models.conn_logs, tb_name_log='log'):
    """
    ��ȡ tb �� excel �� df, ��ӵ� mysql, ����:
        --> ��ȡ ����
        --> ��ӵ� mysql

    :param con_w: Ҫд������ݿ�����
    :param tb_name_w: str, Ҫд��ı���
    :param map_tb_w: str, ׼��д����ӳ��
    :param sum_field: str, ����ֶ�, ���ں˶�����
    :param read_from: default = 'tb', ��ȡ���ݵ���Դ, Ĭ�ϴ����ݿ� tb ��ȡ, ��������ѡ��: tb, excel, df
    :param con_r: Ҫ��ȡ�����ݿ�����
    :param tb_name_r: str, Ҫ��ȡ�����ݿ����
    :param path_r: str, Ҫ��ȡ���ļ�·��
    :param df: ����ı���
    :param con_log: ������ڵ� sql ����
    :param tb_name_log: �����־�ı���
    :return: д���df
    """

    # ��ȡ����
    if read_from == 'tb':
        df = pd.read_sql(con=con_r, sql=tb_name_r)
        df = df[df['is_valid'] == 1]
    elif read_from == 'excel':
        df = pd.read_excel(path_r)
    else:
        df = df

    for i in ['id_sql', 'createtime', 'updatetime']:
        if i in df.columns:
            df.drop(i, axis=1, inplace=True)

    ap.sound(notes=f'���: ��ȡ {tb_name_r} �� {path_r} �� df')

    # ͳ��: д��ǰ����
    row_count_tb_add_before, sum_tb_add_before = statistic_tb(con_w, tb_name_w, map_tb_w,
                                                              select_sql={map_tb_w.is_valid == 1}, field=sum_field,
                                                              func_prop='sum')
    # ��ӵ� mysql
    df.to_sql(con=con_w, name=tb_name_w, if_exists='append', index=False)
    ap.sound(notes=f'���: to_sql {tb_name_w}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    # ͳ��: ��д������ & д�������
    row_count_df = df.shape[0]
    sum_df = df[sum_field].sum()
    row_count_tb_add_after, sum_tb_add_after = statistic_tb(con_w, tb_name_w, map_tb_w,
                                                            select_sql={map_tb_w.is_valid == 1}, field=sum_field,
                                                            func_prop='sum')

    # ��ӡͳ�ƽ��
    ap.sound(notes=f'д��ǰ: ���� {row_count_tb_add_before}, �ܺ� {sum_tb_add_before}, '
                   f'��д��: ���� {row_count_df}, �ܺ� {sum_df}, '
                   f'д���: ���� {row_count_tb_add_after}, �ܺ� {sum_tb_add_after}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    ap.sound(notes=f'���: add_sth_to_tb, from {tb_name_r} �� {path_r} �� df to {tb_name_w}')
    return df


def replace_tb(con_w, tb_name_w, map_tb_w, select_sql, sum_field, read_from='tb', con_r=None, tb_name_r=None,
               path_r=None, df=None, logical_del_way=True, con_log=models.conn_logs, tb_name_log='log'):
    """
    �滻���ݿ� tb_name_w ����, ����:
        --> ʵ�����ӿ� & ӳ���
        --> �߼�ɾ���������� del_res
        --> �� tb_name_r �� path_r �� df ��ȡҪ��ӵ����� df_add
        --> �� df_add ��ӵ� tb_name_w

    :param con_w: Ҫд������ݿ�����
    :param tb_name_w: str, Ҫд��ı���
    :param map_tb_w: Ҫ�滻���ݵı��ӳ��
    :param select_sql: dict, sqlalchemy ��ѯ���, ����: {maptable.Id > 5, maptable.username == 'sd'}
    :param sum_field: str, ����ֶ�, ���ں˶�����
    :param read_from: default = 'tb', ��ȡ���ݵ���Դ, Ĭ�ϴ����ݿ� tb ��ȡ, ��������ѡ��: tb, excel, df
    :param con_r: Ҫ��ȡ�����ݿ�����
    :param tb_name_r: Ҫ��ȡ�ı���
    :param path_r: Ҫ��ȡ�ļ���·��
    :param df: Ҫ����� df
    :param logical_del_way: bool, default = Ture, ɾ�����滻���ݵķ�ʽ, Ĭ��Ϊ�߼�ɾ��
    :param con_log: ������ڵ� sql ����
    :param tb_name_log: �����־�ı���
    :return: д���df
    """

    # ʵ�����ӿ�
    a_api = AlchemyAPI(con_w)

    # ͳ��: ɾ��ǰ������
    row_count_tb_del_before, sum_tb_del_before = statistic_tb(con_w, tb_name_w, map_tb_w,
                                                              select_sql={map_tb_w.is_valid == 1}, field=sum_field,
                                                              func_prop='sum')

    # �߼�ɾ�� �� ����ɾ�� ���滻������
    if logical_del_way:
        del_res = a_api.logical_del(tb_name_w, map_tb_w, select_sql)
    else:
        del_res = a_api.physical_del(tb_name_w, map_tb_w, select_sql)

    # ͳ��: ��ɾ������ & ɾ���������
    row_count_del_res = del_res.shape[0]
    if sum_field in del_res.columns:  # ���ɸѡ���Ϊ��, ��ô��ͻᱨ��, �����ڴ˸�ֵΪ0
        sum_del_res = del_res[sum_field].sum()
    else:
        sum_del_res = 0
    row_count_tb_del_after, sum_tb_del_after = statistic_tb(con_w, tb_name_w, map_tb_w,
                                                            select_sql={map_tb_w.is_valid == 1}, field=sum_field,
                                                            func_prop='sum')

    # ��ӡͳ�ƽ��
    ap.sound(notes=f'ɾ��ǰ: ���� {row_count_tb_del_before}, �ܺ� {sum_tb_del_before}, '
                   f'��ɾ��: ���� {row_count_del_res}, �ܺ� {sum_del_res}, '
                   f'ɾ����: ���� {row_count_tb_del_after}, �ܺ� {sum_tb_del_after}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    # ��Ӷ�ȡ������
    df_add = add_sth_to_tb(con_w, tb_name_w, map_tb_w, sum_field, read_from=read_from, con_r=con_r, tb_name_r=tb_name_r,
                           path_r=path_r, df=df)

    ap.sound(notes=f'���: replace_tb, add {tb_name_r} �� {path_r} �� df to {tb_name_w}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)
    return df_add
