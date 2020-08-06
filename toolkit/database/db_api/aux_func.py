# coding=gbk
# coding=utf-8

from tookit.database.db_api.alchemy_api import AlchemyAPI
from tookit.analysis_package import ap
from tookit.database.configure import models
import pandas as pd


def format_names(package_or_file_name, months=''):
    dict_name = {
        # 业绩——历史
        'package_perf_hty': 'PerfHty',
        'file_perf_hty_sm': 'perf_hty_sm',
        'file_perf_hty_pp': 'perf_hty_pp',

        # 业绩——各月定版
        'package_perf_new_month': 'PerfNewMonth',
        'file_perf_new_month_sm': 'perf_new_month_sm_',
        'file_perf_new_month_pp': 'perf_new_month_pp_',

        # 业绩——临时
        'package_perf_temp': 'PerfTemp',
        'file_perf_temp_pp': 'perf_temp_pp',
        'file_perf_temp_sm': 'perf_temp_sm',

        # 基石——历史
        'package_cnst_hty': 'CnstHty',
        'file_cnst_hty': 'cnst_hty',

        # 基石——临时
        'package_cnst_temp': 'CnsyTemp',
        'file_cnst_temp': 'cnst_temp'}

    return f'{dict_name[package_or_file_name]}{months}'


def statistic_tb(con, tb_name, map_tb_s, select_sql=None, field=None, func_prop='sum'):
    """
    统计表的行数和某个字段的总和, 过程:
        --> 实例化接口
        --> 统计行数
        --> 统计某字段总和

    :param con: 数据库连接
    :param tb_name: str, 要统计的表名
    :param map_tb_s: str, 映射表
    :param select_sql: dict, default = None, 如果为默认值, 则返回表所有内容,
        sqlalchemy 查询语句, 例如: {maptable.Id > 5, maptable.username == 'sd'}
    :param field: str, default = None, 要进行统计的字段名
    :param func_prop: str, default = 'sum', 统计方式
    :return: int, 行数, int, 某字段总和
    """

    # 实例化接口
    a_api = AlchemyAPI(con)

    # 统计
    row_count = a_api.statistic(tb_name, map_tb_s, select_sql=select_sql, func_prop='count', field='is_valid')
    sum_tb = a_api.statistic(tb_name, map_tb_s, select_sql=select_sql, func_prop=func_prop, field=field)

    return row_count, sum_tb


def create_tb(con_w, tb_name_w, sum_field, read_excel=True, path_r=None, df=pd.DataFrame(),
              con_log=models.conn_logs, tb_name_log='log'):
    """
    读取本地 df 写入 mysql, 过程:
        --> 读取本地文件
        --> 写入 mysql

    :param con_w: 数据库连接
    :param tb_name_w: str, 要写入的表名
    :param sum_field: str, 求和字段, 用于核对数据
    :param read_excel: bool, default = True, 读取文件方式, 默认为从 path_r 读取, False为接收传入的 df
    :param path_r: str, 本地文件读取地址
    :param df: 传入的 df
    :param con_log: 输出日期的 sql 连接
    :param tb_name_log: 输出日志的表名
    :return: 写入的df
    """

    # 读取数据
    if read_excel:
        df = pd.read_excel(path_r)
    else:
        df = df

    for i in ['id_sql', 'createtime', 'updatetime']:
        if i in df.columns:
            df.drop(i, axis=1, inplace=True)

    ap.sound(notes=f'完成: 读取 {path_r} 或 df')

    # 写入mysql
    df.to_sql(con=con_w, name=tb_name_w, if_exists='fail', index=False)
    ap.sound(notes=f'完成: to_sql {tb_name_w}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    # 创建id_col, 设置为主键, INT, 自增, 非空; 创建createtime; 创建updatetime;
    with con_w.connect() as conn:
        conn.execute(f'ALTER TABLE `{tb_name_w}` ADD `id_sql` INT(20) UNSIGNED NOT NULL AUTO_INCREMENT, '
                     f'ADD PRIMARY KEY (`id_sql`);')
        conn.execute(f'ALTER TABLE `{tb_name_w}` ADD `createtime` datetime DEFAULT CURRENT_TIMESTAMP;')
        conn.execute(f'ALTER TABLE `{tb_name_w}` ADD `updatetime` datetime '
                     f'DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;')
        ap.sound(notes=f'完成: 修改mysql: 创建 id_sql, createtime, updatetime',
                 write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    a_api = AlchemyAPI(con_w)
    map_tb = a_api.mapping_tb(tb_name_w)

    # 统计
    row_count_df = df.shape[0]
    sum_df = df[sum_field].sum()
    row_count_tb, sum_tb = statistic_tb(con_w, tb_name_w, map_tb, select_sql={map_tb.is_valid == 1}, field=sum_field,
                                        func_prop='sum')

    # 打印统计结果
    ap.sound(notes=f'被写入: 行数 {row_count_df}, 总和 {sum_df}, 写入后: 行数 {row_count_tb}, 总和 {sum_tb}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    ap.sound(notes=f'完成: create_tb, from {path_r} 或 df to {tb_name_w}')
    return df


def add_sth_to_tb(con_w, tb_name_w, map_tb_w, sum_field, read_from='tb', con_r=None, tb_name_r=None, path_r=None,
                  df=None, con_log=models.conn_logs, tb_name_log='log'):
    """
    读取 tb 或 excel 或 df, 添加到 mysql, 过程:
        --> 读取 数据
        --> 添加到 mysql

    :param con_w: 要写入的数据库连接
    :param tb_name_w: str, 要写入的表名
    :param map_tb_w: str, 准备写入表的映射
    :param sum_field: str, 求和字段, 用于核对数据
    :param read_from: default = 'tb', 读取数据的来源, 默认从数据库 tb 读取, 共有三个选项: tb, excel, df
    :param con_r: 要读取的数据库连接
    :param tb_name_r: str, 要读取的数据库表名
    :param path_r: str, 要读取的文件路径
    :param df: 传入的表名
    :param con_log: 输出日期的 sql 连接
    :param tb_name_log: 输出日志的表名
    :return: 写入的df
    """

    # 读取数据
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

    ap.sound(notes=f'完成: 读取 {tb_name_r} 或 {path_r} 或 df')

    # 统计: 写入前数据
    row_count_tb_add_before, sum_tb_add_before = statistic_tb(con_w, tb_name_w, map_tb_w,
                                                              select_sql={map_tb_w.is_valid == 1}, field=sum_field,
                                                              func_prop='sum')
    # 添加到 mysql
    df.to_sql(con=con_w, name=tb_name_w, if_exists='append', index=False)
    ap.sound(notes=f'完成: to_sql {tb_name_w}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    # 统计: 被写入数据 & 写入后数据
    row_count_df = df.shape[0]
    sum_df = df[sum_field].sum()
    row_count_tb_add_after, sum_tb_add_after = statistic_tb(con_w, tb_name_w, map_tb_w,
                                                            select_sql={map_tb_w.is_valid == 1}, field=sum_field,
                                                            func_prop='sum')

    # 打印统计结果
    ap.sound(notes=f'写入前: 行数 {row_count_tb_add_before}, 总和 {sum_tb_add_before}, '
                   f'被写入: 行数 {row_count_df}, 总和 {sum_df}, '
                   f'写入后: 行数 {row_count_tb_add_after}, 总和 {sum_tb_add_after}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    ap.sound(notes=f'完成: add_sth_to_tb, from {tb_name_r} 或 {path_r} 或 df to {tb_name_w}')
    return df


def replace_tb(con_w, tb_name_w, map_tb_w, select_sql, sum_field, read_from='tb', con_r=None, tb_name_r=None,
               path_r=None, df=None, logical_del_way=True, con_log=models.conn_logs, tb_name_log='log'):
    """
    替换数据库 tb_name_w 数据, 过程:
        --> 实例化接口 & 映射表
        --> 逻辑删除查找内容 del_res
        --> 从 tb_name_r 或 path_r 或 df 读取要添加的数据 df_add
        --> 将 df_add 添加到 tb_name_w

    :param con_w: 要写入的数据库连接
    :param tb_name_w: str, 要写入的表名
    :param map_tb_w: 要替换内容的表的映射
    :param select_sql: dict, sqlalchemy 查询语句, 例如: {maptable.Id > 5, maptable.username == 'sd'}
    :param sum_field: str, 求和字段, 用于核对数据
    :param read_from: default = 'tb', 读取数据的来源, 默认从数据库 tb 读取, 共有三个选项: tb, excel, df
    :param con_r: 要读取的数据库连接
    :param tb_name_r: 要读取的表名
    :param path_r: 要读取文件的路径
    :param df: 要传入的 df
    :param logical_del_way: bool, default = Ture, 删除被替换内容的方式, 默认为逻辑删除
    :param con_log: 输出日期的 sql 连接
    :param tb_name_log: 输出日志的表名
    :return: 写入的df
    """

    # 实例化接口
    a_api = AlchemyAPI(con_w)

    # 统计: 删除前的数据
    row_count_tb_del_before, sum_tb_del_before = statistic_tb(con_w, tb_name_w, map_tb_w,
                                                              select_sql={map_tb_w.is_valid == 1}, field=sum_field,
                                                              func_prop='sum')

    # 逻辑删除 或 物理删除 被替换的内容
    if logical_del_way:
        del_res = a_api.logical_del(tb_name_w, map_tb_w, select_sql)
    else:
        del_res = a_api.physical_del(tb_name_w, map_tb_w, select_sql)

    # 统计: 被删除数据 & 删除后的数据
    row_count_del_res = del_res.shape[0]
    if sum_field in del_res.columns:  # 如果筛选结果为空, 那么求和会报错, 所以在此赋值为0
        sum_del_res = del_res[sum_field].sum()
    else:
        sum_del_res = 0
    row_count_tb_del_after, sum_tb_del_after = statistic_tb(con_w, tb_name_w, map_tb_w,
                                                            select_sql={map_tb_w.is_valid == 1}, field=sum_field,
                                                            func_prop='sum')

    # 打印统计结果
    ap.sound(notes=f'删除前: 行数 {row_count_tb_del_before}, 总和 {sum_tb_del_before}, '
                   f'被删除: 行数 {row_count_del_res}, 总和 {sum_del_res}, '
                   f'删除后: 行数 {row_count_tb_del_after}, 总和 {sum_tb_del_after}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)

    # 添加读取的数据
    df_add = add_sth_to_tb(con_w, tb_name_w, map_tb_w, sum_field, read_from=read_from, con_r=con_r, tb_name_r=tb_name_r,
                           path_r=path_r, df=df)

    ap.sound(notes=f'完成: replace_tb, add {tb_name_r} 或 {path_r} 或 df to {tb_name_w}',
             write_log=True, con_log=con_log, tb_name_log=tb_name_log, tb_name_w=tb_name_w)
    return df_add
