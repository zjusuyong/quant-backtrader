# -*- coding: UTF-8 -*-

from configure.all_config import *
import datetime as dt
import decimal
import pandas as pd
import numpy as np
import traceback
# import wxpy
import re
import os
import shutil
import calendar


class AP:
    def __init__(self, send_wechat=False):
        self.start_time = dt.datetime.now()
        self.set_option()
        self.col_sort_cost = []
        self.col_sort_withdrawal = []

        # 二进制的全角字符, 用于在 datacheck 中检查全角字符
        self.l_illegal_sign = '[\u0020\uff08\u0020\uff09\u0020\u2018\u0020\u2019\u0020' \
                              '\u201c\u0020\u201d\u0020\uff1a\u0020\uff1b\u0020\u300a' \
                              '\u0020\u300b\u0020\u3001\u0020\u002d\u0020\u2014\u2014' \
                              '\u0020\u3010\u0020\u3011\u0020\uff0c\u0020\u3002]'

        # if send_wechat:
        #     self.bot = wxpy.Bot(cache_path=True)
        #     self.bot.enable_puid(path='wxpy_puid.pkl')

    @staticmethod
    def set_option():
        pd.set_option('display.max_rows', 10000)
        pd.set_option('display.max_columns', 10000)
        pd.set_option('display.width', 10000)

    @staticmethod
    def add_colname_suffix(list_input, to_lower=False):
        """
        将列表中的重复元素加自增序列后缀
        process：
            都转换为小写
            构造default_dict
            生成频次字典: eg: defaultdict(<class 'list'>, {1: [0], 2: [1, 3], 3: [2], 4: [4]})
            循环赋值自增后缀: eg: defaultdict(<class 'list'>, {1: [0], 2: ['1_1', '3_2'], 3: [2], 4: [4]})
            循环default_dict.keys(): eg: [1, 2, 3, 4]
                如果值为单一元素列表则直接插入到 list_output 原位置
                如果不是单一元素列表, 循环列表:
                    逐个添加自增序列后缀, 插入到 list_output 原位置
        :param list_input: 传入的列表
        :param to_lower: 是否将所有字符串都转为小写后, 再识别是否有重复的, 因为数据库不识别大小写
        :return: list_output, 每个元素都是str
        """
        from collections import defaultdict

        if to_lower:
            list_input = [str(temp).lower() for temp in list_input]

        list_output = []
        default_dict = defaultdict(list)
        for keys, values in [(temp, i) for i, temp in enumerate(list_input)]:
            default_dict[keys].append(values)

        for keys in default_dict.keys():
            list_keys = default_dict.get(keys)
            if len(list_keys) == 1:
                list_output.insert(list_keys[0], list_input[list_keys[0]])
            else:
                for j, jtemp in enumerate(list_keys):
                    list_output.insert(list_keys[j], f'{list_input[jtemp]}_{j + 1}')
        return list_output

    @staticmethod
    def build_double_range_dict(start_start, start_end, start_step, end_statr, end_end, end_step,
                                decimals=4, l_update=None):
        """
        创建双值区间字典
             样例: {'[0.0, 0.005]': [0.0, 0.005]}
             解析: 字典的值是一个包含两个元素的列表, 分别是区间的开始和结束
             过程:
                  创建区间开始列表和区间结束列表, 并保留小数位数
                  利用开始区间列表和结束区间列表创建字典键列表和字典值列表
                  用字典键列表和字典值列表创建字典
        :param start_start: 开始区间列表起始值
        :param start_end: 开始区间列表结束值
        :param start_step: 开始区间列表步进
        :param end_statr: 结束区间列表起始值
        :param end_end: 结束区间列表结束值
        :param end_step: 结束区间列表步进
        :param decimals: 保留小数位数
        :param l_update: 要单独添加的范围区间列表, 列表的每个元素中有两个值,
            例如: [[-1000, 0], [10, 1000]], 即新增加-1000->0区间和10->1000区间
        :return:
        """
        l_start = np.arange(start_start, start_end, start_step)
        l_end = np.arange(end_statr, end_end, end_step)

        if l_update:
            for i in l_update:
                np.append(l_start, i[0])
                np.append(l_end, i[1])

        l_start = np.around(l_start, decimals=decimals)
        l_end = np.around(l_end, decimals=decimals)

        l_dict_key = [str(j) for j in [list(i) for i in zip(l_start, l_end)]]
        l_dict_value = [list(i) for i in zip(l_start, l_end)]

        dict_double_range = dict(zip(l_dict_key, l_dict_value))

        return dict_double_range

    @staticmethod
    def build_double_range_dict_easy(start, end, step, decimals=4, l_extreme_val=None):
        """
        创建双值区间字典
             样例: {'[0.0, 0.005]': [0.0, 0.005]}
             解析: 字典的值是一个包含两个元素的列表, 分别是区间的开始和结束
             过程:
                  创建区间开始列表和区间结束列表, 并保留小数位数
                  利用开始区间列表和结束区间列表创建字典键列表和字典值列表
                  用字典键列表和字典值列表创建字典
        :param start: 列表起始值
        :param end: 列表结束值
        :param step: 步进
        :param decimals: 保留小数位数
        :param l_extreme_val: 由极大值和极小值组成的列表, 添加到双值字典的两端
            eg: [-1000, 1000], 即新增加 -1000 -> start 区间和 end -> 1000 区间
        :return:
        """

        list_range = [round(i, decimals) for i in np.arange(start, end + step, step)]

        if l_extreme_val:
            list_range.extend(l_extreme_val)
            list_range.sort()

        l_start = list_range[:-1]
        l_end = list_range[1:]

        l_dict_key = [str(j) for j in [list(i) for i in zip(l_start, l_end)]]
        l_dict_value = [list(i) for i in zip(l_start, l_end)]

        dict_double_range = dict(zip(l_dict_key, l_dict_value))

        return dict_double_range

    @staticmethod
    def build_single_range_dict(start, end, step, decimals=2):
        """
        创建单值字典
            eg: start=1.5, end=3.5, step=0.5, {1.5: '1.5-2.0', 2.0: '2.0-2.5', 2.5: '2.5-3.0'}
            end + step * 2: 因为 arange 不包括 end, 而且 i < len(lists) - 1, 所以字段会少两个step, 在这里补齐
        :param start: 序列开始值
        :param end: 序列结束值
        :param step: 步进
        :param decimals: 保留小数位数
        :return: 单值字典
        """
        dicts = {}
        lists = [round(i, decimals) for i in np.arange(start, end + step * 3, step)]

        for i, x in enumerate(lists):
            if i < len(lists) - 1:
                dicts.update({x: f'{x}-{lists[i + 1]}'})

        return dicts

    @staticmethod
    def concat(*args, axis=0, reset_index=1):  # 连接几个表，axis=0合并行，axis=1合并列，reset_index=1清理为df格式
        frame = list(args)
        df = pd.concat(frame, axis=axis, sort=False)
        if reset_index == 1:
            df = df.reset_index(drop=True)
        return df

    @staticmethod
    def concat_first(*args, reset_index=1):
        """
        连接几个表
        axis=0 为上下连接, axis只能=0, 因为给定join_axes为列名, 所以不支持左右连接
        """
        frame = list(args)
        df = pd.concat(frame, axis=0, join_axes=[args[0].columns])
        if reset_index == 1:
            df = df.reset_index(drop=True).copy()
        return df

    @staticmethod
    def cosine_similarity(vec_1, vec_2):
        """
        计算余弦距离
        :param vec_1:
        :param vec_2:
        :return:
        """
        num = vec_1.dot(vec_2.T)
        denom = np.linalg.norm(vec_1) * np.linalg.norm(vec_2)
        return num / denom

    @staticmethod
    def data_w(path, **kwargs):
        """
        写入数据
        格式：sheet名字（没有引号）= df,
        eg: ap.ap.data_w(path_w_temp, ths=dfs, df_mom=df_moms)
        :param path:
        :param kwargs:
        :return:
        """
        writer = pd.ExcelWriter(path)
        df_w_list = list(kwargs.values())
        sheet_w_list = list(kwargs.keys())
        for i in range(len(sheet_w_list)):
            kwargs.get(sheet_w_list[i]).to_excel(writer, sheet_w_list[i])
        writer.save()
        return df_w_list

    @staticmethod
    def data_w_septb(path, l_sheet, l_df):  # 分表函数的过程函数，应用于sep_tb()
        writer = pd.ExcelWriter(path)
        for i in range(len(l_sheet)):
            l_df[i].to_excel(writer, l_sheet[i])
        writer.save()
        return l_sheet

    @staticmethod
    def dict_assign(df, dicts, new_col, old_col):  # 从字典中查找值，并赋值
        df[new_col] = df[old_col].map(dicts)
        return df[new_col]

    @staticmethod
    def dict_col(df, new_col, old_col, df_dictfrom, select_col, values_col, fillna=0):  # 组合了get_dict()和dict_assign()函数
        dicts = dict(df_dictfrom.groupby(select_col).apply(lambda x: list(x[values_col])[0]))
        df[new_col] = df[old_col].map(dicts)
        df[new_col] = df[new_col].fillna(fillna)
        return df[new_col]

    @staticmethod
    def drop_duplicates(df, col, keeps='first'):  # 去重,并重构index
        dfs = df.drop_duplicates(subset=col, keep=keeps, inplace=False)
        dfs = dfs.reset_index(drop=True)
        return dfs

    @staticmethod
    def dst_day(year, month, week, weekday, is_dst_algorithm=True):
        """
        根据给出的年份, 月份, 第几个礼拜几, 计算出对应的日期, 可用于计算夏令时
            注意: 如果 is_dst_algorithm=True, 那么当传入week=-1的时候, 实际上是在找最后一周的周几, 有可能往后找到下个月
        :param year: int, 年份, eg: 2019
        :param month: int, 月份, eg: 3
        :param week: int, 第几个, eg: 4, 注: 如果这个值很大, 那么实际上就是例如: 2020年8月的第20个周一, 那么会一直数周一到后边的月份
        :param weekday: int, 周几, 周一最小, 对应0, 周日最大, 对应6
        :param is_dst_algorithm: bool, 是否用夏令时算法, 区别在于, 比如计算某个月的最后一个周五, 正常情况应该是返回当月最后一个周五,
            但是夏令时算法实际上是返回当月最后一周的那个周五, 也就是有可能得到下个月的第一个周五
        :return: datetime, 日期
        """
        assert week != 0, 'week cannot be equal to zero.'

        if week > 0:
            month_firstday = dt.datetime(year=year, month=month, day=1)
            month_firstday_weekday = month_firstday.weekday()

            if month_firstday_weekday <= weekday:
                delta = weekday - month_firstday_weekday
            else:
                delta = 6 - month_firstday_weekday + weekday + 1

            dst_day = month_firstday + dt.timedelta(days=delta + (week - 1) * 7)
        else:
            month_lastday = dt.datetime(year=year, month=month, day=calendar.monthrange(year, month)[1])
            month_lastday_weekday = month_lastday.weekday()

            if not is_dst_algorithm:
                if weekday <= month_lastday_weekday:
                    delta = month_lastday_weekday - weekday
                else:
                    delta = 6 - weekday + month_lastday_weekday + 1

                dst_day = month_lastday - dt.timedelta(days=delta + (week + 1) * 7)

            else:
                if weekday <= month_lastday_weekday:
                    delta = month_lastday_weekday - weekday
                    dst_day = month_lastday - dt.timedelta(days=delta + (week + 1) * 7)
                else:
                    if week != -1:
                        delta = 6 - weekday + month_lastday_weekday + 1
                        dst_day = month_lastday - dt.timedelta(days=delta + (week + 1) * 7)
                    else:
                        delta = weekday - month_lastday_weekday
                        dst_day = month_lastday + dt.timedelta(days=delta)
        return dst_day

    def dst_judge(self, xdate, s_month, s_week, s_weekday, e_month, e_week, e_weekday, is_dst_algorithm=True):
        """
        根据传入的开始和结束时间点, 判断给定日期是否在区间内, 中间调用dst_day()函数, 可以计算是否是夏令时
        :param xdate: datetime, 需要判断的日期
        :param s_month: int, 开始月份
        :param s_week: int, 开始日期阈值是这个月的第几个
        :param s_weekday: int, 开始日期阈值是星期几
        :param e_month: int, 结束月份
        :param e_week: int, 结束日期阈值是这个月的第几个
        :param e_weekday: int, 结束日期阈值是星期几
        :param is_dst_algorithm: bool, 是否用夏令时算法, 区别在于, 比如计算某个月的最后一个周五, 正常情况应该是返回当月最后一个周五,
            但是夏令时算法实际上是返回当月最后一周的那个周五, 也就是有可能得到下个月的第一个周五
        :return: bool, 给定日期是否在目标日期区间内
        """
        s_dst_day = self.dst_day(year=xdate.year, month=s_month, week=s_week, weekday=s_weekday,
                                 is_dst_algorithm=is_dst_algorithm)
        e_dst_day = self.dst_day(year=xdate.year, month=e_month, week=e_week, weekday=e_weekday,
                                 is_dst_algorithm=is_dst_algorithm)
        if s_dst_day <= xdate <= e_dst_day:
            isdst = 1
        else:
            isdst = 0
        return isdst

    def file_copy_to_dirs(self, path_r, path_copy_to):
        """
        将一个文件夹下所有文件拷贝到另一个文件夹下
        os.walk(path_r)返回三个值，分别是:
            root -> 当前目录路径
            dirs -> 当前路径下所有子目录
            files -> 当前路径下所有非目录子文件
        :param path_r: 要读取的文件夹
        :param path_copy_to: 要写入的文件夹
        :return: True
        """
        if not os.path.exists(path_r):
            print(f'{path_r} is not exists')

        if not os.path.exists(path_copy_to):
            os.makedirs(path_copy_to)
            print(f'{path_copy_to} is not exists, creat it')

        self.sound(notes=f'entry: copy')
        for root, dirs, files in os.walk(path_r):
            if files:
                for file in files:
                    path_ths = os.path.join(root, file)
                    shutil.copy(path_ths, path_copy_to)

        self.sound(notes=f'finish')
        return self

    @staticmethod
    def from_sql_to_df_datetime_or_str_to_data(df, data_type='RES'):
        """
        用于将从数据库导出数据的日期列转换为 data 格式
        :param df: 从数据库导出的数据
        :param data_type: str, default: 'RES', 传入数据的内容
            'RES' -> results, 业绩表
            'HR' -> 人事表
            'COST' -> 成本表
            'WD' -> withdrawal, 提现表
        :return: 修改了日期格式的数据
        """
        if data_type == 'RES':
            # 要清洗的列
            col_name_list = ['出借日期', '到账日期', '计息日期', '到期日期', '兑付日期', '提前赎回日期', '投资月份', '首次投资月份',
                             'AUX计息日', '初始到期日', 'AUX到期日', 'AUX兑付日', '本次到期日', '本次兑付日', '本次兑付月份']
        elif data_type == 'HR':
            col_name_list = ['入职时间', '离职日期', '实际转正日期', '月份', '调转居间时间', '居间协议签订日期', '居间协议结束日期',
                             'entry_date', 'leave_date', 'AUX转正日期']
        elif data_type == 'COST':
            col_name_list = ['月份']

        # TODO: 添加其他 data_type
        else:
            raise Exception('data_type 设置有误')

        # 如果是 pd.Timestamp 类型, 则直接转换为 date 类型, 如果是 str 类型, 则检测是否为日期内容, 如果是则转换为 date
        def sth_to_date(x):
            if isinstance(x, pd.Timestamp):
                x = dt.date(x.year, x.month, x.day)

            # 注意: 这里的elif和下一个elif不能交换位置, 因为如果调换, 那么x=='0'的话, 就不会进入本条件了
            elif x == 0 or x == '0' or x == '-':
                x = dt.date(1900, 1, 1)

            elif isinstance(x, str):
                # str_datetime = re.match(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})", x)
                str_datetime = re.match(r"(\d{4}-\d{1,2}-\d{1,2})", x)
                if str_datetime is not None:
                    # datetimes = dt.datetime.strptime(str_datetime.group(), '%Y-%m-%d %H:%M:%S')
                    datetimes = dt.datetime.strptime(str_datetime.group(), '%Y-%m-%d')
                    x = dt.date(datetimes.year, datetimes.month, datetimes.day)
                else:
                    x = x
            else:
                x = x
            return x

        # 如果字段名称存在, 则进行数据类型转换
        for col in col_name_list:
            if col in df.columns.values:
                df[col] = df[col].map(sth_to_date)
        return df

    @staticmethod
    def from_sql_to_df_str_to_int_or_float(df, data_type='RES'):
        """
        用来给从数据库导出的 df 转换格式, 字符串转换为整型或浮点
        :param df: 从 sql 导出需要转换格式的 df
        :param data_type: str, default: 'RES', 传入数据的内容
            'RES' -> results, 业绩表
            'HR' -> 人事表
            'COST' -> 成本表
            'WD' -> withdrawal, 提现表
        :return: 转换好格式的 df
        """
        if data_type == 'RES':
            # 需要转换格式的字段
            to_int_or_float_col = []
        elif data_type == 'HR':
            to_int_or_float_col = []
        elif data_type == 'COST':
            to_int_or_float_col = []
        # TODO: 添加其他 data_type
        else:
            raise Exception('data_type 设置有误')

        def str_to_int_or_float(content, formats='0.0000000', strs='E-7'):
            """
            此方法应用于 lambda 内部, 例如: for col in to_int_or_float_col:
                                            df[col] = df[col].map(lambda x: ap.ap.str_to_int_or_float(x))
            从数据库查询数据后, 数据格式总会有问题, 这个函数是把每一个值进行判断并转换
                如果在数据库中是 double 类型, 那么在 Python 中是 decimal.Decimal 类型, 先转换为 str
                然后判断如果是 str
                    如果是l_strs中的科学记数法字符:
                        赋值为0
                    如果都是数字, 或者 (最前边是'-', 并且去掉'-'全是数字, 并且'-'只有1个):
                        转换为int类型
                    如果(只包含一个'.', 并且去掉'.'全是数字)或者(只包含一个'.', 并且只包含一个'-', 并且以'-'开头, 并且去掉'-'和'.'全是数字)
                        转换为float类型
            :param content: 传入的内容
            :param formats: str, default: '0.0000000', 如果是 decimal.Decimal 格式, 要转换的精度, 默认为7位小数
            :param strs: str, default: '0E-7', 将此内容替换为 0, 此内容与 formats 是对应的, formats 默认为7位小数, 所以 strs 默认为 '0E-7'
            :return: 转换类型后的内容
            """
            if isinstance(content, decimal.Decimal):
                content = str(decimal.Decimal(content).quantize(decimal.Decimal(formats)))
            if isinstance(content, str):
                if strs in content:
                    content = 0
                elif content.isdigit() \
                        or (content.startswith('-')
                            and content.count("-") == 1
                            and content.replace("-", '').isdigit()):
                    content = int(content)
                elif (content.count(".") == 1 and content.replace(".", '').isdigit()) \
                        or (content.count(".") == 1
                            and content.count("-") == 1
                            and content.startswith('-')
                            and content.replace("-", '').replace(".", '').isdigit()):
                    content = float(content)
            return content

        # 循环需要转换格式的字段, 如果字段存在, 进行格式转换
        for col in to_int_or_float_col:
            if col in df.columns.values:
                df[col] = df[col].map(lambda x: str_to_int_or_float(x))
        return df

    def format_sql_to_df(self, df, data_type='RES'):
        """
        --> 转换数据格式
            --> 转换日期字段格式
            --> 转换数字字段格式
        --> 调整字段顺序
            --> 获取字段列表
            --> 循环标准字段, 如果在本表字段则加入到排序字段中
            --> 调整字段顺序
        :param df: 传入的表格
        :param data_type: str, default: 'RES', 传入数据的内容
            'RES' -> results, 业绩表
            'HR' -> 人事表
            'COST' -> 成本表
            'WD' -> withdrawal, 提现表
        :return: 转换之后的表格
        """

        # 转换日期字段和其他字段格式
        df = self.from_sql_to_df_datetime_or_str_to_data(df, data_type=data_type)
        df = self.from_sql_to_df_str_to_int_or_float(df, data_type=data_type)

        # 调整字段顺序
        l_df_col = list(df.columns.values)
        l_sort_col = []

        if data_type == 'RES':
            col_sort = []
        elif data_type == 'HR':
            col_sort = []
        elif data_type == 'COST':
            col_sort = COST_ASCEND_DIM_COL

        # TODO: 添加其他 data_type
        else:
            raise Exception('data_type 设置有误')

        for col in col_sort:
            if col in l_df_col:
                l_sort_col.append(col)
        df = df[l_sort_col]
        return df

    @staticmethod
    def format_sth_to_str(df, data_type='NONE', list_format_col=None):
        """
        将字段内容转换成字符串
        :param df: 传入的表
        :param data_type: 表类型, 输入的类型不同, 备选字段不同, 如果为'NONE', 那么就调用传入的list_format_col
        :param list_format_col: 需要转换为字符串的字段列表
        :return:
        """
        if data_type == 'RES':
            col_name_list = LIST_CNST_STH_TO_STR
        elif data_type == 'NONE':
            col_name_list = list_format_col
        else:
            col_name_list = []

        for col in col_name_list:
            if col in df.columns.values:
                df[col] = df[col].map(lambda x: str(x))
        return df

    def get_dcr_ndx(self, df, list_field):
        """
        title: 创建 list_field中各字段的笛卡尔乘积表, 可以接收任何字段列表, 一维多维都可以
        process:
            判断 list_field 是否是列表:
                False:
                    将字段去重
                    构造表格
                True:
                    循环 list_field:
                        逐个字段去重, 并添加到 l_l_col
                    生成笛卡尔乘积, 并逐个添加到 df_dcr
        :param df: 传入的数据
        :param list_field: 字段列表或单一字段, list / str, eg: 'name', 或['name', 'age']
        :return: df_dcr
        """
        self.sound(notes=f'entry: get_dcr_ndx')
        import itertools
        df_dcr = pd.DataFrame()
        if not isinstance(list_field, list):
            l_field = list(set(df[list_field].tolist()))
            df_dcr[list_field] = l_field
        else:
            l_l_col = []
            for col_name in list_field:
                l_col = list(set(df[col_name].tolist()))
                l_l_col.append(l_col)
                print(f'{col_name} 长度: {len(l_col)}')
            print(f'l_l_col 维度: {len(l_l_col)}')

            for i, t in enumerate(itertools.product(*l_l_col)):
                for j in range(len(list_field)):
                    df_dcr.loc[i, list_field[j]] = t[j]
        return df_dcr

    @staticmethod
    def get_default_dict(lists):
        """
        返回列表元素出现次数的字典
        res eg: defaultdict(<class 'list'>, {1: [0], 2: [1, 3], 3: [2], 4: [4]})
            input: default_dict.get(2)
            output: [1, 3]
        :param lists:
        :return:
        """
        from collections import defaultdict
        default_dict = defaultdict(list)
        for keys, values in [(temp, i) for i, temp in enumerate(lists)]:
            default_dict[keys].append(values)
        return default_dict

    @staticmethod
    def get_dict(df, select_col, values_col):  # 将两列组合成字典
        dicts = dict(df.groupby(select_col).apply(lambda x: list(x[values_col])[0]))
        return dicts

    @staticmethod
    def log_sql(notes, con, tb_name_log, tb_name_w):

        df_log = pd.DataFrame()
        res = traceback.extract_stack()

        # 获取调用列表, 去掉第一个, 倒序
        l_caller = [res[i][2] for i in range(len(res))][1:][::-1]

        # 逐个赋值
        for j in range(len(l_caller)):
            df_log.loc[0, f'func: {j}'] = l_caller[j]

        df_log['notes'] = notes
        df_log['tb_name_w'] = tb_name_w
        df_log.to_sql(con=con, name=tb_name_log, if_exists='append', index=False)

        return df_log

    @staticmethod
    def matching_factor(df, dicts, new_col, old_col, types='df', compare_type='int'):
        """
        根据字典匹配系数，字典的格式：第一个阀值是0，最后一个阀值无限大的数
        :param df:
        :param dicts:
            dict = {'0': '0% - 50%',
                    '0.5': '50% - 100%',
                    '100': '100% +'}
        :param new_col:
        :param old_col:
        :param types:
        :param compare_type: int / float / time, 需要将字典的键转化为int / float / time 类型
        :return:
        """

        def outs(dicty):  # 通过闭包传递参数
            def ins(x):
                lists = list(dicty.keys())
                for i in range(1, len(lists)):  # 字典第一个阀值是0，从字典第二个开始
                    if compare_type == 'int':
                        if int(lists[i - 1]) <= x < int(lists[i]):  # 根据所在区间匹配系数
                            x = dicty.get(lists[i - 1])
                            break
                    elif compare_type == 'float':
                        if float(lists[i - 1]) <= x < float(lists[i]):  # 根据所在区间匹配系数
                            x = dicty.get(lists[i - 1])
                            break
                    elif compare_type == 'time':
                        if dt.datetime.strptime(lists[i - 1], '%H:%M:%S').time() <= x < dt.datetime.strptime(
                                lists[i], '%H:%M:%S').time():
                            x = dicty.get(lists[i - 1])
                            break
                return x

            return ins

        if types == 'table':  # 如果传入透视表
            df = df.reset_index()  # 将此前得到的透视表转换索引，变成普通的df格式
        factor = outs(dicts)  # 给前边的闭包传入字典
        df[new_col] = df[old_col].map(factor)  # 根据指定列匹配系数到新的列
        return df[new_col]

    @staticmethod
    def matching_factor_a_b(df, dicts, new_col, old_col, types='df'):  # 根据字典匹配系数, 字典的值是2个元素列表, 分别是最大和最小阀值
        def outs(dicty):  # 通过闭包传递参数
            def ins(x):
                lists = list(dicty.keys())

                for i in lists:  # 字典第一个阀值是0，从字典第二个开始
                    if dicty.get(i)[0] <= x < dicty.get(i)[1]:  # 根据所在区间匹配系数
                        x = i
                        break
                return x

            return ins

        if types == 'table':  # 如果传入透视表
            df = df.reset_index()  # 将此前得到的透视表转换索引，变成普通的df格式
        factor = outs(dicts)  # 给前边的闭包传入字典
        df[new_col] = df[old_col].map(factor)  # 根据指定列匹配系数到新的列
        return df[new_col]

    @staticmethod
    def mapping_section(n, dicts, is_left_open_right_close=True):
        """
        区间匹配, 用于将给定的一个数, 按照区间赋值
        process:
            初始化v为None
            获取传入区间段个数
            循环区间段个数
                将传入的n逐个比较, 左开右闭, 将同索引的值列表赋值给v
            存在n超过区间最大值的情况, 所以如果循环之后仍然没有赋值, 则将默认值赋给v

        eg:
            dicts = {'section': [0, 5, 11, 21, 31],
                     'map_val': [0, 1, 2, 3, 4],
                     'map_err': 'error'}

        :param n: 给定的数字
        :param dicts: 映射赋值字典
        :param is_left_open_right_close: bool, default: True, 是否左开右闭
        :return: v
        """
        v = None
        length = len(dicts['section']) - 1

        for i in range(length):
            if is_left_open_right_close:
                if dicts['section'][i] <= n < dicts['section'][i + 1]:
                    v = dicts['map_val'][i]
            else:
                if dicts['section'][i] < n <= dicts['section'][i + 1]:
                    v = dicts['map_val'][i]

        if v is None:
            v = dicts['map_err']
            print(f'Error: map_val is not in dicts')

        return v

    @staticmethod
    def module_to_df(res):
        """
        用于将 sqlalchemy 查询语句 query 得到的结果转换为 DataFrame 格式, 过程:
            --> 判断 res 是否为空, 如果为空, 直接建立空 df
            --> 获取所有字段
                --> 传入查询结果 res
                --> 循环 res, 得到每一行的对象 element
                --> 循环 element 的每个属性, 得到 porp 列表
                --> 由于 porp 列表是重复每个 element 的所有 porp, 而实际需要一个即可, 所以切片[0]
                --> 由于 porp 中包含多余的 _sa_instance_state 属性, 所以切片[1:], 得到 porp_list
            --> 转换为 DataFrame
                --> 嵌套循环 res 和 porp_list, 通过 getattr() 方法得到所有内容
        :param res: query 得到的查询结果
        :return: 转换得到的 df
        """
        if res:
            porp_list = [[porp for porp in element.__dict__] for element in res][0][1:]
            df = pd.DataFrame([[getattr(elem, porps, f'错误: 没有 {porps} 属性') for porps in porp_list] for elem in res],
                              columns=porp_list)
        else:
            df = pd.DataFrame()
        return df

    @staticmethod
    def normalized_list(lists):
        """
        将一个列表做归一化处理
        :param lists:
        :return:
        """
        list_normalized = [(i - min(lists)) / (max(lists) - min(lists))
                           if max(lists) - min(lists) != 0 else 0 for i in lists]
        return list_normalized

    def pt_col(self, df_w, new_col, old_col, df_to_pt, index_col, values_col, aggfunc=np.sum):
        tb_pt = pd.pivot_table(df_to_pt, index=index_col, values=values_col, aggfunc=aggfunc)
        df_pt = tb_pt.reset_index()
        df_w[new_col] = self.dict_col(df_w, new_col, old_col, df_pt, index_col, values_col)
        return df_w[new_col]

    def read_sql(self, con, tb_name_r, chunksize=None):
        """
        读取数据库, 支持分批次读取
        :param con: 数据库连接
        :param tb_name_r: 读取的表
        :param chunksize: 每次读取的行数
        :return:
        """
        self.sound(notes=f'entry: read_sql')
        df_source = pd.read_sql(con=con, sql=tb_name_r, chunksize=chunksize)
        if not chunksize:
            self.sound(notes=f'finish: read_sql\ndf_source.shape: {df_source.shape}')
        return df_source

    @staticmethod
    def regular_replacement(parameter):  # 将小写字母替换成大写字母
        parm = parameter.group()
        return str(parm).upper()

    @staticmethod
    def regular_prefix(strs):
        prefix = f'(?<={strs}).+'
        compile_prefix = re.compile(prefix)
        return compile_prefix

    @staticmethod
    def regular_suffix(strs):
        suffix = f'.+(?={strs})'
        compile_suffix = re.compile(suffix)
        return compile_suffix

    @staticmethod
    def select_df(df, *args):  # 可以多条件筛选df的函数 格式：fun(df, ('机构', '!=', '南京分公司'), ('第二个列', '判断符号', 筛选条件)...
        for i in range(len(args)):
            if args[i][1] == '==':
                df = df[df[args[i][0]] == args[i][2]]
            elif args[i][1] == '!=':
                df = df[df[args[i][0]] != args[i][2]]
            elif args[i][1] == '>':
                df = df[df[args[i][0]] > args[i][2]]
            elif args[i][1] == '<':
                df = df[df[args[i][0]] < args[i][2]]
            elif args[i][1] == '>=':
                df = df[df[args[i][0]] >= args[i][2]]
            elif args[i][1] == '<=':
                df = df[df[args[i][0]] <= args[i][2]]
            df = df.copy().reset_index(drop=True)
        return df

    def select_df_merge(self, df_source, select_col, compare='==', reset_index=0, *args):
        # 从一个表中的一个字段筛选不同内容，然后再合并在一起
        # 例: dfx = ap.ap.select_df_merge(df, 'a', 1, 7, compare='==', reset_index=0)

        df = pd.DataFrame(columns=df_source.columns)
        for i in range(len(args)):
            df_args = self.select_df(df_source, (select_col, compare, args[i]))
            df = self.concat(df, df_args, reset_index=reset_index)
        return df

    @staticmethod
    def select_df_from_list(df, l_select_and):
        """
        从数据源中多次筛选'且'关系的内容, 并上下组合在一起
        :param df: 数据源
        :param l_select_and: list, '且'筛选条件列表, 例如:
            [['机构', '==', 'XXXX'], ['年龄', '>=', 30]]
            列表中的每一项都是且的关系
        :return: df
        """
        for select in l_select_and:
            if select[1] == '==':
                df = df[df[select[0]] == select[2]]
            elif select[1] == '!=':
                df = df[df[select[0]] != select[2]]
            elif select[1] == '>':
                df = df[df[select[0]] > select[2]]
            elif select[1] == '<':
                df = df[df[select[0]] < select[2]]
            elif select[1] == '>=':
                df = df[df[select[0]] >= select[2]]
            elif select[1] == '<=':
                df = df[df[select[0]] <= select[2]]
            df = df.copy().reset_index(drop=True)
        return df

    def select_df_dim3(self, df, list_dim3):
        """
        title: 通过传入的三维列表, 从df中筛选数据, dim3是或的关系, dim2是且的关系, dim1是比较的三个内容
        process:
            循环dim3:
                创建新的dim2, 使其等于df
                循环dim2:
                    按照dim1筛选数据
                    复制并重置索引
                将每一个dim2合并成 df_select
        :param df: 数据源
        :param list_dim3: 用于筛选的三维列表, eg: [[['a', '==', 'xxx'], ['b', '>=', 30]], ['a', '==', 'xxx']]
        :return: df
        """
        df_select = pd.DataFrame(columns=df.columns.tolist())
        for dim2 in list_dim3:
            df_dim2 = df.copy()
            for dim1 in dim2:
                if dim1[1] == '==':
                    df_dim2 = df_dim2[df_dim2[dim1[0]] == dim1[2]]
                elif dim1[1] == '!=':
                    df_dim2 = df_dim2[df_dim2[dim1[0]] != dim1[2]]
                elif dim1[1] == '>':
                    df_dim2 = df_dim2[df_dim2[dim1[0]] > dim1[2]]
                elif dim1[1] == '<':
                    df_dim2 = df_dim2[df_dim2[dim1[0]] < dim1[2]]
                elif dim1[1] == '>=':
                    df_dim2 = df_dim2[df_dim2[dim1[0]] >= dim1[2]]
                elif dim1[1] == '<=':
                    df_dim2 = df_dim2[df_dim2[dim1[0]] <= dim1[2]]

                df_dim2 = df_dim2.copy().reset_index(drop=True)
            df_select = self.concat_first(df_select, df_dim2)
        return df_select

    def select_df_concat(self, df_source, l_select_or, reset_index=0):
        """
        从数据源中多次筛选'且'或者'或'关系的内容, 并上下组合在一起
        :param df_source: 数据源
        :param l_select_or: list, dim: 3, '或'筛选条件列表, 例如:
            [
             [['机构', '==', 'XXXX'], ['年龄', '>=', 30]],
             [['项目', '!=', 'XXXX']]
             ]
            即三维列表中的每一个项都是或的关系, 而最低维度列表中的每一项都是且的关系
        :param reset_index: default: 0, 合并的维度, 0 为上下合并, 1 为左右合并
        :return: df
        """
        df = pd.DataFrame(columns=df_source.columns)
        for select_or in l_select_or:
            df_args = self.select_df_from_list(df_source, select_or)
            df = self.concat(df, df_args, reset_index=reset_index)
        return df

    @staticmethod
    def select_dim3(tuple_dim3):
        """
        用于向 a_api.query() 传递筛选内容, 支持多重and和or组合
        note:
            join_and: ['getattr(map_tb, XX) > XX', 'getattr(map_tb, XX) > XX']
            str_and: 'and_('getattr(map_tb, XX) > XX', 'getattr(map_tb, XX) > XX')'
            join_or: ['and_('getattr(map_tb, XX) > XX', 'getattr(map_tb, XX) > XX'),
                      'and_('getattr(map_tb, XX) > XX', 'getattr(map_tb, XX) > XX')]
            str_or: 'or_(and_('getattr(map_tb, XX) > XX', 'getattr(map_tb, XX) > XX'),
                     and_('getattr(map_tb, XX) > XX', 'getattr(map_tb, XX) > XX')'
        process:
            初始化join_or为空列表
            循环dim3:
                每次将join_and初始化为空列表
                循环dim2:
                    将比较内容添加到join_and, 要区分被比较项list_dim1[2]是否是字符串
                将join_and转换为str_and
                将str_and添加到join_or
            将join_or转换为str_or
        调用方式:
            需要用eval()解析
            select_sql_base = eval(select_dim3(tuple_dim3))
            df = a_api.query(conn_kl_dmps, map_tb, select_sql_base)
        :param tuple_dim3:
            eg: [[['field_1', '==', 'XXXX'], ['field_2', '>=', 30]], [['field_3', '!=', 'XXXX']]]
                最外层是or, 倒数第二层是and, 最内层是三个比较元素, 其中中间一个是比较符号
        :return: str_or
        """
        join_or = []
        for list_dim2 in tuple_dim3:
            join_and = []
            for list_dim1 in list_dim2:
                if isinstance(list_dim1[2], str):
                    join_and.append(f'getattr(map_tb, "{list_dim1[0]}") {list_dim1[1]} "{list_dim1[2]}"')
                else:
                    join_and.append(f'getattr(map_tb, "{list_dim1[0]}") {list_dim1[1]} {list_dim1[2]}')
            str_and = f'and_({", ".join(join_and)})'
            join_or.append(str_and)
        str_or = f'or_({", ".join(join_or)})'

        print(f'select_dim3: {str_or}')
        return str_or

    @staticmethod
    def select_time(df, time_col, start_time, end_time, type_datetime=1):  # 筛选指定时期的数据, start_time是元祖方式传入日期
        if type_datetime == 1:
            start_time = dt.datetime(start_time[0], start_time[1], start_time[2])  # 将开始时间参数转换为datetime格式
            end_time = dt.datetime(end_time[0], end_time[1], end_time[2])  # 将结束时间参数转换为datetime格式
            df = df[(df[time_col] >= start_time) & (df[time_col] < end_time)]  # 筛选指定日期的数据
        else:
            start_time = dt.date(start_time[0], start_time[1], start_time[2])  # 将开始时间参数转换为datetime格式
            end_time = dt.date(end_time[0], end_time[1], end_time[2])  # 将结束时间参数转换为datetime格式
            df = df[(df[time_col] >= start_time) & (df[time_col] < end_time)]  # 筛选指定日期的数据
        df = df.reset_index()
        return df

    def sep_tb(self, df, sep_col, sum_col, path, sums=True, *args):  # 分表函数，sep_col需要分表的列名，*args需要转换成字符串的表名
        df = self.to_str(df, *args)  # 将长数字转换为字符

        l_data = df[sep_col].tolist()  # 将需要分表的列转换为list
        l_data = list(set(l_data))  # 将list去重
        l_data.sort()  # 排序

        sume = 0
        l_sheet = []
        l_df = []
        for i in range(len(l_data)):
            if isinstance(l_data[i], dt.datetime):
                sheet = dt.datetime.strftime(l_data[i], '%y-%m-%d')  # 如果需要分表的内容为日期格式，则转换为字符
            else:
                sheet = l_data[i]
            l_sheet.append(sheet)
            l_df.append(df[df[sep_col] == l_data[i]])
            if sums is True:
                sume += l_df[i][sum_col].sum()
        self.data_w_septb(path, l_sheet, l_df)
        print(f'数据验证: {sume}')
        return l_data

    @staticmethod
    def slope(lists):
        """
        计算一个列表中每个点的变化率
        :param lists: 传入的列表
        :return:
        """
        return [(lists[i] - lists[i + 1]) / max(lists[i], lists[i + 1]) for i in range(len(lists) - 1)]

    def sound(self, notes='', send_wechat=False, write_log=False, con_log=None, tb_name_log=None,
              tb_name_w=None):  # 蜂鸣提示+运行时间
        end_timey = dt.datetime.now()
        running_time = (end_timey - self.start_time).seconds

        length = len(notes)
        print(f'\n+--{"-" * length}+\n| {notes} |')
        print(f'+--{"-" * length}+{"-" * (65 - length)}> '
              f'CurrentTime: {dt.datetime.now()}, '
              f'RunTime: {int(running_time / 60)} minutes, {running_time % 60} seconds')

        # winsound.Beep(700, 500)
        # winsound.Beep(400, 500)
        # winsound.Beep(500, 250)
        # winsound.Beep(600, 250)
        # winsound.Beep(700, 250)

        if write_log:
            self.log_sql(notes, con_log, tb_name_log, tb_name_w)

        if send_wechat:
            self.bot.file_helper.send(f"Program's start-time: {dt.datetime.now()}")
            self.bot.file_helper.send(notes)

        return running_time

    @staticmethod
    def split_col_to_add_row(df, split_col, split_symbol):
        """
        将某个字段按组成元素拆分成很多列, 然后再添加行
        :param df: 传入表
        :param split_col: 要拆分的字段, str
        :param split_symbol: 分割符号, str
        :return:
        """
        df_lift = df.drop(split_col, axis=1)
        df_right = df[split_col].str.split(split_symbol, expand=True).stack().reset_index(level=1, drop=True).rename(
            split_col)
        df = df_lift.join(df_right)
        # useing pd.merge
        # df = pd.merge(df_lift, df_right, left_on=df_lift.index, right_on=df_right.index)
        return df

    @staticmethod
    def sql_add_aux_col(con, tb_name_w):
        with con.connect() as conn:
            conn.execute(f'ALTER TABLE `{tb_name_w}` ADD `id_sql` INT(20) UNSIGNED NOT NULL AUTO_INCREMENT, '
                         f'ADD PRIMARY KEY (`id_sql`);')
            conn.execute(f'ALTER TABLE `{tb_name_w}` ADD `createtime` datetime DEFAULT CURRENT_TIMESTAMP;')
            conn.execute(f'ALTER TABLE `{tb_name_w}` ADD `updatetime` datetime '
                         f'DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;')

    @staticmethod
    def sth_to_datetime(x):
        if isinstance(x, pd.Timestamp):
            x = dt.datetime(x.year, x.month, x.day, x.hour, x.minute, x.second)
        elif pd.isnull(x):
            x = dt.datetime(1900, 1, 1)
        elif isinstance(x, int):
            if 1000000000 <= x <= 9999999999:
                timestamp = pd.Timestamp(x, unit='s')
                x = dt.datetime(
                    timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second)
            else:
                x = x
        elif isinstance(x, str):
            str_datetime = re.match(r"(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})", x)
            if str_datetime is not None:
                datetimes = dt.datetime.strptime(str_datetime.group(), '%Y-%m-%d %H:%M:%S')
                x = dt.datetime(
                    datetimes.year, datetimes.month, datetimes.day, datetimes.hour, datetimes.minute, datetimes.second)
            else:
                x = x
        else:
            x = x
        return x

    @staticmethod
    def str_to_date(strs):
        match_res_date = re.match(r'\d{4}-\d{1,2}-\d{1,2}', strs)
        if match_res_date:
            res_datetime = dt.datetime.strptime(match_res_date.group(), '%Y-%m-%d')
            res_date = dt.date(res_datetime.year, res_datetime.month, res_datetime.day)
            return res_date

        match_res_datetime = re.match(r'\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}', strs)
        if match_res_datetime:
            res_datetime = dt.datetime.strptime(match_res_datetime.group(), '%Y-%m-%d %H:%M:%S')
            res_date = dt.date(res_datetime.year, res_datetime.month, res_datetime.day)
            return res_date

    @staticmethod
    def to_str(df, *args):
        for i in range(len(args)):
            df[args[i]] = df[args[i]].map(lambda x: str(x))
        return df

    def to_sql(self, df, con, tb_name_w, if_exists='fail', drop_aux=False, dtypedict=None, chunksize=None):
        if drop_aux:
            list_df_col = df.columns.tolist()
            list_aux_col = ['id_sql', 'createtime', 'updatetime']
            for col in list_aux_col:
                if col in list_df_col:
                    df = df.drop(columns=col)

        self.sound(notes=f'entry: to_sql')
        if dtypedict:
            df.to_sql(con=con, name=tb_name_w, if_exists=if_exists, index=False, dtype=dtypedict, chunksize=chunksize)
        else:
            df.to_sql(con=con, name=tb_name_w, if_exists=if_exists, index=False, chunksize=chunksize)

        self.sound(notes=f'entry: sql_add_aux_col')
        self.sql_add_aux_col(con=con, tb_name_w=tb_name_w)

        self.sound(notes=f'finish')
        return self


ap = AP()
# ap_sw = AP(send_wechat=True)
