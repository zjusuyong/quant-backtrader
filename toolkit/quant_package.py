from toolkit.analysis_package import ap
import backtrader as bt
import datetime as dt
import pandas as pd
import numpy as np
import math


def maxdrawdown(arr):
    """
    计算最大回撤值
    :param arr: 收益率np数组
    :return: dict: {'ratio': 最大回撤比率, 'range': [开始位置, 结束位置]}
    """
    i = np.argmax((np.maximum.accumulate(arr) - arr) / np.maximum.accumulate(arr))
    j = np.argmax(arr[:i])
    return {'ratio': (1 - arr[i] / arr[j]), 'range': [j, i]}


def maxrevenuegrowth(arr):
    """
    计算最大收益
    :param arr: 收益率np数组
    :return: dict: {'ratio': 最大收益比率, 'range': [开始位置, 结束位置]}
    """
    i = np.argmax((arr - np.minimum.accumulate(arr)) / np.minimum.accumulate(arr))
    j = np.argmin(arr[:i])
    return {'ratio': (arr[i] / arr[j] - 1), 'range': [j, i]}


def volatility(l_each_pct, year_tradeday, kline_num):
    """
    计算波动率
    :param l_each_pct: list, 每一Kline的pct_change
    :param year_tradeday: 每年交易天数
    :param kline_num: 测试数据总Kline数量
    :return:
    """
    series_diff = l_each_pct - np.mean(l_each_pct)
    # temp = series_diff.map(lambda x: math.pow(x, 2)).sum()
    temp = np.sum(series_diff ** 2)
    return np.sqrt(temp * year_tradeday / (kline_num - 1))


def information_ratio(l_each_strategy_pct, l_each_benchmark_pct, strategy_ytd_pct, benchmark_ytd_pct, year_tradeday):
    """
    计算信息比率
    :param l_each_strategy_pct: list, 策略每一Kline的pcg_change
    :param l_each_benchmark_pct: list, 基准每一Kline的pcg_change
    :param strategy_ytd_pct: 策略年华收益率
    :param benchmark_ytd_pct: 基准年华收益率
    :param year_tradeday: 每年交易天数
    :return:
    """
    year_proyield_std = np.sqrt(year_tradeday) * np.std(np.array(l_each_strategy_pct) - np.array(l_each_benchmark_pct))
    return (strategy_ytd_pct - benchmark_ytd_pct) / year_proyield_std


def sharpe_ratio(eac_stgy_rt, eac_bcmk_rt, rf, annualization):
    """
    计算夏普比率
    :param eac_stgy_rt: series, 策略每日收益率
    :param eac_bcmk_rt: series, 基准每日收益率
    :param rf: 无风险利率
    :param annualization: 年化天数
    :return:
    """
    diff = np.array(eac_stgy_rt) - np.array(eac_bcmk_rt)
    return np.mean(diff) - rf / np.std(diff, ddof=1) * np.sqrt(annualization)


def beta_ratio(l_each_strategy_pct, l_each_benchmark_pct):
    """
    计算贝塔值
    :param l_each_strategy_pct: list, 策略每一Kline的pcg_change
    :param l_each_benchmark_pct: list, 基准每一Kline的pcg_change
    :return:
    """
    cov = np.mean(l_each_strategy_pct * l_each_benchmark_pct) - np.mean(l_each_strategy_pct) * np.mean(
        l_each_benchmark_pct)
    return cov / np.var(l_each_benchmark_pct)


def alpha_ratio(strategy_ytd_pct, benchmark_ytd_pct, rf, beta):
    """
    计算alpha
    :param strategy_ytd_pct: 策略年化收益率
    :param benchmark_ytd_pct: 基准年华收益率
    :param rf: 无风险利率
    :param beta: beta
    :return:
    """
    return strategy_ytd_pct - rf - beta * (benchmark_ytd_pct - rf)


def annualized(total_returns, year_tradeday, kline_num):
    """
    计算年华收益率
    :param total_returns: 总收益
    :param year_tradeday: 每年交易天数
    :param kline_num: 测试数据总Kline数量
    :return:
    """
    return math.pow(1 + total_returns, year_tradeday / kline_num) - 1


def get_signal(l_signal_open, l_signal_flat):
    """
    获取非同源信号，process:
        将平仓0/1信号替换为10/21信号
        错列相减得到signal，其中1为开始，-11为结束
        重制索引，筛选出1和-11，得到index列表，每两个一组生成l_holdidx
        生成全零列表，根据l_holdidx里边的索引将全零列表对应位置换成1，得到信号列表
    :param l_signal_open: DataFrame.Series, 开仓0/1信号列表
    :param l_signal_flat: DataFrame.Series, 平仓0/1信号列表
    :return: array, 0/1信号列表
    """
    df = pd.DataFrame()
    df['open'] = l_signal_open
    df['flat'] = l_signal_flat
    df['flat'] = df['flat'].map(lambda x: 21 if x == 1 else 10)
    df['diff'] = df['open'] - df['flat']
    df['diff_shift'] = df['diff'].shift(1).fillna(-10).astype(int)
    df['signal'] = df['diff'] - df['diff_shift']

    print(df)
    # todo: 这里发现开始并不只是1，也有12等，结束不只是-11，也有-12等，需要用别的办法处理

    df = df.reset_index()
    df_temp = df[['signal']][(df['signal'] == 1) | (df['signal'] == -11)].reset_index().copy()

    if df_temp.loc[len(df_temp) - 1, 'signal'] == 1:
        df_temp = ap.concat(df_temp, pd.DataFrame({'index': [df_temp['index'].tolist()[-1] + 1], 'signal': [-11]}))
    l_aux = df_temp['index'].tolist()
    step = 2
    l_holdidx = [l_aux[i:i + step] for i in range(0, len(l_aux), step)]

    l_signal = np.zeros(len(df['signal']))

    print(df_temp)
    print(l_holdidx)

    for (m, n) in l_holdidx:
        num = n - m
        l_fill = [1] * num
        l_signal[m: n] = l_fill

    return l_signal


def get_holdidx(l_signal):
    """
    获取持仓索引列表l_holdidx
        l_aux eg: [1, 3, 4, 6, 7, 8], 两两一组，表示开仓和平仓的索引，第一笔交易开仓1，平仓3，第二笔交易开仓4，平仓6...
        l_holdidx: [[4, 6], [8, 20], [45, 47]], 每一个元素表示一次交易，内层列表表示每一次交易开始和结束的索引
    process：
        l_signal是信号布尔列表->aux，1表示满足开仓条件，持有仓位，0表示不满足开仓条件，没有仓位
        l_signal平移一位相减，得到的列表只包括1/0/-1三种情况，1为开仓，-1为平仓，
        存在一种特殊情况，最后一次开仓后，在测试数据范围内没有满足平仓条件，需要判断后补上一个-1表示平仓
        信号布尔列表aux只保留1和-1值后，得到的就是开平仓的列表，取其索引得到l_aux
        通过l_aux获取到l_holdidx
    :return:
    """
    df_temp = pd.DataFrame()
    df_temp['sig'] = l_signal
    df_temp['sig_shift'] = l_signal
    df_temp['sig_shift'] = df_temp['sig_shift'].shift(1).fillna(0).astype(int)
    df_temp['aux'] = df_temp['sig'] - df_temp['sig_shift']
    df_temp = df_temp.reset_index()
    df_aux = df_temp[['aux']][df_temp['aux'] != 0].reset_index().copy()

    if df_aux.loc[len(df_aux) - 1, 'aux'] == 1:
        df_aux = ap.concat(df_aux,
                           pd.DataFrame({'index': [df_aux['index'].tolist()[-1] + 1], 'aux': [-1]}))
    l_aux = df_aux['index'].tolist()
    step = 2
    l_holdidx = [l_aux[i:i + step] for i in range(0, len(l_aux), step)]
    return l_holdidx


def trade_process(begin_price, end_price, vol, direction):
    """
    计算每个kline的初始金额, 利润, 结束金额
    :param begin_price: 初始价格
        开仓: 执行价格
        持仓: 上个kline收盘价
        平仓: 上个kline收盘价
    :param end_price: 结束价格
        开仓: 本kline收盘价
        持仓: 本kline收盘价
        平仓: 执行价格
    :param vol: 持仓数量
    :param direction: 交易方向, 'short' or 'long'
    :return: 利润金额, 上个kline结束金额, 本kline结束金额
    """
    begin_tur = vol * begin_price
    profit = vol * (begin_price - end_price) if direction == 'short' else vol * (end_price - begin_price)
    end_tur = begin_tur + profit
    return profit, begin_tur, end_tur


def get_btdata_from_sqldata(con, symbol, fromdate, todate):
    """
    从数据库获取一个品种的数据，做成BackTrader数据格式
    :param con: 数据库连接
    :param symbol: str, 要读取的表名
    :param fromdate: str, 开始日期, eg: '20200102'
    :param todate: str, 结束日期, eg: '20200202'
    return: BackTrader数据格式的df
    """
    fromdate = dt.datetime.strptime(fromdate, '%Y%m%d')
    todate = dt.datetime.strptime(todate, '%Y%m%d')

    df = pd.read_sql(con=con, sql=symbol)
    df = df.rename(columns={'vol': 'volume'}).set_index('datetime').sort_index()
    df = bt.feeds.PandasData(dataname=df, fromdate=fromdate, todate=todate)
    return df
