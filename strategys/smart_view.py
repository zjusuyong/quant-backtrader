import argparse
import datetime
import glob
import os.path
from toolkit.database.configure.models import *
from toolkit import quant_package as qp

import backtrader as bt


class NetPayOutData(bt.feeds.GenericCSVData):
    lines = ('npy',)  # add a line containing the net payout yield
    params = dict(
        npy=6,  # npy field is in the 6th column (0 based index)
        dtformat='%Y-%m-%d',  # fix date format a yyyy-mm-dd
        timeframe=bt.TimeFrame.Months,  # fixed the timeframe
        openinterest=-1,  # -1 indicates there is no openinterest field
    )


class St(bt.Strategy):
    params = dict(
        selcperc=0.50,  # percentage of stocks to select from the universe
        rperiod=1,  # period for the returns calculation, default 1 period
        vperiod=36,  # lookback period for volatility - default 36 periods
        mperiod=12,  # lookback period for momentum - default 12 periods
        reserve=0.05  # 5% reserve capital
    )

    def log(self, arg):
        print('{} {}'.format(self.datetime.date(), arg))

    def __init__(self):

        self.selnum = int(len(self.datas) * self.p.selcperc)
        print(self.selnum)

        self.perctarget = (1.0 - self.p.reserve) / self.selnum

        rs = [bt.ind.PctChange(d, period=self.p.rperiod) for d in self.datas]
        vs = [bt.ind.StdDev(ret, period=self.p.vperiod) for ret in rs]
        ms = [bt.ind.ROC(d, period=self.p.mperiod) for d in self.datas]

        self.ranks = {d: d.Close * m / v for d, v, m in zip(self.datas, vs, ms)}

    def next(self):

        ranks = sorted(
            self.ranks.items(),  # get the (d, rank), pair
            key=lambda x: x[1][0],  # use rank (elem 1) and current time "0"
            reverse=True,  # highest ranked 1st ... please
        )

        rtop = dict(ranks[:self.selnum])
        rbot = dict(ranks[self.selnum:])

        posdata = [d for d, pos in self.getpositions().items() if pos]

        # do this first to issue sell orders and free cash
        for d in (d for d in posdata if d not in rtop):
            self.log('Leave {} - Rank {:.2f}'.format(d._name, rbot[d][0]))
            self.order_target_percent(d, target=0.0)

        # rebalance those already top ranked and still there
        for d in (d for d in posdata if d in rtop):
            self.log('Rebal {} - Rank {:.2f}'.format(d._name, rtop[d][0]))
            self.order_target_percent(d, target=self.perctarget)
            del rtop[d]  # remove it, to simplify next iteration

        # issue a target order for the newly top ranked stocks
        # do this last, as this will generate buy orders consuming cash
        for d in rtop:
            self.log('Enter {} - Rank {:.2f}'.format(d._name, rtop[d][0]))
            self.order_target_percent(d, target=self.perctarget)


def run():
    cerebro = bt.Cerebro()

    symbol_list = conn_cn_daily.table_names()[:4]
    print(symbol_list)
    for symbol in symbol_list:
        df = qp.get_btdata_from_sqldata(con=conn_cn_daily, symbol=symbol, fromdate='20180101', todate='20190101')
        cerebro.adddata(df, name=symbol)

    cerebro.broker.setcash(1000000.0)  # 设置启动资金
    cerebro.addstrategy(St)  # 添加策略
    cerebro.run()  # execute it all

    pnl = cerebro.broker.get_value()
    print('Profit ... or Loss: {:.2f}'.format(pnl))
    cerebro.plot()


if __name__ == '__main__':
    run()
