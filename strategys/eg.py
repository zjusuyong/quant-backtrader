import backtrader as bt
import numpy as np
from scipy import stats

from configure.config import *
import tushare as ts
from tookit.database.configure.models import *
from tookit import quant_package as qp
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt


class Momentum(bt.Indicator):
    lines = ('trend',)
    params = (('period', 90),)

    def __init__(self):
        self.addminperiod(self.params.period)

    def next(self):
        returns = np.log(self.data.get(size=self.p.period))
        x = np.arange(len(returns))
        slope, _, rvalue, _, _ = stats.linregress(x, returns)
        annualized = (1 + slope) ** 252
        self.lines.trend[0] = annualized * (rvalue ** 2)


class Strategy(bt.Strategy):
    def __init__(self):
        self.i = 0
        self.inds = {}
        self.spy = self.datas[0]
        self.stocks = self.datas[1:]

        self.spy_sma200 = bt.indicators.SimpleMovingAverage(self.spy.close, period=30)

        for d in self.stocks:
            self.inds[d] = {}
            self.inds[d]["momentum"] = Momentum(d.close, period=30)
            self.inds[d]["sma100"] = bt.indicators.SimpleMovingAverage(d.close, period=30)
            self.inds[d]["atr20"] = bt.indicators.ATR(d, period=20)
    def log(self, arg):
        print(f'{self.datetime.date()}, {arg}')

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

        elif order.status in [order.Canceled]:
            self.log('Order Canceled')
        elif order.status in [order.Margin]:
            self.log('Order Margin')
        elif order.status in [order.Rejected]:
            self.log('Order Rejected')

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def prenext(self):
        # call next() even when data is not available for all tickers
        self.next()

    def next(self):
        if self.i % 5 == 0:
            self.rebalance_portfolio()
        if self.i % 10 == 0:
            self.rebalance_positions()
        self.i += 1

    def rebalance_portfolio(self):
        # only look at data that we can have indicators for
        self.rankings = list(self.stocks)

        # self.rankings = list(filter(lambda d: len(d) > 100, self.stocks))
        self.rankings.sort(key=lambda d: self.inds[d]["momentum"][0])
        num_stocks = len(self.rankings)

        # sell stocks based on criteria
        for i, d in enumerate(self.rankings):
            if self.getposition(d).size:

                if i > num_stocks / 2 or d.close[0] < self.inds[d]["sma100"]:
                    self.close(d)

        if self.spy < self.spy_sma200:
            return

        # buy stocks with remaining cash
        # for i, d in enumerate(self.rankings[:int(num_stocks * 0.2)]):
        for i, d in enumerate(self.rankings[:2]):
            cash = self.broker.get_cash()
            value = self.broker.get_value()
            if cash <= 0:
                break
            if not self.getposition(self.data).size:
                size = value * 0.001 / self.inds[d]["atr20"]
                self.buy(d, size=size)

    def rebalance_positions(self):
        num_stocks = len(self.rankings)

        if self.spy < self.spy_sma200:
            return

        # rebalance all stocks
        for i, d in enumerate(self.rankings[:int(num_stocks * 0.2)]):
            cash = self.broker.get_cash()
            value = self.broker.get_value()
            if cash <= 0:
                break
            size = value * 0.001 / self.inds[d]["atr20"]
            print(size)
            self.order_target_size(d, size)


cerebro = bt.Cerebro(stdstats=False)
cerebro.broker.set_coc(True)

ts.pro_api(TushareToken)
df = ts.pro_bar(ts_code='000001.SH',
                start_date='20180101',
                end_date='20200101',
                adj='qfq', asset='I', freq='D')
df['datetime'] = df['trade_date'].map(lambda x: dt.datetime.strptime(str(x), '%Y%m%d'))
df = df.rename(columns={'vol': 'volume'}).set_index('datetime').sort_index()
df = bt.feeds.PandasData(dataname=df, fromdate=dt.datetime(2018, 1, 1), todate=dt.datetime(2020, 1, 1))

cerebro.adddata(df)  # add S&P 500 Index

symbol_list = conn_cn_daily.table_names()[:4]
print(symbol_list)
for symbol in symbol_list:
    df = qp.get_btdata_from_sqldata(con=conn_cn_daily, symbol=symbol, fromdate='20180101', todate='20200101')
    cerebro.adddata(df, name=symbol)

cerebro.addobserver(bt.observers.Value)
cerebro.addanalyzer(bt.analyzers.SharpeRatio, riskfreerate=0.0)
cerebro.addanalyzer(bt.analyzers.Returns)
cerebro.addanalyzer(bt.analyzers.DrawDown)
cerebro.addstrategy(Strategy)
cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')

results = cerebro.run()
strat = results[0]
pyfoliozer = strat.analyzers.getbyname('pyfolio')
returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
# df = pd.DataFrame()
# df['returns'] = returns
# df['positions'] = positions
# print(returns)
df = pd.DataFrame()
df['rr'] = returns
df['ee'] = (1 + returns).cumprod()
df['rr'].plot()
plt.show()

df['ee'].plot()

plt.show()


cerebro.plot(iplot=False)[0][0]

print(f"Sharpe: {results[0].analyzers.sharperatio.get_analysis()['sharperatio']}")

print(f"Norm. Annual Return: {results[0].analyzers.returns.get_analysis()['rnorm100']}%")
print(f"Max Drawdown: {results[0].analyzers.drawdown.get_analysis()['max']['drawdown']}%")
