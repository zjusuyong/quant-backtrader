# 常用方法

获取当前第几行: len(self)

获取当前日期: self.datas[1].datetime.date(0)

设置每笔交易交易的股票数量: cerebro.addsizer(bt.sizers.FixedSize, stake=10)

把构建的数据集添加到回测数据: cerebro.adddata(data)

添加策略: cerebro.addstrategy(MyStrategy)

设置初始资金: cerebro.broker.setcash(100.0)

设置佣金比例: cerebro.broker.setcommission(commission=0.0)

# 属性

## Strategy

notify_order(self, order): 下的单子，order的任何状态变化都将引起这一方法的调用

notify_trade(self, trade): 任何一笔交易头寸的改变都将调用这一方法

notify_cashvalue(cash, value): 任何现金和资产组合的变化都将调用这一方法 

notify_store(msg, *args, **kwargs): 可以结合cerebro类进行自定义方法的调用

## trade
ref: 唯一id

size (int): trade的当前头寸

price (float): trade资产的当前价格

value (float): trade的当前价值

commission (float): trade的累计手续费

pnl (float): trade的当前pnl

pnlcomm (float): trade的当前pnl减去手续费

isclosed (bool): 当前时刻trade头寸是否归零

isopen (bool): 新的交易更新了trade

justopened (bool): 新开头寸

dtopen (float): trade open的datetime

dtclose (float): trade close的datetime

## order

order.status: 可以返回order的当前状态, 包括: 
* Created: 被创建
* Submitted: 被发送到broker
* Accepted: 被提交到系统等待成交
* Partial: 部分成交
* Complete: 已完成
* Rejected: 被拒绝
* Margin: 被删除，因为需要追加保证金
* Cancelled: 被用户取消
* Expired: 已过期

order.isbuy: 可以获得这笔order是否是buy

order.executed.price: 获得执行order的价格

order.executed.value: 获得执行order的总价

order.executed.comm: 获得执行order的手续费

# 获取统计数据

获取一个策略中单品种的每个kline的收益率、现金余额、持仓金额等

    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    results = cerebro.run()
    strat = results[0]
    pyfoliozer = strat.analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()