import backtrader as bt

class RSIGapStrategy(bt.Strategy):
    params = (
        ('rsi_period', 14),
        ('max_hold_days', 15),
        ('profit_target', 0.02),
        ('trailing_stop', 0.015),
    )

    def __init__(self):
        self.rsi_list = [bt.indicators.RSI(d.close, period=self.p.rsi_period) for d in self.datas]
        self.orders = [None] * len(self.datas)
        self.entry_prices = [None] * len(self.datas)
        self.entry_bars = [None] * len(self.datas)
        self.highest_prices = [None] * len(self.datas)
        self.trades = []  # 取引履歴を保存
        self.entry_attempts = [0] * len(self.datas)
        self.entry_successes = [0] * len(self.datas)
        self.debug_entries = [[] for _ in self.datas]

    def next(self):
        for i, data in enumerate(self.datas):
            if not self.getposition(data).size:
                if len(data) < 2:
                    continue
                gap = (data.open[0] - data.close[-1]) / data.close[-1]
                rsi_value = self.rsi_list[i][0]
                if gap < 0.10 and 15 < rsi_value < 60:
                    self.entry_attempts[i] += 1
                    size = int(self.broker.get_cash() / data.open[0] / len(self.datas))
                    if size > 0:
                        self.orders[i] = self.buy(data=data, size=size)
                        self.entry_prices[i] = data.open[0]
                        self.entry_bars[i] = len(self)
                        self.highest_prices[i] = data.open[0]
                        self.entry_successes[i] += 1
                        self.debug_entries[i].append(f"{data._name}: エントリー成功 (ギャップ: {gap:.3f}, RSI: {rsi_value:.1f})")
            else:
                price = data.close[0]
                if price > self.highest_prices[i]:
                    self.highest_prices[i] = price
                if price >= self.entry_prices[i] * (1 + self.p.profit_target):
                    self.close(data=data)
                    self.trades.append({
                        'symbol': data._name,
                        'entry_price': self.entry_prices[i],
                        'exit_price': price,
                        'return': (price - self.entry_prices[i]) / self.entry_prices[i] * 100,
                        'exit_reason': 'profit_target'
                    })
                elif price <= self.highest_prices[i] * (1 - self.p.trailing_stop):
                    self.close(data=data)
                    self.trades.append({
                        'symbol': data._name,
                        'entry_price': self.entry_prices[i],
                        'exit_price': price,
                        'return': (price - self.entry_prices[i]) / self.entry_prices[i] * 100,
                        'exit_reason': 'trailing_stop'
                    })
                elif len(self) - self.entry_bars[i] >= self.p.max_hold_days:
                    self.close(data=data)
                    self.trades.append({
                        'symbol': data._name,
                        'entry_price': self.entry_prices[i],
                        'exit_price': price,
                        'return': (price - self.entry_prices[i]) / self.entry_prices[i] * 100,
                        'exit_reason': 'max_hold_days'
                    }) 