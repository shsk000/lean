import backtrader as bt
import yfinance as yf
import pandas as pd
from datetime import datetime

class RSIGapStrategy(bt.Strategy):
    params = (
        ('rsi_period', 14),
        ('max_hold_days', 5),
        ('profit_target', 0.05),
        ('trailing_stop', 0.03),
    )

    def __init__(self):
        self.rsi = bt.indicators.RSI(self.data.close, period=self.p.rsi_period)
        self.order = None
        self.entry_price = None
        self.entry_bar = None
        self.highest_price = None

    def next(self):
        if not self.position:
            # ギャップ判定（前日終値と当日始値）
            if len(self.data) < 2:
                return
            gap = (self.data.open[0] - self.data.close[-1]) / self.data.close[-1]
            if gap < 0.02 and 30 < self.rsi[0] < 40:
                size = int(self.broker.get_cash() / self.data.open[0])
                if size > 0:
                    self.order = self.buy(size=size)
                    self.entry_price = self.data.open[0]
                    self.entry_bar = len(self)
                    self.highest_price = self.data.open[0]
        else:
            # トレーリングストップ・利確・最大保有日数
            price = self.data.close[0]
            if price > self.highest_price:
                self.highest_price = price
            if price >= self.entry_price * (1 + self.p.profit_target):
                self.close()
            elif price <= self.highest_price * (1 - self.p.trailing_stop):
                self.close()
            elif len(self) - self.entry_bar >= self.p.max_hold_days:
                self.close()

if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.addstrategy(RSIGapStrategy)

    # yfinanceでMSFTのデータ取得（auto_adjust=Falseを明示）
    df = yf.download('MSFT', start='2021-01-01', end='2023-01-01', auto_adjust=False)
    if df.empty:
        print('データが取得できませんでした。銘柄や期間を見直してください。')
        exit(1)
    df.dropna(inplace=True)
    # 必要なカラムだけ抽出し、カラム名を明示的にリネーム
    df = df[['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']]
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']

    data = bt.feeds.PandasData(dataname=df)
    cerebro.adddata(data)
    cerebro.broker.setcash(100000)
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.plot() 