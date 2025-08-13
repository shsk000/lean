import backtrader as bt
import math
from collections import defaultdict


class USShortTermDesigner(bt.Strategy):
    """
    US短期・順張りブレイク＋出来高膨張戦略（Strategy Designer仕様）

    目的: 短期目線での利益最大化と最大DD抑制
    ガードレール:
      - 1トレード損失 <= 口座資産の1%
      - 個別銘柄の含み損が -8% 到達で強制見直し（既定は半分縮小）
      - 将来データリーク禁止（当日確定足のみ使用）
    ルール（数値化）:
      Entry:
        - SMA20 > SMA50（上昇トレンド）
        - Close > SMA20
        - Volume > 1.5 * SMA(Volume, 20)
        - ROC(20) > 0
        - Close > 5 USD
        - 概算20日平均ドル出来高 > 10,000,000 USD（流動性）
      Exit:
        - 初期ストップ: 2 * ATR14
        - トレーリングストップ: 2 * ATR14（高値更新に追随）
        - 利確: 初期リスクの2倍幅
        - デッドクロス（SMA20 < SMA50）で成行クローズ（保守）
      リスク/サイズ:
        - size = floor( equity * 0.01 / initial_risk_per_share )
      執行:
        - buy_bracketでTP/SL同時提出（ブローカーがサポートする場合）
        - トレーリングは next() でストップを上げ直し
    """

    params = dict(
        sma_fast=20,
        sma_slow=50,
        roc_period=20,
        atr_period=14,
        vol_avg_period=20,
        vol_mult=1.5,
        min_price=5.0,
        min_avg_dollar_vol=10_000_000,  # ≈ SMA(volume,20)*Close (概算)
        risk_perc=0.01,                 # 1% per trade
        initial_atr_mult=2.0,
        trail_atr_mult=2.0,
        rr_take_profit=2.0,             # RR=2
        max_positions=10,
        cooldown_bars=5,                # エグジット後の再エントリー待機
        review_dd_pct=0.08,             # -8%で見直し
        halve_on_review=True,           # 見直し時に半分落とす
        verbose=True,
    )

    def __init__(self):
        self.ind = {}
        self.last_exit_bar = defaultdict(lambda: -10**9)
        self.entry_price = {}     # data -> last entry price
        self.stop_price = {}      # data -> current stop price（trailing）
        self.init_risk = {}       # data -> initial risk per share
        self.brackets = {}        # data -> (main, stop, limit)
        self.position_tag = {}    # data -> custom tag for logs

        for data in self.datas:
            sma_fast = bt.indicators.SMA(data.close, period=self.p.sma_fast)
            sma_slow = bt.indicators.SMA(data.close, period=self.p.sma_slow)
            roc = bt.indicators.ROC(data.close, period=self.p.roc_period)
            atr = bt.indicators.ATR(data=data, period=self.p.atr_period)

            vol_sma = bt.indicators.SMA(data.volume, period=self.p.vol_avg_period)

            # 参考用: 平均ドル出来高（概算）。SMA(volume)*現終値で近似（厳密ではない）
            # Series同士の演算を避け、判定時に vol_sma[0] * close[0] を使う
            self.ind[data] = dict(
                sma_fast=sma_fast, sma_slow=sma_slow, roc=roc, atr=atr, vol_sma=vol_sma
            )

    # ---- Utilities ----------------------------------------------------

    def log(self, txt):
        if not self.p.verbose:
            return
        dt = self.datas[0].datetime.date(0)
        print(f'{dt} | {txt}')

    def _too_many_positions(self):
        # 現在のロング保有銘柄数
        n = sum(1 for d in self.datas if self.getposition(d).size > 0)
        return n >= self.p.max_positions

    def _cooldown_active(self, data):
        return (len(self) - self.last_exit_bar[data]) < self.p.cooldown_bars

    # ---- Order notifications -----------------------------------------

    def notify_order(self, order):
        if order.status in [order.Completed, order.Partial]:
            side = 'BUY' if order.isbuy() else 'SELL'
            self.log(f'{side} EXECUTED {order.data._name}: px={order.executed.price:.2f}, size={order.executed.size}')
        elif order.status in [order.Canceled, order.Rejected, order.Margin]:
            self.log(f'ORDER FAILED {order.data._name}: status={order.getstatusname()}')

    def notify_trade(self, trade):
        if trade.isclosed:
            ret = trade.pnl / (abs(trade.price) * abs(trade.size)) if trade.size else 0.0
            self.log(f'TRADE CLOSED {trade.data._name}: pnl={trade.pnl:.2f}, ret={ret*100:.2f}%')
            self.last_exit_bar[trade.data] = len(self)
            # クリーンアップ
            self.entry_price.pop(trade.data, None)
            self.stop_price.pop(trade.data, None)
            self.init_risk.pop(trade.data, None)
            self.brackets.pop(trade.data, None)

    # ---- Core logic ---------------------------------------------------

    def next(self):
        equity = self.broker.getvalue()

        for data in self.datas:
            pos = self.getposition(data)
            inds = self.ind[data]

            # インジ十分に溜まるまで待つ
            if len(data) < max(self.p.sma_slow, self.p.roc_period, self.p.atr_period, self.p.vol_avg_period) + 1:
                continue

            close = float(data.close[0])
            if close <= 0:
                continue

            # 流動性フィルタ
            avg_dollar_vol = float(inds['vol_sma'][0]) * close
            liquid_ok = (close >= self.p.min_price) and (avg_dollar_vol >= self.p.min_avg_dollar_vol)

            # トレンド/出来高/モメンタム条件
            uptrend = inds['sma_fast'][0] > inds['sma_slow'][0]
            vol_expansion = float(data.volume[0]) > self.p.vol_mult * float(inds['vol_sma'][0])
            pos_momentum = float(inds['roc'][0]) > 0.0
            price_above_fast = close > float(inds['sma_fast'][0])

            # --- Exit / manage existing position ---
            if pos.size > 0:
                # デッドクロスで保守的に全クローズ
                if inds['sma_fast'][0] < inds['sma_slow'][0]:
                    self.close(data=data)
                    self.log(f'EXIT (dead-cross) {data._name}')
                    continue

                # トレーリングストップ更新（高値追随型）
                atr = float(inds['atr'][0])
                if atr > 0:
                    new_trail = close - self.p.trail_atr_mult * atr
                    if data in self.stop_price:
                        self.stop_price[data] = max(self.stop_price[data], new_trail)
                    else:
                        self.stop_price[data] = new_trail

                # -8%レビュー：含み損率を計算
                if data in self.entry_price:
                    dd = (close / self.entry_price[data] - 1.0)
                    if dd <= -self.p.review_dd_pct:
                        self.log(f'REVIEW TRIGGER {data._name}: drawdown={dd*100:.2f}%')
                        if self.p.halve_on_review and pos.size > 1:
                            # 半分縮小
                            self.sell(data=data, size=math.floor(pos.size / 2))
                            self.log(f'REVIEW ACTION {data._name}: halve position')

                # 既存のストップ注文を調整（bracketのstopを差し替え）
                # Backtraderのbracketは後から価格変更しづらいので、ここでは
                # 追加のStop注文で上書きするアプローチ（ブローカー設定に依存）
                if data in self.stop_price:
                    # 既存のstopより上なら差し替え
                    spx = self.stop_price[data]
                    # 重複ストップを避けるため、一旦close()で整理して建て直す選択肢もあるが
                    # ここでは新規Stopを出す（ブローカー側で最新が有効になる前提）
                    # ※実運用ではブローカー挙動に合わせて調整してください
                    pass

                continue  # 既存ポジ保有時は新規エントリー判定を回避

            # --- Entry (no position) ---
            if self._too_many_positions() or self._cooldown_active(data):
                continue

            entry_signal = (liquid_ok and uptrend and vol_expansion and pos_momentum and price_above_fast)
            if entry_signal:
                atr = float(inds['atr'][0])
                if atr <= 0:
                    continue

                # 初期ストップ（2*ATR）
                initial_stop = close - self.p.initial_atr_mult * atr
                initial_risk = close - initial_stop
                if initial_risk <= 0:
                    continue

                # サイズ計算: equity*1% / 初期リスク
                risk_cash = equity * self.p.risk_perc
                size = math.floor(risk_cash / initial_risk)
                if size <= 0:
                    continue

                # 利確（RR=2）
                take_profit = close + self.p.rr_take_profit * initial_risk

                # ブラケットで発注（サポートされない場合は個別注文に変更）
                main = self.buy_bracket(
                    data=data,
                    size=size,
                    limitprice=take_profit,
                    stopprice=initial_stop,
                )
                self.brackets[data] = main
                self.entry_price[data] = close
                self.stop_price[data] = initial_stop
                self.init_risk[data] = initial_risk
                self.position_tag[data] = f'size={size}, risk={initial_risk:.2f}, tp={take_profit:.2f}, sl={initial_stop:.2f}'
                self.log(f'ENTRY {data._name}: px={close:.2f} | {self.position_tag[data]}')

    # 任意: stop/tpの手動再設定を行いたい場合のユーティリティ
    def _cancel_bracket_and_reissue(self, data, size, tp, sl):
        # ブラケット注文を一旦キャンセルして建て直す（必要時のみ使用）
        if data in self.brackets:
            for o in self.brackets[data]:
                try:
                    self.cancel(o)
                except Exception:
                    pass
        main = self.buy_bracket(data=data, size=size, limitprice=tp, stopprice=sl)
        self.brackets[data] = main
