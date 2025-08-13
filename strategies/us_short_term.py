import backtrader as bt
import math
from collections import defaultdict

class USShortTermDesignerDiag(bt.Strategy):
    params = dict(
        sma_fast=20, sma_slow=50, roc_period=20, atr_period=14,
        vol_avg_period=20, vol_mult=1.5,
        min_price=5.0,
        min_avg_dollar_vol=10_000_000,   # 前日 SMA(volume)*Close の概算
        risk_perc=0.01,                  # 1%/トレード
        portfolio_risk_cap=0.03,         # 同時オープンリスク合計<=3%
        initial_atr_mult=2.0, trail_atr_mult=2.0,
        rr_take_profit=2.0,
        max_positions=10,
        adv_cap_pct=0.10,                # 平均出来高の10%上限
        gap_filter_pct=0.015,            # ±1.5%ギャップで見送り
        cooldown_bars=5,
        review_dd_pct=0.08,
        halve_on_review=True,
        use_coc=True,                    # ← 前日判定→翌日寄り約定
        verbose=True,
    )

    def __init__(self):
        self.ind = {}
        self.entry_price, self.stop_price, self.init_risk = {}, {}, {}
        self.last_exit_bar = defaultdict(lambda: -10**9)
        self.skip = defaultdict(int)  # スキップ理由カウンタ

        for d in self.datas:
            self.ind[d] = dict(
                sma_fast=bt.ind.SMA(d.close, period=self.p.sma_fast),
                sma_slow=bt.ind.SMA(d.close, period=self.p.sma_slow),
                roc=bt.ind.ROC(d.close, period=self.p.roc_period),
                atr=bt.ind.ATR(d, period=self.p.atr_period),
                vol_sma=bt.ind.SMA(d.volume, period=self.p.vol_avg_period),
            )

    # ---- utils ----
    def log(self, msg):
        if self.p.verbose:
            dt = self.datas[0].datetime.date(0)
            print(f'{dt} | {msg}')

    def _open_risk_usd(self):
        risk = 0.0
        for d in self.datas:
            pos = self.getposition(d)
            if pos.size > 0 and d in self.stop_price:
                px = float(d.close[0])
                per_share = max(0.0, px - self.stop_price[d])
                risk += per_share * pos.size
        return risk

    def _too_many_positions(self):
        return sum(1 for d in self.datas if self.getposition(d).size > 0) >= self.p.max_positions

    def _cooldown(self, d):
        return (len(self) - self.last_exit_bar[d]) < self.p.cooldown_bars

    # ---- notifications ----
    def notify_order(self, order):
        if order.status in [order.Completed, order.Partial]:
            side = 'BUY' if order.isbuy() else 'SELL'
            self.log(f'{side} EXEC {order.data._name}: px={order.executed.price:.2f} size={order.executed.size}')
        elif order.status in [order.Canceled, order.Rejected, order.Margin]:
            self.log(f'ORDER {order.getstatusname()} {order.data._name}')

    def notify_trade(self, trade):
        if trade.isclosed:
            ret = trade.pnl / (abs(trade.price) * abs(trade.size)) if trade.size else 0.0
            self.log(f'TRADE CLOSED {trade.data._name}: pnl={trade.pnl:.2f} ret={ret*100:.2f}%')
            self.last_exit_bar[trade.data] = len(self)
            for dct in (self.entry_price, self.stop_price, self.init_risk):
                dct.pop(trade.data, None)

    # ---- core ----
    def next(self):
        idx = -1 if self.p.use_coc else 0    # 前日判定→翌日寄り
        equity = self.broker.getvalue()
        open_risk_cap_usd = equity * self.p.portfolio_risk_cap
        open_risk_now = self._open_risk_usd()

        for d in self.datas:
            inds = self.ind[d]
            if len(d) < max(self.p.sma_slow, self.p.roc_period, self.p.atr_period, self.p.vol_avg_period) + 2:
                self.skip['warmup'] += 1
                continue

            pos = self.getposition(d)

            close_0 = float(d.close[0])
            open_0  = float(d.open[0])
            close_p = float(d.close[idx])
            sma_fast_p = float(inds['sma_fast'][idx])
            sma_slow_p = float(inds['sma_slow'][idx])
            roc_p = float(inds['roc'][idx])
            vol_p = float(d.volume[idx])
            vol_sma_p = float(inds['vol_sma'][idx])
            atr_0 = float(inds['atr'][0])

            # === manage existing ===
            if pos.size > 0:
                if sma_fast_p < sma_slow_p:
                    self.close(data=d); self.log(f'EXIT dead-cross {d._name}')
                    continue
                if atr_0 > 0:
                    new_trail = close_0 - self.p.trail_atr_mult * atr_0
                    self.stop_price[d] = max(self.stop_price.get(d, new_trail), new_trail)
                # -8% review（任意）
                if d in self.entry_price:
                    dd = (close_0 / self.entry_price[d] - 1.0)
                    if dd <= -self.p.review_dd_pct and self.p.halve_on_review and pos.size > 1:
                        self.sell(data=d, size=pos.size // 2)
                        self.log(f'REVIEW halve {d._name} dd={dd*100:.2f}%')
                continue

            # === entry (no position) ===
            if self._too_many_positions():
                self.skip['slots_full'] += 1; continue
            if self._cooldown(d):
                self.skip['cooldown'] += 1; continue

            # 前日ベースのフィルタ
            avg_dollar_vol_p = vol_sma_p * close_p if vol_sma_p and close_p else 0.0
            if close_p < self.p.min_price:
                self.skip['min_price'] += 1; continue
            if avg_dollar_vol_p < self.p.min_avg_dollar_vol:
                self.skip['illiquid'] += 1; continue

            uptrend = sma_fast_p > sma_slow_p
            vol_expansion = (vol_p > self.p.vol_mult * vol_sma_p) if vol_sma_p > 0 else False
            pos_momentum = roc_p > 0.0
            price_above_fast = close_p > sma_fast_p

            if not (uptrend and vol_expansion and pos_momentum and price_above_fast):
                self.skip['no_signal'] += 1; continue

            # ギャップ回避（当日寄り vs 前日終値）
            gap = abs(open_0 / close_p - 1.0) if close_p else 1.0
            if gap > self.p.gap_filter_pct:
                self.skip['gap_filter'] += 1; continue

            if atr_0 <= 0:
                self.skip['atr_zero'] += 1; continue

            # 初期ストップ・リスク
            entry_px = open_0 if self.p.use_coc else close_0
            initial_stop = entry_px - self.p.initial_atr_mult * atr_0
            initial_risk = entry_px - initial_stop
            if initial_risk <= 0:
                self.skip['risk_nonpos'] += 1; continue

            # 同時オープンリスク制限
            avail_risk_usd = max(0.0, open_risk_cap_usd - open_risk_now)
            alloc_risk_usd = min(equity * self.p.risk_perc, avail_risk_usd)
            if alloc_risk_usd <= 0:
                self.skip['portfolio_risk_cap'] += 1; continue

            raw_size = math.floor(alloc_risk_usd / initial_risk)
            adv_cap = int(vol_sma_p * self.p.adv_cap_pct) if vol_sma_p else 0
            size = max(0, min(raw_size, adv_cap)) if adv_cap > 0 else raw_size
            if size <= 0:
                self.skip['size_zero'] += 1; continue

            # 親子＋OCO（bracket不使用）
            tp = entry_px + self.p.rr_take_profit * initial_risk
            main = self.buy(data=d, size=size, exectype=bt.Order.Market if self.p.use_coc else bt.Order.Close)
            stop = self.sell(data=d, size=size, exectype=bt.Order.Stop,  price=initial_stop, parent=main)
            limit= self.sell(data=d, size=size, exectype=bt.Order.Limit, price=tp,            parent=main, oco=stop)

            self.entry_price[d] = entry_px
            self.stop_price[d]  = initial_stop
            self.init_risk[d]   = initial_risk
            open_risk_now += initial_risk * size
            self.log(f'ENTRY {d._name}: px={entry_px:.2f} size={size} tp={tp:.2f} sl={initial_stop:.2f}')

    def stop(self):
        # スキップ理由の集計を最後に出力
        if self.skip:
            self.log('--- Skip reasons ---')
            for k, v in sorted(self.skip.items(), key=lambda x: -x[1]):
                self.log(f'{k}: {v}')
