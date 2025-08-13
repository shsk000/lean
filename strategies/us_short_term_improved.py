import backtrader as bt
import math
from collections import defaultdict

class USShortTermImproved(bt.Strategy):
    """
    米国株短期売買戦略 - 改善版

    目的: 短期目線での利益最大化と最大ドローダウンの抑制

    改善点:
    1. エントリー条件の緩和（より多くの機会を捕捉）
    2. エグジット条件の多様化（利確・損切り・トレーリングストップ）
    3. リスク管理の強化（ポジションサイジングの最適化）
    4. 市場環境適応型のパラメータ調整
    """

    params = dict(
        # 移動平均パラメータ
        sma_fast=10,           # 短期移動平均（20→10に短縮）
        sma_slow=30,           # 長期移動平均（50→30に短縮）

        # モメンタム・ボラティリティパラメータ
        roc_period=10,         # ROC期間（20→10に短縮）
        atr_period=14,         # ATR期間
        vol_avg_period=10,     # 出来高平均期間（20→10に短縮）

        # フィルタ条件
        min_price=5.0,         # 最小価格
        min_avg_dollar_vol=5_000_000,  # 最小出来高（1,000万→500万に緩和）
        vol_mult=1.2,          # 出来高倍率（1.5→1.2に緩和）

        # リスク管理
        risk_perc=0.01,        # 1%/トレード
        portfolio_risk_cap=0.05,  # 同時オープンリスク上限（3%→5%に緩和）
        initial_atr_mult=1.5,  # 初期ストップ（2.0→1.5に短縮）
        trail_atr_mult=1.5,    # トレーリングストップ（2.0→1.5に短縮）

        # 利確・損切り
        rr_take_profit=1.5,    # リスク・リワード比（2.0→1.5に調整）
        stop_loss_pct=0.05,    # 固定損切り（5%）
        take_profit_pct=0.08,  # 固定利確（8%）

        # ポジション管理
        max_positions=15,      # 最大ポジション数（10→15に増加）
        adv_cap_pct=0.15,      # 出来高制限（10%→15%に緩和）

        # エントリー条件の緩和
        gap_filter_pct=0.025,  # ギャップフィルタ（1.5%→2.5%に緩和）
        cooldown_bars=3,       # クールダウン期間（5→3に短縮）

        # 見直し条件
        review_dd_pct=0.06,    # 見直しドローダウン（8%→6%に短縮）
        halve_on_review=True,  # 見直し時の半減

        # 実行設定
        use_coc=True,          # 前日判定→翌日寄り約定
        verbose=True,

        # 新機能: 市場環境適応
        adaptive_volatility=True,  # ボラティリティ適応
        volatility_lookback=20,    # ボラティリティ計算期間
        min_volatility_threshold=0.02,  # 最小ボラティリティ閾値
    )

    def __init__(self):
        self.ind = {}
        self.entry_price, self.stop_price, self.init_risk = {}, {}, {}
        self.last_exit_bar = defaultdict(lambda: -10**9)
        self.skip = defaultdict(int)  # スキップ理由カウンタ
        self.trade_count = 0  # 取引回数カウンタ
        self.win_count = 0    # 勝率カウンタ

        for d in self.datas:
            self.ind[d] = dict(
                # 基本インジケータ
                sma_fast=bt.ind.SMA(d.close, period=self.p.sma_fast),
                sma_slow=bt.ind.SMA(d.close, period=self.p.sma_slow),
                roc=bt.ind.ROC(d.close, period=self.p.roc_period),
                atr=bt.ind.ATR(d, period=self.p.atr_period),
                vol_sma=bt.ind.SMA(d.volume, period=self.p.vol_avg_period),

                # 新機能: ボラティリティ適応
                volatility=bt.ind.StdDev(d.close, period=self.p.volatility_lookback),

                # 新機能: 追加フィルタ
                rsi=bt.ind.RSI(d.close, period=14),
                macd=bt.ind.MACD(d.close),
                bb_upper=bt.ind.BollingerBands(d.close).lines.top,
                bb_lower=bt.ind.BollingerBands(d.close).lines.bot,
            )

    # ---- ユーティリティ関数 ----
    def log(self, msg):
        if self.p.verbose:
            dt = self.datas[0].datetime.date(0)
            print(f'{dt} | {msg}')

    def _open_risk_usd(self):
        """現在のオープンリスクを計算"""
        risk = 0.0
        for d in self.datas:
            pos = self.getposition(d)
            if pos.size > 0 and d in self.stop_price:
                px = float(d.close[0])
                per_share = max(0.0, px - self.stop_price[d])
                risk += per_share * pos.size
        return risk

    def _too_many_positions(self):
        """ポジション数制限チェック"""
        return sum(1 for d in self.datas if self.getposition(d).size > 0) >= self.p.max_positions

    def _cooldown(self, d):
        """クールダウン期間チェック"""
        return (len(self) - self.last_exit_bar[d]) < self.p.cooldown_bars

    def _calculate_position_size(self, d, entry_price, stop_price, available_risk):
        """ポジションサイズを計算（改善版）"""
        if stop_price >= entry_price:
            return 0

        risk_per_share = entry_price - stop_price
        if risk_per_share <= 0:
            return 0

        # 基本サイズ
        raw_size = math.floor(available_risk / risk_per_share)

        # 出来高制限
        vol_sma = float(self.ind[d]['vol_sma'][-1])
        adv_cap = int(vol_sma * self.p.adv_cap_pct) if vol_sma else 0

        # ボラティリティ調整
        volatility = float(self.ind[d]['volatility'][-1])
        if self.p.adaptive_volatility and volatility > self.p.min_volatility_threshold:
            # ボラティリティが高い場合はサイズを調整
            volatility_factor = min(1.0, 0.02 / volatility)
            raw_size = int(raw_size * volatility_factor)

        # 最終サイズ決定
        if adv_cap > 0:
            size = max(0, min(raw_size, adv_cap))
        else:
            size = raw_size

        return size

    # ---- 通知関数 ----
    def notify_order(self, order):
        if order.status in [order.Completed, order.Partial]:
            side = 'BUY' if order.isbuy() else 'SELL'
            self.log(f'{side} EXEC {order.data._name}: px={order.executed.price:.2f} size={order.executed.size}')
        elif order.status in [order.Canceled, order.Rejected, order.Margin]:
            self.log(f'ORDER {order.getstatusname()} {order.data._name}')

    def notify_trade(self, trade):
        if trade.isclosed:
            self.trade_count += 1
            if trade.pnl > 0:
                self.win_count += 1

            ret = trade.pnl / (abs(trade.price) * abs(trade.size)) if trade.size else 0.0
            win_rate = (self.win_count / self.trade_count * 100) if self.trade_count > 0 else 0.0

            self.log(f'TRADE CLOSED {trade.data._name}: pnl={trade.pnl:.2f} ret={ret*100:.2f}% | Win Rate: {win_rate:.1f}%')
            self.last_exit_bar[trade.data] = len(self)

            # クリーンアップ
            for dct in (self.entry_price, self.stop_price, self.init_risk):
                dct.pop(trade.data, None)

    # ---- メインロジック ----
    def next(self):
        idx = -1 if self.p.use_coc else 0    # 前日判定→翌日寄り
        equity = self.broker.getvalue()
        open_risk_cap_usd = equity * self.p.portfolio_risk_cap
        open_risk_now = self._open_risk_usd()

        for d in self.datas:
            inds = self.ind[d]

            # ウォームアップチェック
            if len(d) < max(self.p.sma_slow, self.p.roc_period, self.p.atr_period, self.p.vol_avg_period) + 2:
                self.skip['warmup'] += 1
                continue

            pos = self.getposition(d)

            # 価格データ
            close_0 = float(d.close[0])
            open_0  = float(d.open[0])
            close_p = float(d.close[idx])

            # インジケータデータ
            sma_fast_p = float(inds['sma_fast'][idx])
            sma_slow_p = float(inds['sma_slow'][idx])
            roc_p = float(inds['roc'][idx])
            vol_p = float(d.volume[idx])
            vol_sma_p = float(inds['vol_sma'][idx])
            atr_0 = float(inds['atr'][0])
            rsi_p = float(inds['rsi'][idx])
            bb_upper_p = float(inds['bb_upper'][idx])
            bb_lower_p = float(inds['bb_lower'][idx])

            # === 既存ポジション管理 ===
            if pos.size > 0:
                # 1. トレンド転換によるエグジット
                if sma_fast_p < sma_slow_p:
                    self.close(data=d)
                    self.log(f'EXIT dead-cross {d._name}')
                    continue

                # 2. トレーリングストップ更新
                if atr_0 > 0:
                    new_trail = close_0 - self.p.trail_atr_mult * atr_0
                    self.stop_price[d] = max(self.stop_price.get(d, new_trail), new_trail)

                # 3. 固定損切り・利確チェック
                if d in self.entry_price:
                    entry_px = self.entry_price[d]
                    current_return = (close_0 / entry_px - 1.0)

                    # 固定損切り（-5%）
                    if current_return <= -self.p.stop_loss_pct:
                        self.close(data=d)
                        self.log(f'EXIT stop-loss {d._name}: {current_return*100:.2f}%')
                        continue

                    # 固定利確（+8%）
                    if current_return >= self.p.take_profit_pct:
                        self.close(data=d)
                        self.log(f'EXIT take-profit {d._name}: {current_return*100:.2f}%')
                        continue

                    # 見直し（-6%）
                    if current_return <= -self.p.review_dd_pct and self.p.halve_on_review and pos.size > 1:
                        self.sell(data=d, size=pos.size // 2)
                        self.log(f'REVIEW halve {d._name} dd={current_return*100:.2f}%')

                continue

            # === 新規エントリー ===
            if self._too_many_positions():
                self.skip['slots_full'] += 1
                continue

            if self._cooldown(d):
                self.skip['cooldown'] += 1
                continue

            # 基本フィルタ
            avg_dollar_vol_p = vol_sma_p * close_p if vol_sma_p and close_p else 0.0
            if close_p < self.p.min_price:
                self.skip['min_price'] += 1
                continue

            if avg_dollar_vol_p < self.p.min_avg_dollar_vol:
                self.skip['illiquid'] += 1
                continue

            # エントリー条件（緩和版）
            uptrend = sma_fast_p > sma_slow_p
            vol_expansion = (vol_p > self.p.vol_mult * vol_sma_p) if vol_sma_p > 0 else False
            pos_momentum = roc_p > -0.02  # -2%以上に緩和（0%→-2%）
            price_above_fast = close_p > sma_fast_p * 0.98  # 2%以内に緩和

            # 新機能: RSIフィルタ
            rsi_ok = 30 < rsi_p < 70  # RSIが30-70の範囲

            # 新機能: ボリンジャーバンドフィルタ
            bb_ok = bb_lower_p < close_p < bb_upper_p

            if not (uptrend and vol_expansion and pos_momentum and price_above_fast and rsi_ok and bb_ok):
                self.skip['no_signal'] += 1
                continue

            # ギャップフィルタ（緩和版）
            gap = abs(open_0 / close_p - 1.0) if close_p else 1.0
            if gap > self.p.gap_filter_pct:
                self.skip['gap_filter'] += 1
                continue

            if atr_0 <= 0:
                self.skip['atr_zero'] += 1
                continue

            # リスク計算
            entry_px = open_0 if self.p.use_coc else close_0
            initial_stop = entry_px - self.p.initial_atr_mult * atr_0
            initial_risk = entry_px - initial_stop

            if initial_risk <= 0:
                self.skip['risk_nonpos'] += 1
                continue

            # ポートフォリオリスク制限
            avail_risk_usd = max(0.0, open_risk_cap_usd - open_risk_now)
            alloc_risk_usd = min(equity * self.p.risk_perc, avail_risk_usd)

            if alloc_risk_usd <= 0:
                self.skip['portfolio_risk_cap'] += 1
                continue

            # ポジションサイズ計算（改善版）
            size = self._calculate_position_size(d, entry_px, initial_stop, alloc_risk_usd)

            if size <= 0:
                self.skip['size_zero'] += 1
                continue

            # 注文発注（改善版）
            tp = entry_px + self.p.rr_take_profit * initial_risk

            # 親子注文 + OCO
            main = self.buy(data=d, size=size, exectype=bt.Order.Market if self.p.use_coc else bt.Order.Close)
            stop = self.sell(data=d, size=size, exectype=bt.Order.Stop, price=initial_stop, parent=main)
            limit = self.sell(data=d, size=size, exectype=bt.Order.Limit, price=tp, parent=main, oco=stop)

            # 記録
            self.entry_price[d] = entry_px
            self.stop_price[d] = initial_stop
            self.init_risk[d] = initial_risk
            open_risk_now += initial_risk * size

            self.log(f'ENTRY {d._name}: px={entry_px:.2f} size={size} tp={tp:.2f} sl={initial_stop:.2f}')

    def stop(self):
        """戦略終了時の処理"""
        if self.skip:
            self.log('--- Skip reasons ---')
            for k, v in sorted(self.skip.items(), key=lambda x: -x[1]):
                self.log(f'{k}: {v}')

        if self.trade_count > 0:
            win_rate = (self.win_count / self.trade_count * 100)
            self.log(f'--- Final Statistics ---')
            self.log(f'Total Trades: {self.trade_count}')
            self.log(f'Win Rate: {win_rate:.1f}%')
            self.log(f'Total Wins: {self.win_count}')
            self.log(f'Total Losses: {self.trade_count - self.win_count}')
