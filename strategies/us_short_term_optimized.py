import backtrader as bt
import math
from collections import defaultdict

class USShortTermOptimized(bt.Strategy):
    """
    米国株短期売買戦略 - 最適化版

    目的: 短期目線での利益最大化と最大ドローダウンの抑制

    最適化ポイント:
    1. パラメータの市場環境適応型調整
    2. エントリー条件のさらなる最適化
    3. 動的リスク・リワード比の実装
    4. 市場ボラティリティに応じたポジションサイジング
    5. 複数時間軸のシグナル統合
    """

    params = dict(
        # 移動平均パラメータ（最適化）
        sma_fast=8,            # 短期移動平均（10→8に短縮）
        sma_slow=25,           # 長期移動平均（30→25に短縮）

        # モメンタム・ボラティリティパラメータ（最適化）
        roc_period=8,          # ROC期間（10→8に短縮）
        atr_period=10,         # ATR期間（14→10に短縮）
        vol_avg_period=8,      # 出来高平均期間（10→8に短縮）

        # フィルタ条件（最適化）
        min_price=5.0,         # 最小価格
        min_avg_dollar_vol=3_000_000,  # 最小出来高（500万→300万に緩和）
        vol_mult=1.1,          # 出来高倍率（1.2→1.1に緩和）

        # リスク管理（最適化）
        risk_perc=0.015,       # 1.5%/トレード（1%→1.5%に増加）
        portfolio_risk_cap=0.08,  # 同時オープンリスク上限（5%→8%に緩和）
        initial_atr_mult=1.2,  # 初期ストップ（1.5→1.2に短縮）
        trail_atr_mult=1.2,    # トレーリングストップ（1.5→1.2に短縮）

        # 利確・損切り（最適化）
        rr_take_profit=1.2,    # リスク・リワード比（1.5→1.2に調整）
        stop_loss_pct=0.04,    # 固定損切り（5%→4%に短縮）
        take_profit_pct=0.06,  # 固定利確（8%→6%に短縮）

        # ポジション管理（最適化）
        max_positions=20,      # 最大ポジション数（15→20に増加）
        adv_cap_pct=0.20,      # 出来高制限（15%→20%に緩和）

        # エントリー条件（最適化）
        gap_filter_pct=0.03,   # ギャップフィルタ（2.5%→3%に緩和）
        cooldown_bars=2,       # クールダウン期間（3→2に短縮）

        # 見直し条件（最適化）
        review_dd_pct=0.05,    # 見直しドローダウン（6%→5%に短縮）
        halve_on_review=True,  # 見直し時の半減

        # 実行設定
        use_coc=True,          # 前日判定→翌日寄り約定
        verbose=True,

        # 新機能: 高度な市場環境適応
        adaptive_volatility=True,      # ボラティリティ適応
        volatility_lookback=15,        # ボラティリティ計算期間（20→15に短縮）
        min_volatility_threshold=0.015,  # 最小ボラティリティ閾値（2%→1.5%に調整）

        # 新機能: 動的リスク・リワード比
        dynamic_rr=True,               # 動的リスク・リワード比
        min_rr_ratio=1.0,             # 最小リスク・リワード比
        max_rr_ratio=2.0,             # 最大リスク・リワード比

        # 新機能: 市場強度フィルタ
        market_strength_filter=True,   # 市場強度フィルタ
        strength_lookback=10,         # 強度計算期間

        # 新機能: 時間フィルタ
        time_filter=True,             # 時間フィルタ
        avoid_earnings=True,          # 決算発表前後を回避
        earnings_buffer_days=3,       # 決算前後の回避日数
    )

    def __init__(self):
        self.ind = {}
        self.entry_price, self.stop_price, self.init_risk = {}, {}, {}
        self.last_exit_bar = defaultdict(lambda: -10**9)
        self.skip = defaultdict(int)  # スキップ理由カウンタ
        self.trade_count = 0  # 取引回数カウンタ
        self.win_count = 0    # 勝率カウンタ
        self.market_strength = 0.0  # 市場強度

        # 取引履歴記録用（追加）
        self.trades = []
        self.trade_sizes = {}
        self.entry_dates = {}

        for d in self.datas:
            self.ind[d] = dict(
                # 基本インジケータ（最適化）
                sma_fast=bt.ind.SMA(d.close, period=self.p.sma_fast),
                sma_slow=bt.ind.SMA(d.close, period=self.p.sma_slow),
                roc=bt.ind.ROC(d.close, period=self.p.roc_period),
                atr=bt.ind.ATR(d, period=self.p.atr_period),
                vol_sma=bt.ind.SMA(d.volume, period=self.p.vol_avg_period),

                # 高度なフィルタ
                volatility=bt.ind.StdDev(d.close, period=self.p.volatility_lookback),
                rsi=bt.ind.RSI(d.close, period=14),
                macd=bt.ind.MACD(d.close),
                bb_upper=bt.ind.BollingerBands(d.close).lines.top,
                bb_lower=bt.ind.BollingerBands(d.close).lines.bot,

                # 新機能: 市場強度指標
                market_strength_ind=bt.ind.SMA(d.close, period=self.p.strength_lookback),

                # 新機能: 追加モメンタム指標
                stoch_k=bt.ind.Stochastic(d, period=14),
                williams_r=bt.ind.WilliamsR(d, period=14),
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

    def _calculate_dynamic_rr(self, d, entry_price, stop_price):
        """動的リスク・リワード比を計算"""
        if not self.p.dynamic_rr:
            return self.p.rr_take_profit

        # ボラティリティに基づいてリスク・リワード比を調整
        volatility = float(self.ind[d]['volatility'][-1])
        base_rr = self.p.rr_take_profit

        if volatility > 0.03:  # 高ボラティリティ
            dynamic_rr = base_rr * 0.8  # より保守的
        elif volatility < 0.015:  # 低ボラティリティ
            dynamic_rr = base_rr * 1.2  # より積極的
        else:
            dynamic_rr = base_rr

        return max(self.p.min_rr_ratio, min(self.p.max_rr_ratio, dynamic_rr))

    def _calculate_position_size(self, d, entry_price, stop_price, available_risk):
        """ポジションサイズを計算（最適化版）"""
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

        # ボラティリティ調整（最適化）
        volatility = float(self.ind[d]['volatility'][-1])
        if self.p.adaptive_volatility and volatility > self.p.min_volatility_threshold:
            # ボラティリティが高い場合はサイズを調整
            volatility_factor = min(1.0, self.p.min_volatility_threshold / volatility)
            raw_size = int(raw_size * volatility_factor)

        # 市場強度調整
        if self.p.market_strength_filter:
            market_strength = float(self.ind[d]['market_strength_ind'][-1])
            if market_strength > 0:
                strength_factor = min(1.5, 1.0 + (market_strength / 100))
                raw_size = int(raw_size * strength_factor)

        # 最終サイズ決定
        if adv_cap > 0:
            size = max(0, min(raw_size, adv_cap))
        else:
            size = raw_size

        return size

    def _check_market_conditions(self, d):
        """市場環境チェック（新機能）"""
        # RSIフィルタ（最適化）
        rsi_p = float(self.ind[d]['rsi'][-1])
        rsi_ok = 25 < rsi_p < 75  # 25-75の範囲に拡大

        # ボリンジャーバンドフィルタ（最適化）
        bb_upper_p = float(self.ind[d]['bb_upper'][-1])
        bb_lower_p = float(self.ind[d]['bb_lower'][-1])
        close_p = float(d.close[-1])
        bb_ok = bb_lower_p < close_p < bb_upper_p

        # ストキャスティクスフィルタ
        stoch_k = float(self.ind[d]['stoch_k'][-1])
        stoch_ok = 20 < stoch_k < 80

        # ウィリアムズ%Rフィルタ
        williams_r = float(self.ind[d]['williams_r'][-1])
        williams_ok = -80 < williams_r < -20

        return rsi_ok and bb_ok and stoch_ok and williams_ok

    # ---- 通知関数 ----
    def notify_order(self, order):
        if order.status in [order.Completed, order.Partial]:
            side = 'BUY' if order.isbuy() else 'SELL'
            self.log(f'{side} EXEC {order.data._name}: px={order.executed.price:.2f} size={order.executed.size}')

            # 買い注文実行時に取引サイズとエントリー日を保存
            if order.isbuy():
                self.trade_sizes[order.data] = order.executed.size
                self.entry_dates[order.data] = len(self)
            else:
                # 売り注文実行時にも保存（絶対値として）
                self.trade_sizes[order.data] = abs(order.executed.size)

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

            # 取引履歴を記録（追加）
            self._record_trade(trade)

            # クリーンアップ
            for dct in (self.entry_price, self.stop_price, self.init_risk):
                dct.pop(trade.data, None)

    def _record_trade(self, trade):
        """取引履歴を記録"""
        try:
            # 日付を人間が読める形式に変換
            import datetime
            entry_date_formatted = datetime.datetime.fromordinal(int(trade.dtopen)).strftime('%Y-%m-%d')
            exit_date_formatted = datetime.datetime.fromordinal(int(trade.dtclose)).strftime('%Y-%m-%d')

            # 保存された取引サイズを使用
            actual_size = self.trade_sizes.get(trade.data, 0)

            # 出口理由を判定
            exit_reason = 'unknown'
            if trade.data in self.entry_price:
                entry_px = self.entry_price[trade.data]
                current_return = (trade.price / entry_px - 1.0)
                if current_return >= self.p.take_profit_pct:
                    exit_reason = 'take_profit'
                elif current_return <= -self.p.stop_loss_pct:
                    exit_reason = 'stop_loss'
                else:
                    exit_reason = 'strategy_exit'

            trade_record = {
                'entry_date': entry_date_formatted,
                'exit_date': exit_date_formatted,
                'symbol': trade.data._name if hasattr(trade.data, '_name') else 'Unknown',
                'entry_price': trade.price,  # average entry price
                'exit_price': trade.price,   # average exit price
                'size': actual_size,         # 実際の取引サイズ
                'pnl': trade.pnl,
                'return': (trade.pnl / (abs(trade.price) * abs(trade.size)) * 100) if trade.size else 0.0,
                'exit_reason': exit_reason
            }
            self.trades.append(trade_record)

            # デバッグ用：取引カウント表示
            if self.p.verbose:
                print(f'[DEBUG] Total trades recorded: {len(self.trades)}')

        except Exception as e:
            print(f'[ERROR] Failed to record trade: {e}')

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

                    # 固定損切り（-4%）
                    if current_return <= -self.p.stop_loss_pct:
                        self.close(data=d)
                        self.log(f'EXIT stop-loss {d._name}: {current_return*100:.2f}%')
                        continue

                    # 固定利確（+6%）
                    if current_return >= self.p.take_profit_pct:
                        self.close(data=d)
                        self.log(f'EXIT take-profit {d._name}: {current_return*100:.2f}%')
                        continue

                    # 見直し（-5%）
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

            # エントリー条件（最適化版）
            uptrend = sma_fast_p > sma_slow_p
            vol_expansion = (vol_p > self.p.vol_mult * vol_sma_p) if vol_sma_p > 0 else False
            pos_momentum = roc_p > -0.015  # -1.5%以上に緩和（-2%→-1.5%）
            price_above_fast = close_p > sma_fast_p * 0.97  # 3%以内に緩和

            # 市場環境チェック
            market_ok = self._check_market_conditions(d)

            if not (uptrend and vol_expansion and pos_momentum and price_above_fast and market_ok):
                self.skip['no_signal'] += 1
                continue

            # ギャップフィルタ（最適化版）
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

            # ポジションサイズ計算（最適化版）
            size = self._calculate_position_size(d, entry_px, initial_stop, alloc_risk_usd)

            if size <= 0:
                self.skip['size_zero'] += 1
                continue

            # 動的リスク・リワード比計算
            dynamic_rr = self._calculate_dynamic_rr(d, entry_px, initial_stop)
            tp = entry_px + dynamic_rr * initial_risk

            # 注文発注（最適化版）
            main = self.buy(data=d, size=size, exectype=bt.Order.Market if self.p.use_coc else bt.Order.Close)
            stop = self.sell(data=d, size=size, exectype=bt.Order.Stop, price=initial_stop, parent=main)
            limit = self.sell(data=d, size=size, exectype=bt.Order.Limit, price=tp, parent=main, oco=stop)

            # 記録
            self.entry_price[d] = entry_px
            self.stop_price[d] = initial_stop
            self.init_risk[d] = initial_risk
            open_risk_now += initial_risk * size

            self.log(f'ENTRY {d._name}: px={entry_px:.2f} size={size} tp={tp:.2f} sl={initial_stop:.2f} (RR: {dynamic_rr:.2f})')

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

            # 最適化版の追加統計
            if self.p.dynamic_rr:
                self.log(f'Dynamic Risk-Reward: Enabled')
            if self.p.market_strength_filter:
                self.log(f'Market Strength Filter: Enabled')
            if self.p.adaptive_volatility:
                self.log(f'Adaptive Volatility: Enabled')

            # 取引履歴の確認（追加）
            self.log(f'Total trades recorded: {len(self.trades)}')
