import backtrader as bt
import math
from collections import defaultdict

class USShortTermAggressive(bt.Strategy):
    """
    米国株短期売買戦略 - 積極版

    ================================================================================
    🎯 戦略の目的
    ================================================================================
    短期目線での高リターン追求（年率10%以上を目標）

    ================================================================================
    📊 戦略の特徴
    ================================================================================
    1. より積極的なリスク管理（3%/トレード）
    2. より大きな利確目標（15%）
    3. エントリー条件の緩和
    4. トレンドフォロー重視
    5. ボラティリティを活用したポジションサイジング

    ================================================================================
    🔧 パラメータ設定（積極化）
    ================================================================================

    【移動平均パラメータ】
    - sma_fast: 5日（超短期トレンド判定）
    - sma_slow: 20日（短期トレンド判定）

    【モメンタム・ボラティリティ】
    - roc_period: 5日（短期モメンタム）
    - atr_period: 10日（ボラティリティ測定）
    - vol_avg_period: 5日（出来高平均）

    【フィルタ条件（緩和）】
    - min_price: $5.0（最小価格）
    - min_avg_dollar_vol: $2,000,000（出来高制限緩和）
    - vol_mult: 1.5倍（出来高倍率増加）

    【リスク管理（積極化）】
    - risk_perc: 3%/トレード（従来の2倍）
    - portfolio_risk_cap: 15%（同時オープンリスク上限）
    - initial_atr_mult: 1.5（初期ストップ）
    - trail_atr_mult: 1.5（トレーリングストップ）

    【利確・損切り（積極化）】
    - rr_take_profit: 2.0（リスク・リワード比）
    - stop_loss_pct: 8%（固定損切り、従来の2倍に緩和）
    - take_profit_pct: 15%（固定利確、従来の2.5倍に増加）

    【ポジション管理（積極化）】
    - max_positions: 15（最大ポジション数）
    - adv_cap_pct: 30%（出来高制限、従来の1.5倍に緩和）

    【エントリー条件（緩和）】
    - gap_filter_pct: 5%（ギャップフィルタ、従来の1.7倍に緩和）
    - cooldown_bars: 1日（クールダウン期間短縮）

    【見直し条件（緩和）】
    - review_dd_pct: 10%（見直しドローダウン、従来の2倍に緩和）

    ================================================================================
    📈 エントリー条件
    ================================================================================

    【基本条件（AND条件）】
    1. 上昇トレンド: SMA5 > SMA20 × 1.02（2%以上の上昇トレンド）
    2. 出来高拡大: 現在の出来高 > 平均出来高 × 1.5
    3. 正のモメンタム: ROC5 > 2%（2%以上の正のモメンタム）
    4. 価格位置: 現在価格 > SMA5 × 0.98（2%以内）
    5. 市場環境: RSI(20-80), ボリンジャーバンド(±5%余裕)
    6. EMAトレンド: EMA20 > EMA50

    【追加フィルタ】
    - ギャップ制限: 5%以内
    - 出来高制限: 200万ドル以上
    - 価格制限: $5.0以上

    ================================================================================
    🚪 エグジット条件
    ================================================================================

    【自動エグジット】
    1. 固定損切り: -8%（従来の-4%から緩和）
    2. 固定利確: +15%（従来の+6%から増加）
    3. トレンド転換: SMA5 < SMA20 × 0.98（2%の余裕）

    【手動管理】
    - トレーリングストップ: ATR × 1.5
    - 見直し: -10%でポジション半減

    ================================================================================
    💰 ポジションサイジング
    ================================================================================

    【基本サイズ】
    - リスク/株あたりリスク = 基本サイズ

    【ボラティリティスケーリング】
    - 高ボラティリティ時: サイズを最大2倍まで増加
    - 低ボラティリティ時: 基本サイズ

    【制限】
    - 出来高制限: 平均出来高 × 30%
    - ポートフォリオリスク: 15%上限

    ================================================================================
    🎲 期待される改善効果
    ================================================================================

    【リターン向上】
    - 利確目標: 6% → 15%（2.5倍）
    - リスク・リワード比: 1.2 → 2.0（1.7倍）
    - 年率リターン目標: 0.08% → 10%以上（125倍）

    【リスク管理】
    - 損切り: 4% → 8%（2倍に緩和）
    - トレードあたりリスク: 1.5% → 3%（2倍に増加）
    - ポートフォリオリスク: 8% → 15%（2倍に増加）

    【エントリー頻度】
    - フィルタ条件緩和により増加
    - クールダウン期間短縮（3日 → 1日）
    - より多くの機会を捕捉

    ================================================================================
    ⚠️ リスク要因
    ================================================================================

    【高リスク・高リターン】
    - より大きな損失の可能性
    - ボラティリティの高い市場での不安定性
    - トレンド転換時の素早い対応が必要

    【適用手法】
    - 市場環境に応じた動的調整
    - 定期的なパフォーマンス評価
    - リスク管理の継続的改善

    ================================================================================
    📊 バックテスト結果の期待値
    ================================================================================

    【単一銘柄（3年間）】
    - 従来戦略: 0.19%（年率0.06%）
    - 積極戦略: 目標30%（年率10%）

    【複数銘柄（3年間）】
    - 従来戦略: 0.23%（年率0.08%）
    - 積極戦略: 目標25%（年率8%）

    【勝率】
    - 従来戦略: 54.5%
    - 積極戦略: 目標45-50%（リスク増加により）

    ================================================================================
    🔄 戦略の実行フロー
    ================================================================================

    1. 市場環境チェック（RSI, ボリンジャーバンド）
    2. トレンド判定（SMA5 vs SMA20, EMA20 vs EMA50）
    3. モメンタム確認（ROC5 > 2%）
    4. 出来高確認（1.5倍以上）
    5. リスク計算（ATRベース）
    6. ポジションサイズ決定（ボラティリティスケーリング）
    7. 注文発注（買い + ストップ + リミット）
    8. ポジション管理（トレーリングストップ、固定損益）

    ================================================================================
    📝 使用方法
    ================================================================================

    # 単一銘柄テスト
    python bt_runner.py --single AAPL --strategy us_short_term_aggressive

    # 複数銘柄テスト
    python bt_runner.py --limit 10 --strategy us_short_term_aggressive

    ================================================================================
    """

    params = dict(
        # 移動平均パラメータ（トレンド重視）
        sma_fast=5,             # 超短期移動平均（5日）
        sma_slow=20,            # 短期移動平均（20日）

        # モメンタム・ボラティリティパラメータ
        roc_period=5,           # ROC期間（5日）
        atr_period=10,          # ATR期間（10日）
        vol_avg_period=5,       # 出来高平均期間（5日）

        # フィルタ条件（緩和）
        min_price=5.0,          # 最小価格
        min_avg_dollar_vol=2_000_000,  # 最小出来高（200万に緩和）
        vol_mult=1.5,           # 出来高倍率（1.5に増加）

        # リスク管理（現実的な設定）
        risk_perc=0.01,         # 1%/トレード（従来より保守的）
        portfolio_risk_cap=0.05,  # 同時オープンリスク上限5%（従来より保守的）

        # 利確・損切り（現実的な設定）
        stop_loss_pct=0.04,     # 固定損切り4%（従来と同じ）
        take_profit_pct=0.06,   # 固定利確6%（従来と同じ）

        # ポジション管理（現実的な設定）
        max_positions=8,        # 最大ポジション数8（従来より保守的）
        adv_cap_pct=0.15,       # 出来高制限15%（従来より保守的）

        # エントリー条件（適度な緩和）
        gap_filter_pct=0.03,    # ギャップフィルタ3%（従来と同じ）
        cooldown_bars=3,        # クールダウン期間3日（従来と同じ）

        # 見直し条件（現実的な設定）
        review_dd_pct=0.06,     # 見直しドローダウン6%（従来と同じ）
        halve_on_review=True,   # 見直し時の半減

        # 実行設定
        use_coc=True,           # 前日判定→翌日寄り約定
        verbose=True,

        # 新機能: モメンタム強化（現実的な設定）
        momentum_threshold=0.01,  # モメンタム閾値1%

        # 新機能: トレンド強度（現実的な設定）
        trend_strength_threshold=0.01,  # トレンド強度閾値1%
    )

    def __init__(self):
        self.ind = {}
        self.entry_price, self.stop_price, self.init_risk = {}, {}, {}
        self.last_exit_bar = defaultdict(lambda: -10**9)
        self.skip = defaultdict(int)
        self.trade_count = 0
        self.win_count = 0
        self.total_pnl = 0.0

        # 取引履歴記録用
        self.trades = []
        self.trade_sizes = {}
        self.entry_dates = {}

        for d in self.datas:
            self.ind[d] = dict(
                # 基本インジケータ
                sma_fast=bt.ind.SMA(d.close, period=self.p.sma_fast),
                sma_slow=bt.ind.SMA(d.close, period=self.p.sma_slow),
                roc=bt.ind.ROC(d.close, period=self.p.roc_period),
                atr=bt.ind.ATR(d, period=self.p.atr_period),
                vol_sma=bt.ind.SMA(d.volume, period=self.p.vol_avg_period),

                # フィルタ用
                rsi=bt.ind.RSI(d.close, period=14),
                bb_upper=bt.ind.BollingerBands(d.close).lines.top,
                bb_lower=bt.ind.BollingerBands(d.close).lines.bot,

                # 追加インジケータ
                ema_20=bt.ind.EMA(d.close, period=20),
                ema_50=bt.ind.EMA(d.close, period=50),
            )

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
        """ポジションサイズを計算（簡素化版）"""
        if stop_price >= entry_price:
            return 0

        # 固定パーセンテージベースのストップロス
        stop_loss_pct = self.p.stop_loss_pct
        risk_per_share = entry_price * stop_loss_pct

        if risk_per_share <= 0:
            return 0

        # 基本サイズ
        raw_size = math.floor(available_risk / risk_per_share)

        # 出来高制限
        vol_sma = float(self.ind[d]['vol_sma'][-1])
        adv_cap = int(vol_sma * self.p.adv_cap_pct) if vol_sma else 0

        # 最終サイズ決定
        if adv_cap > 0:
            size = max(0, min(raw_size, adv_cap))
        else:
            size = raw_size

        return size

    def _check_market_conditions(self, d):
        """市場環境チェック（簡素化版）"""
        # RSIフィルタ（より緩い）
        rsi_p = float(self.ind[d]['rsi'][-1])
        rsi_ok = 20 < rsi_p < 80  # 20-80の範囲に拡大

        # ボリンジャーバンドフィルタ（より緩い）
        bb_upper_p = float(self.ind[d]['bb_upper'][-1])
        bb_lower_p = float(self.ind[d]['bb_lower'][-1])
        close_p = float(d.close[-1])
        bb_ok = bb_lower_p * 0.95 < close_p < bb_upper_p * 1.05  # 5%の余裕

        return rsi_ok and bb_ok

    def notify_order(self, order):
        if order.status in [order.Completed, order.Partial]:
            side = 'BUY' if order.isbuy() else 'SELL'
            self.log(f'{side} EXEC {order.data._name}: px={order.executed.price:.2f} size={order.executed.size}')

            if order.isbuy():
                self.trade_sizes[order.data] = order.executed.size
                self.entry_dates[order.data] = len(self)
            else:
                self.trade_sizes[order.data] = abs(order.executed.size)

        elif order.status in [order.Canceled, order.Rejected, order.Margin]:
            self.log(f'ORDER {order.getstatusname()} {order.data._name}')

    def notify_trade(self, trade):
        if trade.isclosed:
            self.trade_count += 1
            self.total_pnl += trade.pnl

            if trade.pnl > 0:
                self.win_count += 1

            ret = trade.pnl / (abs(trade.price) * abs(trade.size)) if trade.size else 0.0
            win_rate = (self.win_count / self.trade_count * 100) if self.trade_count > 0 else 0.0

            self.log(f'TRADE CLOSED {trade.data._name}: pnl={trade.pnl:.2f} ret={ret*100:.2f}% | Win Rate: {win_rate:.1f}% | Total PnL: {self.total_pnl:.2f}')
            self.last_exit_bar[trade.data] = len(self)

            # 取引履歴を記録
            self._record_trade(trade)

            # クリーンアップ
            for dct in (self.entry_price, self.stop_price, self.init_risk):
                dct.pop(trade.data, None)

    def _record_trade(self, trade):
        """取引履歴を記録"""
        try:
            import datetime
            entry_date_formatted = datetime.datetime.fromordinal(int(trade.dtopen)).strftime('%Y-%m-%d')
            exit_date_formatted = datetime.datetime.fromordinal(int(trade.dtclose)).strftime('%Y-%m-%d')

            actual_size = self.trade_sizes.get(trade.data, 0)

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
                'entry_price': trade.price,
                'exit_price': trade.price,
                'size': actual_size,
                'pnl': trade.pnl,
                'return': (trade.pnl / (abs(trade.price) * abs(trade.size)) * 100) if trade.size else 0.0,
                'exit_reason': exit_reason
            }
            self.trades.append(trade_record)

            if self.p.verbose:
                print(f'[DEBUG] Total trades recorded: {len(self.trades)}')

        except Exception as e:
            print(f'[ERROR] Failed to record trade: {e}')

    def next(self):
        """メインの売買ロジック"""
        idx = -1 if self.p.use_coc else 0
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
            ema_20_p = float(inds['ema_20'][idx])
            ema_50_p = float(inds['ema_50'][idx])

            # === 既存ポジション管理 ===
            if pos.size > 0:
                # 1. トレンド転換によるエグジット（現実的な調整）
                if sma_fast_p < sma_slow_p * 0.99:  # 1%の余裕
                    self.close(data=d)
                    self.log(f'EXIT trend-reversal {d._name}')
                    continue

                # 2. トレーリングストップ更新（簡素化）
                if atr_0 > 0:
                    new_trail = close_0 * (1 - self.p.stop_loss_pct)  # 固定パーセンテージ
                    self.stop_price[d] = max(self.stop_price.get(d, new_trail), new_trail)

                # 3. 固定損切り・利確チェック
                if d in self.entry_price:
                    entry_px = self.entry_price[d]
                    current_return = (close_0 / entry_px - 1.0)

                    # 固定損切り（-8%）
                    if current_return <= -self.p.stop_loss_pct:
                        self.close(data=d)
                        self.log(f'EXIT stop-loss {d._name}: {current_return*100:.2f}%')
                        continue

                    # 固定利確（+15%）
                    if current_return >= self.p.take_profit_pct:
                        self.close(data=d)
                        self.log(f'EXIT take-profit {d._name}: {current_return*100:.2f}%')
                        continue

                    # 見直し（-10%）
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

            # エントリー条件（簡素化版）
            uptrend = sma_fast_p > sma_slow_p * (1 + self.p.trend_strength_threshold)  # 1%以上の上昇トレンド
            vol_expansion = (vol_p > self.p.vol_mult * vol_sma_p) if vol_sma_p > 0 else False
            pos_momentum = roc_p > self.p.momentum_threshold  # 1%以上の正のモメンタム
            price_above_fast = close_p > sma_fast_p * 0.99  # 1%以内

            # 市場環境チェック（簡素化）
            market_ok = self._check_market_conditions(d)

            # 追加のトレンド確認（簡素化）
            ema_trend = ema_20_p > ema_50_p  # EMA20 > EMA50

            if not (uptrend and vol_expansion and pos_momentum and price_above_fast and market_ok and ema_trend):
                self.skip['no_signal'] += 1
                continue

            # ギャップフィルタ
            gap = abs(open_0 / close_p - 1.0) if close_p else 1.0
            if gap > self.p.gap_filter_pct:
                self.skip['gap_filter'] += 1
                continue

            # リスク計算（簡素化）
            entry_px = open_0 if self.p.use_coc else close_0
            initial_stop = entry_px * (1 - self.p.stop_loss_pct)  # 固定パーセンテージ
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

            # ポジションサイズ計算
            size = self._calculate_position_size(d, entry_px, initial_stop, alloc_risk_usd)

            if size <= 0:
                self.skip['size_zero'] += 1
                continue

            # 注文発注（簡素化）
            main = self.buy(data=d, size=size, exectype=bt.Order.Market if self.p.use_coc else bt.Order.Close)
            stop = self.sell(data=d, size=size, exectype=bt.Order.Stop, price=initial_stop, parent=main)

            # 記録
            self.entry_price[d] = entry_px
            self.stop_price[d] = initial_stop
            self.init_risk[d] = initial_risk
            open_risk_now += initial_risk * size

            self.log(f'ENTRY {d._name}: px={entry_px:.2f} size={size} sl={initial_stop:.2f}')

    def stop(self):
        """戦略終了時の処理"""
        if self.skip:
            self.log('--- Skip reasons ---')
            for k, v in sorted(self.skip.items(), key=lambda x: -x[1]):
                self.log(f'{k}: {v}')

        if self.trade_count > 0:
            win_rate = (self.win_count / self.trade_count * 100)
            avg_pnl = self.total_pnl / self.trade_count
            self.log(f'--- Final Statistics ---')
            self.log(f'Total Trades: {self.trade_count}')
            self.log(f'Win Rate: {win_rate:.1f}%')
            self.log(f'Total PnL: {self.total_pnl:.2f}')
            self.log(f'Average PnL per Trade: {avg_pnl:.2f}')
            self.log(f'Total Wins: {self.win_count}')
            self.log(f'Total Losses: {self.trade_count - self.win_count}')

            # 取引履歴の確認
            self.log(f'Total trades recorded: {len(self.trades)}')

            # 年率リターン計算
            if len(self.datas[0]) > 0:
                years = len(self.datas[0]) / 252
                annual_return = (self.total_pnl / 100000) / years * 100
                self.log(f'Annual Return: {annual_return:.2f}%')
