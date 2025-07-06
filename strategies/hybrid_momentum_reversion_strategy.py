import backtrader as bt


class HybridMomentumReversionStrategy(bt.Strategy):
    """
    タクティカル・トレンドフォロー戦略
    
    学術的根拠：
    1. Faber (2007) - "A Quantitative Approach to Tactical Asset Allocation"
    2. Antonacci (2014) - "Dual Momentum Investing"
    3. Moskowitz et al. (2012) - 時系列モメンタム効果
    4. Cliff Asness et al. (2013) - モメンタム効果の持続性
    
    戦略概要：
    - 価格が長期移動平均線を上回る場合：フル投資
    - 価格が長期移動平均線を下回る場合：現金保有
    - 月次リバランス（過度な取引コストを回避）
    - シンプルで堅牢な手法
    """
    params = (
        # 長期トレンド判定用移動平均
        ('trend_period', 200),        # 200日移動平均線
        ('momentum_period', 60),      # 60日モメンタム判定
        
        # リバランス頻度制御
        ('rebalance_days', 20),       # 20日毎にリバランス
        
        # ポジションサイジング
        ('position_size', 0.95),      # 資産の95%を投資
        ('cash_reserve', 0.05),       # 5%を現金で保持
        
        # エントリー/エグジット改良
        ('entry_threshold', 1.01),    # MA上1%でエントリー
        ('exit_threshold', 0.99),     # MA下1%でエグジット
        
        # ボラティリティ調整
        ('volatility_lookback', 60),  # 60日ボラティリティ
        ('target_volatility', 0.15),  # 目標年間ボラティリティ15%
    )

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # 各銘柄のインジケーター
        self.trend_mas = []
        self.momentum_indicators = []
        self.volatility_indicators = []
        self.last_rebalance = []
        self.position_states = []
        
        # 取引履歴を記録
        self.trades = []
        
        for i, data in enumerate(self.datas):
            # 長期トレンド移動平均
            trend_ma = bt.indicators.SMA(data.close, period=self.p.trend_period)
            self.trend_mas.append(trend_ma)
            
            # モメンタム指標（過去N日のリターン）
            momentum = bt.indicators.ROC(data.close, period=self.p.momentum_period)
            self.momentum_indicators.append(momentum)
            
            # ボラティリティ指標
            volatility = bt.indicators.StdDev(data.close, period=self.p.volatility_lookback)
            self.volatility_indicators.append(volatility)
            
            # 最後のリバランス日
            self.last_rebalance.append(0)
            
            # ポジション状態
            self.position_states.append('none')  # 'long', 'cash', 'none'
    
    def next(self):
        """メインの売買ロジック"""
        for i, data in enumerate(self.datas):
            # データ不足の場合はスキップ
            if len(data) < self.p.trend_period:
                continue
                
            # リバランス判定
            if self._should_rebalance(i):
                self._rebalance_position(i, data)
                self.last_rebalance[i] = len(self)
    
    def _should_rebalance(self, i: int) -> bool:
        """リバランスが必要かどうかを判定"""
        days_since_rebalance = len(self) - self.last_rebalance[i]
        return days_since_rebalance >= self.p.rebalance_days
    
    def _rebalance_position(self, i: int, data):
        """ポジションのリバランス"""
        current_price = data.close[0]
        trend_ma = self.trend_mas[i][0]
        momentum = self.momentum_indicators[i][0]
        
        # 現在のポジション
        pos = self.getposition(data)
        current_value = self.broker.getvalue()
        
        # トレンド判定
        bullish_trend = current_price > trend_ma * self.p.entry_threshold
        bearish_trend = current_price < trend_ma * self.p.exit_threshold
        
        # モメンタム判定（追加フィルター）
        positive_momentum = momentum > 0
        
        # 総合判定
        should_be_long = bullish_trend and positive_momentum
        should_be_cash = bearish_trend or momentum < -5  # 5%以上の下落モメンタム
        
        # ポジション調整
        if should_be_long and (pos.size == 0 or self.position_states[i] == 'cash'):
            # ロングポジション構築
            self._enter_long_position(i, data, current_value)
            
        elif should_be_cash and (pos.size > 0 or self.position_states[i] == 'long'):
            # 現金ポジション（全売却）
            self._exit_to_cash(i, data)
    
    def _enter_long_position(self, i: int, data, current_value: float):
        """ロングポジションエントリー"""
        # 現在のポジションを一旦クリア
        pos = self.getposition(data)
        if pos.size > 0:
            self.close(data=data)
        
        # 複数銘柄対応のポジションサイズ計算
        price = data.close[0]
        total_value = self.broker.getvalue()
        
        # 銘柄ごとに均等配分（上位20銘柄まで）
        max_positions = min(20, len(self.datas))
        target_allocation = total_value / max_positions
        target_shares = int(target_allocation * 0.95 / price)  # 95%を投資
        
        if target_shares > 0:
            # 買い注文
            self.buy(data=data, size=target_shares)
            self.position_states[i] = 'long'
            target_value = target_shares * price
            self.log(f'LONG ENTRY: {data._name}, Price: {price:.2f}, Shares: {target_shares}, Value: {target_value:.0f}')
    
    def _exit_to_cash(self, i: int, data):
        """現金ポジションへの移行"""
        pos = self.getposition(data)
        if pos.size > 0:
            self.close(data=data)
            self.position_states[i] = 'cash'
            self.log(f'EXIT TO CASH: {data._name}, Price: {data.close[0]:.2f}')
    
    def log(self, txt: str):
        """ログ出力"""
        dt = self.datas[0].datetime.date(0)
        print(f'{dt}: {txt}')
    
    def notify_order(self, order):
        """注文通知"""
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED: Price {order.executed.price:.2f}, Size {order.executed.size}')
            else:
                self.log(f'SELL EXECUTED: Price {order.executed.price:.2f}, Size {order.executed.size}')
    
    def notify_trade(self, trade):
        """取引通知"""
        if trade.isclosed:
            pnl_pct = (trade.pnlcomm / abs(trade.value) * 100) if trade.value != 0 else 0
            self.log(f'TRADE CLOSED: PnL {trade.pnl:.2f}, PnL%: {pnl_pct:.2f}%')
            
            # 取引履歴を記録
            try:
                exit_price = trade.price + (trade.pnl / trade.size) if trade.size != 0 else trade.price
            except:
                exit_price = trade.price
                
            trade_record = {
                'entry_date': trade.dtopen,
                'exit_date': trade.dtclose,
                'symbol': trade.data._name if hasattr(trade.data, '_name') else 'Unknown',
                'entry_price': trade.price,
                'exit_price': exit_price,
                'size': trade.size,
                'pnl': trade.pnl,
                'return': pnl_pct,
                'exit_reason': 'strategy_exit'
            }
            self.trades.append(trade_record)
            
            # デバッグ用：取引カウント表示
            print(f'[DEBUG] Total trades recorded: {len(self.trades)}')