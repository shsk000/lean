"""
RSI Gap Strategy Backtesting Tool
================================

このスクリプトは、RSIとギャップを組み合わせたトレーディング戦略のバックテストを実行します。

実行方法:
--------

1. 単一銘柄バックテスト（推奨 - GUI表示が確実）
   python bt_rsi_gap.py --single AAPL
   python bt_rsi_gap.py --single MSFT --start-date 2022-01-01 --end-date 2023-01-01
   python bt_rsi_gap.py --single TSLA --no-plot  # チャート表示なし

2. 複数銘柄バックテスト（S&P500銘柄から自動選択）
   python bt_rsi_gap.py
   python bt_rsi_gap.py --workers 30  # 並列処理数を変更
   python bt_rsi_gap.py --no-plot     # チャート表示なし

コマンドライン引数:
------------------
--single SYMBOL     : 単一銘柄でバックテスト（例: AAPL, MSFT, TSLA）
--start-date DATE   : 開始日（デフォルト: 2021-01-01）
--end-date DATE     : 終了日（デフォルト: 2023-01-01）
--no-plot          : チャート表示を無効化
--workers N        : 並列処理数（デフォルト: 20）

戦略パラメータ:
--------------
- RSI期間: 14日
- 最大保有日数: 5日
- 利確目標: 5%
- トレーリングストップ: 3%
- エントリー条件: ギャップ < 2% かつ 30 < RSI < 40

出力ファイル:
------------
単一銘柄の場合:
- output/single_stock_SYMBOL_results.csv    : 基本結果
- output/trades_history_SYMBOL.csv          : 取引履歴
- output/detailed_statistics_SYMBOL.csv     : 詳細統計
- output/reason_statistics_SYMBOL.csv       : 取引理由別統計

複数銘柄の場合:
- output/multi_stock_backtest_results.csv   : 基本結果
- output/trades_history.csv                 : 取引履歴
- output/detailed_statistics.csv            : 詳細統計
- output/symbol_statistics.csv              : 銘柄別統計
- output/reason_statistics.csv              : 取引理由別統計

注意事項:
--------
- GUI環境が必要です（WSL2の場合はX11設定が必要な場合があります）
- 初回実行時はS&P500銘柄リストの取得に時間がかかります
- 並列処理により大幅に高速化されています
"""

import backtrader as bt
import yfinance as yf
import pandas as pd
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import argparse
import os

# 日本語フォント設定（Linux環境用）
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False

# 出力ディレクトリの設定
OUTPUT_DIR = 'output'

def ensure_output_dir():
    """出力ディレクトリが存在しない場合は作成"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"出力ディレクトリ '{OUTPUT_DIR}' を作成しました")

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
        self.trades = []  # 取引履歴を保存

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
                self.trades.append({
                    'symbol': self.data._name,
                    'entry_price': self.entry_price,
                    'exit_price': price,
                    'return': (price - self.entry_price) / self.entry_price * 100,
                    'exit_reason': 'profit_target'
                })
            elif price <= self.highest_price * (1 - self.p.trailing_stop):
                self.close()
                self.trades.append({
                    'symbol': self.data._name,
                    'entry_price': self.entry_price,
                    'exit_price': price,
                    'return': (price - self.entry_price) / self.entry_price * 100,
                    'exit_reason': 'trailing_stop'
                })
            elif len(self) - self.entry_bar >= self.p.max_hold_days:
                self.close()
                self.trades.append({
                    'symbol': self.data._name,
                    'entry_price': self.entry_price,
                    'exit_price': price,
                    'return': (price - self.entry_price) / self.entry_price * 100,
                    'exit_reason': 'max_hold_days'
                })

class CompatibleCerebro(bt.Cerebro):
    """backtraderの互換性問題を解決するCerebroクラス"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 必要な属性を初期化
        if not hasattr(self, '_exactbars'):
            self._exactbars = 0
        if not hasattr(self, '_plotting'):
            self._plotting = True

def get_sp500_symbols():
    """S&P500の銘柄リストを取得"""
    try:
        # S&P500の銘柄リストを取得
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        sp500_table = tables[0]
        symbols = sp500_table['Symbol'].tolist()
        return symbols
    except Exception as e:
        print(f"S&P500銘柄リストの取得に失敗しました: {e}")
        # フォールバック: 主要な銘柄リスト
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'AMD', 'INTC']

def download_stock_data(symbol, start_date, end_date):
    """個別銘柄のデータをダウンロード"""
    try:
        df = yf.download(symbol, start=start_date, end=end_date, progress=False, auto_adjust=False)
        if df is None or df.empty or len(df) < 10:
            return symbol, None, "データ不足"
        
        if 'Volume' not in df.columns:
            return symbol, None, "Volumeカラムなし"
        
        volume_data = df['Volume'].dropna()
        if len(volume_data) == 0:
            return symbol, None, "出来高データなし"
        
        avg_volume = float(volume_data.mean().item())
        return symbol, avg_volume, "成功"
        
    except Exception as e:
        return symbol, None, f"エラー: {e}"

def filter_high_volume_stocks_parallel(symbols, min_avg_volume=1000000, lookback_days=30, max_workers=10):
    """並列処理で出来高の多い銘柄をフィルタリング"""
    high_volume_symbols = []
    
    print("並列処理で出来高フィルタリング中...")
    end_date = datetime.now()
    start_date = end_date - pd.Timedelta(days=lookback_days)
    
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 全銘柄のダウンロードタスクを開始
        future_to_symbol = {
            executor.submit(download_stock_data, symbol, start_date, end_date): symbol 
            for symbol in symbols
        }
        
        completed = 0
        for future in as_completed(future_to_symbol):
            symbol, avg_volume, status = future.result()
            completed += 1
            
            if status == "成功" and avg_volume >= min_avg_volume:
                high_volume_symbols.append(symbol)
                print(f"✓ {symbol}: 平均出来高 {avg_volume:,.0f}")
            elif status == "成功":
                print(f"✗ {symbol}: 出来高不足 (平均: {avg_volume:,.0f})")
            else:
                print(f"✗ {symbol}: {status}")
            
            # 進捗表示
            if completed % 50 == 0:
                elapsed = time.time() - start_time
                rate = completed / elapsed
                remaining = (len(symbols) - completed) / rate if rate > 0 else 0
                print(f"進捗: {completed}/{len(symbols)} 銘柄処理済み "
                      f"(経過時間: {elapsed:.1f}秒, 残り推定: {remaining:.1f}秒)")
    
    total_time = time.time() - start_time
    print(f"\nフィルタリング完了: {len(high_volume_symbols)}/{len(symbols)} 銘柄が条件を満たしました")
    print(f"総処理時間: {total_time:.1f}秒, 平均: {total_time/len(symbols):.2f}秒/銘柄")
    
    return high_volume_symbols

def download_backtest_data(symbol, start_date, end_date):
    """バックテスト用のデータをダウンロード"""
    try:
        df = yf.download(symbol, start=start_date, end=end_date, auto_adjust=False, progress=False)
        if df.empty or len(df) < 50:
            return symbol, None, f"データ不足 ({len(df)}日)"
        
        df.dropna(inplace=True)
        if len(df) < 50:
            return symbol, None, f"データ不足 ({len(df)}日)"
        
        # 必要なカラムだけ抽出し、カラム名を明示的にリネーム
        df = df[['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']]
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
        
        return symbol, df, "成功"
        
    except Exception as e:
        return symbol, None, f"エラー: {e}"

def run_multi_stock_backtest_parallel(symbols, start_date='2021-01-01', end_date='2023-01-01', initial_cash=100000, max_workers=10, show_plot=True):
    """並列処理で複数銘柄のバックテストを実行"""
    cerebro = CompatibleCerebro()
    cerebro.addstrategy(RSIGapStrategy)
    
    print(f"\n並列処理で{len(symbols)}銘柄のデータ取得中...")
    start_time = time.time()
    
    # 並列でデータを取得
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {
            executor.submit(download_backtest_data, symbol, start_date, end_date): symbol 
            for symbol in symbols
        }
        
        completed = 0
        for future in as_completed(future_to_symbol):
            symbol, df, status = future.result()
            completed += 1
            
            if status == "成功":
                data = bt.feeds.PandasData(dataname=df, name=symbol)
                cerebro.adddata(data)
                print(f"✓ {symbol}: データ追加完了 ({len(df)}日)")
            else:
                print(f"✗ {symbol}: {status}")
            
            # 進捗表示
            if completed % 10 == 0:
                elapsed = time.time() - start_time
                rate = completed / elapsed
                remaining = (len(symbols) - completed) / rate if rate > 0 else 0
                print(f"進捗: {completed}/{len(symbols)} 銘柄処理済み "
                      f"(経過時間: {elapsed:.1f}秒, 残り推定: {remaining:.1f}秒)")
    
    data_time = time.time() - start_time
    print(f"データ取得完了: {data_time:.1f}秒")
    
    # バックテスト実行
    print(f"\nバックテスト実行中...")
    cerebro.broker.setcash(initial_cash)
    initial_value = cerebro.broker.getvalue()
    print(f'初期ポートフォリオ価値: ${initial_value:,.2f}')
    
    results = cerebro.run()
    final_value = cerebro.broker.getvalue()
    
    # 結果の集計
    total_return = (final_value - initial_value) / initial_value * 100
    total_time = time.time() - start_time
    profit = final_value - initial_value
    
    print(f'\n{"="*60}')
    print(f'📊 バックテスト結果サマリー')
    print(f'{"="*60}')
    print(f'処理銘柄数: {len(symbols)}')
    print(f'初期ポートフォリオ価値: ${initial_value:,.2f}')
    print(f'最終ポートフォリオ価値: ${final_value:,.2f}')
    print(f'{"-"*60}')
    
    # 利益の有無を明確に表示
    if profit > 0:
        print(f'💰 利益: +${profit:,.2f} (+{total_return:.2f}%)')
        print(f'✅ 戦略は利益を上げました！')
    elif profit < 0:
        print(f'💸 損失: -${abs(profit):,.2f} ({total_return:.2f}%)')
        print(f'❌ 戦略は損失を出しました')
    else:
        print(f'⚖️  損益: $0.00 (0.00%)')
        print(f'➖ 戦略は損益ゼロでした')
    
    print(f'{"="*60}')
    print(f'総処理時間: {total_time:.1f}秒')
    
    # GUIで結果を表示
    if show_plot:
        print(f'\nチャートを表示中...')
        try:
            # チャート表示設定
            cerebro.plot(style='candlestick', 
                        barup='green', 
                        bardown='red', 
                        volume=True, 
                        figsize=(15, 10),
                        dpi=100)
            plt.show()
        except Exception as e:
            print(f"チャート表示エラー: {e}")
            print("代替方法でチャート表示を試行中...")
            try:
                # 代替方法：よりシンプルな設定
                cerebro.plot(figsize=(15, 10))
                plt.show()
            except Exception as e2:
                print(f"代替チャート表示も失敗: {e2}")
                print("最終手段：最もシンプルなチャート表示を試行...")
                try:
                    # 最終手段：最もシンプルな設定
                    cerebro.plot()
                    plt.show()
                except Exception as e3:
                    print(f"チャート表示に失敗しました: {e3}")
                    print("結果はCSVファイルで確認してください")
                    import traceback
                    print("詳細エラー:")
                    print(traceback.format_exc())
    else:
        print(f'\nチャート表示は無効化されています')
    
    # 取引履歴を表示
    all_trades = []
    for strategy in results:
        if hasattr(strategy, 'trades'):
            all_trades.extend(strategy.trades)
    
    if all_trades:
        print(f'\n{"="*60}')
        print(f'📈 取引履歴詳細')
        print(f'{"="*60}')
        print(f'総取引数: {len(all_trades)}')
        
        # 取引結果をDataFrameに変換
        trades_df = pd.DataFrame(all_trades)
        
        # 統計情報
        print(f'\n📊 統計情報')
        print(f'{"-"*40}')
        avg_return = trades_df["return"].mean()
        win_rate = (trades_df["return"] > 0).mean() * 100
        max_profit = trades_df["return"].max()
        max_loss = trades_df["return"].min()
        std_return = trades_df["return"].std()
        
        print(f'平均リターン: {avg_return:.2f}%')
        print(f'勝率: {win_rate:.1f}%')
        print(f'最大利益: {max_profit:.2f}%')
        print(f'最大損失: {max_loss:.2f}%')
        print(f'標準偏差: {std_return:.2f}%')
        
        # 勝率の評価
        if win_rate >= 60:
            print(f'🎯 勝率評価: 優秀 ({win_rate:.1f}%)')
        elif win_rate >= 50:
            print(f'👍 勝率評価: 良好 ({win_rate:.1f}%)')
        elif win_rate >= 40:
            print(f'⚠️  勝率評価: 普通 ({win_rate:.1f}%)')
        else:
            print(f'❌ 勝率評価: 低い ({win_rate:.1f}%)')
        
        # 取引理由別の統計
        print(f'\n📋 取引理由別統計')
        print(f'{"-"*40}')
        reason_stats = trades_df.groupby('exit_reason')['return'].agg(['count', 'mean', 'sum'])
        print(reason_stats)
        
        # 銘柄別の統計
        print(f'\n🏢 銘柄別統計')
        print(f'{"-"*40}')
        symbol_stats = trades_df.groupby('symbol')['return'].agg(['count', 'mean', 'sum'])
        print(symbol_stats)
        
        # 取引履歴をCSVに保存
        trades_df.to_csv(os.path.join(OUTPUT_DIR, f'trades_history.csv'), index=False)
        print(f'\n💾 取引履歴を "{OUTPUT_DIR}/trades_history.csv" に保存しました')
        
        # 詳細統計をCSVに保存
        detailed_stats = {
            'metric': ['総取引数', '平均リターン', '勝率', '最大利益', '最大損失', '標準偏差'],
            'value': [
                len(all_trades),
                f"{avg_return:.2f}%",
                f"{win_rate:.1f}%",
                f"{max_profit:.2f}%",
                f"{max_loss:.2f}%",
                f"{std_return:.2f}%"
            ]
        }
        stats_df = pd.DataFrame(detailed_stats)
        stats_df.to_csv(os.path.join(OUTPUT_DIR, 'detailed_statistics.csv'), index=False)
        print(f'💾 詳細統計を "{OUTPUT_DIR}/detailed_statistics.csv" に保存しました')
        
        # 銘柄別統計をCSVに保存
        symbol_stats.to_csv(os.path.join(OUTPUT_DIR, 'symbol_statistics.csv'))
        print(f'💾 銘柄別統計を "{OUTPUT_DIR}/symbol_statistics.csv" に保存しました')
        
        # 取引理由別統計をCSVに保存
        reason_stats.to_csv(os.path.join(OUTPUT_DIR, 'reason_statistics.csv'))
        print(f'💾 取引理由別統計を "{OUTPUT_DIR}/reason_statistics.csv" に保存しました')
    else:
        print(f'\n{"="*60}')
        print(f'📈 取引履歴')
        print(f'{"="*60}')
        print(f'❌ 取引が実行されませんでした')
        print(f'💡 戦略の条件を調整するか、より長い期間でテストしてみてください')
    
    return {
        'initial_value': initial_value,
        'final_value': final_value,
        'total_return': total_return,
        'profit': profit,
        'symbols_processed': len(symbols),
        'total_trades': len(all_trades) if all_trades else 0,
        'total_time': total_time
    }

def run_single_stock_backtest(symbol, start_date='2021-01-01', end_date='2023-01-01', initial_cash=100000, show_plot=True):
    """単一銘柄のバックテストを実行"""
    cerebro = CompatibleCerebro()
    cerebro.addstrategy(RSIGapStrategy)
    
    print(f"\n{symbol}のデータ取得中...")
    
    try:
        # データ取得
        df = yf.download(symbol, start=start_date, end=end_date, auto_adjust=False, progress=False)
        if df.empty or len(df) < 50:
            print(f"✗ データが取得できませんでした または データが不足しています ({len(df)}日)")
            return None
        
        df.dropna(inplace=True)
        if len(df) < 50:
            print(f"✗ データが不足しています ({len(df)}日)")
            return None
        
        # 必要なカラムだけ抽出し、カラム名を明示的にリネーム
        df = df[['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']]
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close']
        
        # データをCerebroに追加
        data = bt.feeds.PandasData(dataname=df, name=symbol)
        cerebro.adddata(data)
        
        print(f"✓ データ追加完了 ({len(df)}日)")
        
    except Exception as e:
        print(f"✗ エラー: {e}")
        return None
    
    # バックテスト実行
    print(f"\nバックテスト実行中...")
    cerebro.broker.setcash(initial_cash)
    initial_value = cerebro.broker.getvalue()
    print(f'初期ポートフォリオ価値: ${initial_value:,.2f}')
    
    results = cerebro.run()
    final_value = cerebro.broker.getvalue()
    
    # 結果の集計
    total_return = (final_value - initial_value) / initial_value * 100
    profit = final_value - initial_value
    
    print(f'\n{"="*60}')
    print(f'📊 バックテスト結果サマリー ({symbol})')
    print(f'{"="*60}')
    print(f'銘柄: {symbol}')
    print(f'期間: {start_date} から {end_date}')
    print(f'初期ポートフォリオ価値: ${initial_value:,.2f}')
    print(f'最終ポートフォリオ価値: ${final_value:,.2f}')
    print(f'{"-"*60}')
    
    # 利益の有無を明確に表示
    if profit > 0:
        print(f'💰 利益: +${profit:,.2f} (+{total_return:.2f}%)')
        print(f'✅ 戦略は利益を上げました！')
    elif profit < 0:
        print(f'💸 損失: -${abs(profit):,.2f} ({total_return:.2f}%)')
        print(f'❌ 戦略は損失を出しました')
    else:
        print(f'⚖️  損益: $0.00 (0.00%)')
        print(f'➖ 戦略は損益ゼロでした')
    
    print(f'{"="*60}')
    
    # GUIで結果を表示
    if show_plot:
        print(f'\nチャートを表示中...')
        try:
            # チャート表示設定
            cerebro.plot(style='candlestick', 
                        barup='green', 
                        bardown='red', 
                        volume=True, 
                        figsize=(15, 10),
                        dpi=100)
            plt.show()
        except Exception as e:
            print(f"チャート表示エラー: {e}")
            print("代替方法でチャート表示を試行中...")
            try:
                # 代替方法：よりシンプルな設定
                cerebro.plot(figsize=(15, 10))
                plt.show()
            except Exception as e2:
                print(f"代替チャート表示も失敗: {e2}")
                print("最終手段：最もシンプルなチャート表示を試行...")
                try:
                    # 最終手段：最もシンプルな設定
                    cerebro.plot()
                    plt.show()
                except Exception as e3:
                    print(f"チャート表示に失敗しました: {e3}")
                    print("結果はCSVファイルで確認してください")
                    import traceback
                    print("詳細エラー:")
                    print(traceback.format_exc())
    else:
        print(f'\nチャート表示は無効化されています')
    
    # 取引履歴を表示
    all_trades = []
    for strategy in results:
        if hasattr(strategy, 'trades'):
            all_trades.extend(strategy.trades)
    
    if all_trades:
        print(f'\n{"="*60}')
        print(f'📈 取引履歴詳細 ({symbol})')
        print(f'{"="*60}')
        print(f'総取引数: {len(all_trades)}')
        
        # 取引結果をDataFrameに変換
        trades_df = pd.DataFrame(all_trades)
        
        # 統計情報
        print(f'\n📊 統計情報')
        print(f'{"-"*40}')
        avg_return = trades_df["return"].mean()
        win_rate = (trades_df["return"] > 0).mean() * 100
        max_profit = trades_df["return"].max()
        max_loss = trades_df["return"].min()
        std_return = trades_df["return"].std()
        
        print(f'平均リターン: {avg_return:.2f}%')
        print(f'勝率: {win_rate:.1f}%')
        print(f'最大利益: {max_profit:.2f}%')
        print(f'最大損失: {max_loss:.2f}%')
        print(f'標準偏差: {std_return:.2f}%')
        
        # 勝率の評価
        if win_rate >= 60:
            print(f'🎯 勝率評価: 優秀 ({win_rate:.1f}%)')
        elif win_rate >= 50:
            print(f'👍 勝率評価: 良好 ({win_rate:.1f}%)')
        elif win_rate >= 40:
            print(f'⚠️  勝率評価: 普通 ({win_rate:.1f}%)')
        else:
            print(f'❌ 勝率評価: 低い ({win_rate:.1f}%)')
        
        # 取引理由別の統計
        print(f'\n📋 取引理由別統計')
        print(f'{"-"*40}')
        reason_stats = trades_df.groupby('exit_reason')['return'].agg(['count', 'mean', 'sum'])
        print(reason_stats)
        
        # 取引履歴をCSVに保存
        trades_df.to_csv(os.path.join(OUTPUT_DIR, f'trades_history_{symbol}.csv'), index=False)
        print(f'\n💾 取引履歴を "{OUTPUT_DIR}/trades_history_{symbol}.csv" に保存しました')
        
        # 詳細統計をCSVに保存
        detailed_stats = {
            'metric': ['総取引数', '平均リターン', '勝率', '最大利益', '最大損失', '標準偏差'],
            'value': [
                len(all_trades),
                f"{avg_return:.2f}%",
                f"{win_rate:.1f}%",
                f"{max_profit:.2f}%",
                f"{max_loss:.2f}%",
                f"{std_return:.2f}%"
            ]
        }
        stats_df = pd.DataFrame(detailed_stats)
        stats_df.to_csv(os.path.join(OUTPUT_DIR, f'detailed_statistics_{symbol}.csv'), index=False)
        print(f'💾 詳細統計を "{OUTPUT_DIR}/detailed_statistics_{symbol}.csv" に保存しました')
        
        # 取引理由別統計をCSVに保存
        reason_stats.to_csv(os.path.join(OUTPUT_DIR, f'reason_statistics_{symbol}.csv'))
        print(f'💾 取引理由別統計を "{OUTPUT_DIR}/reason_statistics_{symbol}.csv" に保存しました')
    else:
        print(f'\n{"="*60}')
        print(f'📈 取引履歴 ({symbol})')
        print(f'{"="*60}')
        print(f'❌ 取引が実行されませんでした')
        print(f'💡 戦略の条件を調整するか、より長い期間でテストしてみてください')
    
    return {
        'symbol': symbol,
        'initial_value': initial_value,
        'final_value': final_value,
        'total_return': total_return,
        'profit': profit,
        'total_trades': len(all_trades) if all_trades else 0
    }

if __name__ == '__main__':
    # 出力ディレクトリの作成
    ensure_output_dir()
    
    # コマンドライン引数の解析
    parser = argparse.ArgumentParser(description='RSI Gap Strategy Backtesting')
    parser.add_argument('--no-plot', action='store_true', help='チャート表示を無効化')
    parser.add_argument('--workers', type=int, default=20, help='並列処理数（デフォルト: 20）')
    parser.add_argument('--single', type=str, help='単一銘柄でバックテスト（例: AAPL）')
    parser.add_argument('--start-date', type=str, default='2021-01-01', help='開始日（デフォルト: 2021-01-01）')
    parser.add_argument('--end-date', type=str, default='2023-01-01', help='終了日（デフォルト: 2023-01-01）')
    args = parser.parse_args()
    
    # 並列処理の設定
    MAX_WORKERS = args.workers  # 並列度（CPUコア数に応じて調整）
    
    if args.single:
        # 単一銘柄バックテスト
        print(f"単一銘柄バックテスト: {args.single}")
        print(f"期間: {args.start_date} から {args.end_date}")
        
        # 単一銘柄用のバックテスト実行
        results = run_single_stock_backtest(
            args.single, 
            start_date=args.start_date, 
            end_date=args.end_date, 
            show_plot=not args.no_plot
        )
        
        if results:
            # 結果をCSVに保存
            results_df = pd.DataFrame([results])
            results_df.to_csv(os.path.join(OUTPUT_DIR, f'single_stock_{args.single}_results.csv'), index=False)
            print(f"\n💾 結果を '{OUTPUT_DIR}/single_stock_{args.single}_results.csv' に保存しました")
            
            # 最終結果の要約
            print(f"\n{"="*60}")
            print(f'📋 最終結果要約')
            print(f'{"="*60}')
            if results['profit'] > 0:
                print(f'✅ {args.single}: 利益 +${results["profit"]:,.2f} (+{results["total_return"]:.2f}%)')
            elif results['profit'] < 0:
                print(f'❌ {args.single}: 損失 -${abs(results["profit"]):,.2f} ({results["total_return"]:.2f}%)')
            else:
                print(f'➖ {args.single}: 損益ゼロ')
            print(f'取引回数: {results["total_trades"]}回')
        else:
            print(f"\n❌ バックテストの実行に失敗しました")
        
    else:
        # 複数銘柄バックテスト（既存の処理）
        # S&P500銘柄を取得
        print("S&P500銘柄リストを取得中...")
        all_symbols = get_sp500_symbols()
        print(f"取得銘柄数: {len(all_symbols)}")
        
        # 出来高フィルタリング（平均出来高100万株以上）
        high_volume_symbols = filter_high_volume_stocks_parallel(
            all_symbols, 
            min_avg_volume=1000000, 
            max_workers=MAX_WORKERS
        )
        
        if not high_volume_symbols:
            print("条件を満たす銘柄が見つかりませんでした。")
            exit(1)
        
        # テスト用に最初の20銘柄のみ使用（処理時間短縮のため）
        test_symbols = high_volume_symbols[:20]
        print(f"\nテスト対象銘柄: {test_symbols}")
        
        # バックテスト実行
        results = run_multi_stock_backtest_parallel(test_symbols, max_workers=MAX_WORKERS, show_plot=not args.no_plot)
        
        # 結果をCSVに保存
        results_df = pd.DataFrame([results])
        results_df.to_csv(os.path.join(OUTPUT_DIR, 'multi_stock_backtest_results.csv'), index=False)
        print(f"\n💾 結果を '{OUTPUT_DIR}/multi_stock_backtest_results.csv' に保存しました")
        
        # 最終結果の要約
        print(f"\n{"="*60}")
        print(f'📋 最終結果要約')
        print(f'{"="*60}')
        if results['profit'] > 0:
            print(f'✅ 複数銘柄戦略: 利益 +${results["profit"]:,.2f} (+{results["total_return"]:.2f}%)')
        elif results['profit'] < 0:
            print(f'❌ 複数銘柄戦略: 損失 -${abs(results["profit"]):,.2f} ({results["total_return"]:.2f}%)')
        else:
            print(f'➖ 複数銘柄戦略: 損益ゼロ')
        print(f'処理銘柄数: {results["symbols_processed"]}銘柄')
        print(f'総取引回数: {results["total_trades"]}回')
        print(f'処理時間: {results["total_time"]:.1f}秒') 