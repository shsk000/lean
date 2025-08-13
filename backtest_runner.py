"""
バックテスト実行関連の処理
"""
import backtrader as bt
import time
import importlib
import os
from data_loader import LocalDataLoader
from strategies.hybrid_momentum_reversion_strategy import HybridMomentumReversionStrategy
from strategies.us_short_term import USShortTermDesigner
from statistics_calculator import display_results
from file_manager import ensure_output_dir


def get_strategy_class(strategy_name):
    """戦略名から戦略クラスを動的に読み込む"""
    try:
        # strategies/配下のファイル名から戦略を検索
        strategy_module = importlib.import_module(f'strategies.{strategy_name}')
        
        # モジュール内の戦略クラスを検索
        for attr_name in dir(strategy_module):
            attr = getattr(strategy_module, attr_name)
            if (isinstance(attr, type) and 
                issubclass(attr, bt.Strategy) and 
                attr != bt.Strategy):
                return attr
        
        raise ValueError(f"Strategy class not found in {strategy_name}")
        
    except ImportError:
        raise ValueError(f"Strategy module '{strategy_name}' not found")


def list_available_strategies():
    """利用可能な戦略一覧を取得"""
    strategies_dir = 'strategies'
    available_strategies = []
    
    for filename in os.listdir(strategies_dir):
        if filename.endswith('.py') and not filename.startswith('__'):
            strategy_name = filename[:-3]  # .py拡張子を削除
            if strategy_name != '__init__':
                available_strategies.append(strategy_name)
    
    return available_strategies


class CompatibleCerebro(bt.Cerebro):
    """backtraderの互換性問題を解決するCerebroクラス"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 必要な属性を初期化
        if not hasattr(self, '_exactbars'):
            self._exactbars = 0
        if not hasattr(self, '_plotting'):
            self._plotting = True


def run_multi_stock_backtest_parallel(symbols, start_date='2012-01-01', end_date='2022-12-31', initial_cash=100000, max_workers=10, show_plot=True, strategy_name='hybrid_momentum_reversion_strategy'):
    """各銘柄を個別に独立したキャッシュで実行"""
    start_time = time.time()

    # 戦略クラスを動的に読み込み
    try:
        strategy_class = get_strategy_class(strategy_name)
        print(f"使用戦略: {strategy_class.__name__}")
    except ValueError as e:
        print(f"戦略読み込みエラー: {e}")
        available = list_available_strategies()
        print(f"利用可能な戦略: {available}")
        return {
            'initial_value': 0,
            'final_value': 0,
            'total_return': 0,
            'profit': 0,
            'symbols_processed': 0,
            'total_trades': 0,
            'total_time': 0,
            'trades': []
        }

    # 出力ディレクトリの作成
    ensure_output_dir()

    print(f"各銘柄を個別に独立実行中...")
    print(f"期間: {start_date} から {end_date}")
    print(f"処理銘柄数: {len(symbols)}")
    print(f"各銘柄の初期資金: ${initial_cash:,.2f}")

    # 各銘柄の結果を格納
    all_results = []
    all_trades = []
    successful_symbols = []
    failed_symbols = []

    for i, symbol in enumerate(symbols):
        print(f"\n[{i+1}/{len(symbols)}] {symbol} 処理中...")

        try:
            # 各銘柄ごとに独立したcerebro
            cerebro = CompatibleCerebro()
            cerebro.broker.setcash(initial_cash)
            cerebro.broker.setcommission(commission=0.001)  # 0.1%の手数料

            # データ読み込み
            loader = LocalDataLoader()
            loader.validate_date_range(start_date, end_date)
            data_feed = loader.create_backtrader_data(symbol, start_date, end_date)
            cerebro.adddata(data_feed)

            # 戦略追加
            cerebro.addstrategy(strategy_class)

            # バックテスト実行
            results = cerebro.run()
            final_value = cerebro.broker.getvalue()

            # 結果集計
            profit = final_value - initial_cash
            total_return = profit / initial_cash

            # 取引履歴収集
            strategy = results[0]
            symbol_trades = []
            if hasattr(strategy, 'trades'):
                symbol_trades = strategy.trades

            # 結果記録
            result = {
                'symbol': symbol,
                'initial_value': initial_cash,
                'final_value': final_value,
                'profit': profit,
                'total_return': total_return,
                'total_trades': len(symbol_trades)
            }

            all_results.append(result)
            all_trades.extend(symbol_trades)
            successful_symbols.append(symbol)

            print(f"✓ {symbol}: 利益 ${profit:,.2f} ({total_return:.2%}), 取引回数: {len(symbol_trades)}")

        except Exception as e:
            print(f"✗ {symbol}: エラー - {e}")
            failed_symbols.append(symbol)

    print(f"\n処理完了: {time.time() - start_time:.1f}秒")
    print(f"成功: {len(successful_symbols)}銘柄, 失敗: {len(failed_symbols)}銘柄")

    if len(successful_symbols) == 0:
        print("❌ バックテスト可能な銘柄がありません")
        return {
            'initial_value': initial_cash,
            'final_value': initial_cash,
            'total_return': 0,
            'profit': 0,
            'symbols_processed': 0,
            'total_trades': 0,
            'total_time': time.time() - start_time
        }

    # 個別実行結果の集計
    total_profit = sum(r['profit'] for r in all_results)
    total_initial = initial_cash * len(successful_symbols)
    total_final = total_initial + total_profit
    total_return = total_profit / total_initial if total_initial > 0 else 0

    # 結果の表示と保存
    display_results(total_initial, total_final, total_return, total_profit, len(symbols), len(all_trades), time.time() - start_time, all_trades, successful_symbols, failed_symbols, show_plot)

    return {
        'initial_value': total_initial,
        'final_value': total_final,
        'total_return': total_return,
        'profit': total_profit,
        'symbols_processed': len(symbols),
        'total_trades': len(all_trades) if all_trades else 0,
        'total_time': time.time() - start_time,
        'trades': all_trades if all_trades else []
    }


def run_single_stock_backtest(symbol, start_date='2012-01-01', end_date='2022-12-31', initial_cash=100000, show_plot=True, strategy_name='hybrid_momentum_reversion_strategy'):
    """単一銘柄のバックテストを実行"""
    from statistics_calculator import display_single_stock_results

    # 戦略クラスを動的に読み込み
    try:
        strategy_class = get_strategy_class(strategy_name)
        print(f"使用戦略: {strategy_class.__name__}")
    except ValueError as e:
        print(f"戦略読み込みエラー: {e}")
        available = list_available_strategies()
        print(f"利用可能な戦略: {available}")
        return None

    cerebro = CompatibleCerebro()

    cerebro.addstrategy(strategy_class)

    print(f"\n{symbol}のデータ取得中...")

    try:
        # ローカルデータローダーを使用
        loader = LocalDataLoader()
        loader.validate_date_range(start_date, end_date)

        # データをCerebroに追加（LocalDataLoaderが整合性チェック済み）
        data_feed = loader.create_backtrader_data(symbol, start_date, end_date)
        cerebro.adddata(data_feed)

        print(f"✓ ローカルデータ読み込み完了")

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

    # 結果の処理
    result = display_single_stock_results(symbol, start_date, end_date, initial_value, final_value, results, show_plot, cerebro)

    return result