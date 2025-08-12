"""
汎用バックテスト実行ツール (Backtest Runner)
========================================

このスクリプトは、任意のトレーディング戦略クラス（strategies/配下）を差し替えて、
複数銘柄・単一銘柄のバックテストを柔軟に実行できる汎用基盤です。

デフォルトではHybridMomentumReversionStrategyを使用しますが、
strategies/ディレクトリに新しい戦略クラスを追加し、差し替えも容易です。

戦略の変更方法:
--strategy STRATEGY_NAME で任意の戦略を指定できます。
例: --strategy rsi_gap_strategy
    --strategy hybrid_momentum_reversion_strategy

実行方法:
--------

1. 単一銘柄バックテスト（推奨 - GUI表示が確実）
   python bt_runner.py --single AAPL
   python bt_runner.py --single MSFT --start-date 2020-01-01 --end-date 2022-12-31
   python bt_runner.py --single TSLA --no-plot  # チャート表示なし

2. 複数銘柄バックテスト（S&P500銘柄から自動選択）
   python bt_runner.py
   python bt_runner.py --workers 30  # 並列処理数を変更
   python bt_runner.py --no-plot     # チャート表示なし

コマンドライン引数:
------------------
--single SYMBOL     : 単一銘柄でバックテスト（例: AAPL, MSFT, TSLA）
--start-date DATE   : 開始日（デフォルト: 2012-01-01）
--end-date DATE     : 終了日（デフォルト: 2022-12-31）
--no-plot           : チャート表示を無効化
--workers N         : 並列処理数（デフォルト: 20）
--limit             : テストする最大銘柄数（例: 20）

戦略パラメータ例（HybridMomentumReversionStrategy）:
------------------------------
- トレンド期間: 200日移動平均
- モメンタム期間: 60日
- リバランス頻度: 20日毎
- エントリー閾値: MA上1%
- エグジット閾値: MA下1%

出力ファイル:
------------
単一銘柄の場合:
- output/single_stocks/single_stock_SYMBOL_results.csv    : 基本結果
- output/single_stocks/trades_history_SYMBOL.csv          : 取引履歴
- output/single_stocks/detailed_statistics_SYMBOL.csv     : 詳細統計
- output/single_stocks/reason_statistics_SYMBOL.csv       : 取引理由別統計

複数銘柄の場合:
- output/multi_stocks/multi_stock_backtest_results.csv   : 基本結果
- output/multi_stocks/multi_stock_trades_history.csv     : 取引履歴
- output/multi_stocks/detailed_statistics.csv            : 詳細統計
- output/multi_stocks/symbol_performance.csv             : 銘柄別統計

注意事項:
--------
- GUI環境が必要です（WSL2の場合はX11設定が必要な場合があります）
- 初回実行時はS&P500銘柄リストの取得に時間がかかります
- 並列処理により大幅に高速化されています
"""

import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt
from data_utils import get_local_symbols, get_high_volume_symbols_from_local_data
from backtest_runner import run_single_stock_backtest, run_multi_stock_backtest_parallel
from file_manager import ensure_output_dir, save_detailed_statistics_to_csv, SINGLE_STOCK_DIR, MULTI_STOCK_DIR

# 日本語フォント設定（Linux環境用）
plt.rcParams['font.family'] = 'DejaVu Sans'
plt.rcParams['axes.unicode_minus'] = False


def main():
    """メイン処理"""
    parser = argparse.ArgumentParser(description='Hybrid Momentum Reversion Strategy Backtesting')
    parser.add_argument('--no-plot', action='store_true', help='チャート表示を無効化')
    parser.add_argument('--workers', type=int, default=20, help='並列処理数（デフォルト: 20）')
    parser.add_argument('--single', type=str, help='単一銘柄でバックテスト（例: AAPL）')
    parser.add_argument('--start-date', type=str, default='2012-01-01', help='開始日（デフォルト: 2012-01-01）')
    parser.add_argument('--end-date', type=str, default='2022-12-31', help='終了日（デフォルト: 2022-12-31）')
    parser.add_argument('--limit', type=int, default=None, help='テストする最大銘柄数（例: 20）')
    parser.add_argument('--strategy', type=str, default='hybrid_momentum_reversion_strategy', 
                       help='使用する戦略名（例: rsi_gap_strategy, hybrid_momentum_reversion_strategy）')
    args = parser.parse_args()
    
    # 出力ディレクトリの確保
    ensure_output_dir()
    
    # 並列処理の設定
    MAX_WORKERS = args.workers
    
    if args.single:
        # 単一銘柄バックテスト
        print(f"単一銘柄バックテスト: {args.single}")
        print(f"期間: {args.start_date} から {args.end_date}")
        
        results = run_single_stock_backtest(
            args.single, 
            start_date=args.start_date, 
            end_date=args.end_date, 
            show_plot=not args.no_plot,
            strategy_name=args.strategy
        )
        
        if results:
            # 結果をCSVに保存
            results_df = pd.DataFrame([results])
            results_df.to_csv(os.path.join(SINGLE_STOCK_DIR, f'single_stock_{args.single}_results.csv'), index=False)
            print(f"\\n💾 結果を '{SINGLE_STOCK_DIR}/single_stock_{args.single}_results.csv' に保存しました")
            
            # 最終結果の要約
            print(f"\\n{'='*60}")
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
            print(f"\\n❌ バックテストの実行に失敗しました")
        
    else:
        # 複数銘柄バックテスト
        print("S&P500銘柄リストを取得中...")
        all_symbols = get_local_symbols()
        print(f"取得銘柄数: {len(all_symbols)}")
        
        # ローカルデータから高出来高銘柄を取得
        print("[INFO] ローカルデータから高出来高銘柄を使用します")
        
        high_volume_symbols = get_high_volume_symbols_from_local_data(max_symbols=args.limit)
        
        if not high_volume_symbols:
            print("条件を満たす銘柄が見つかりませんでした。")
            exit(1)
        
        # 全銘柄でバックテストを実行
        test_symbols = high_volume_symbols
        print(f"\\nテスト対象銘柄数: {len(test_symbols)}銘柄")
        print(f"最初の10銘柄: {test_symbols[:10]}")
        if len(test_symbols) > 10:
            print(f"最後の10銘柄: {test_symbols[-10:]}")

        # limitを適用
        if args.limit is not None:
            print(f"[INFO] --limit指定により、最終的な{args.limit}件のみテストします")
            test_symbols = test_symbols[:args.limit]
            print(f"[INFO] limit適用後のテスト対象銘柄数: {len(test_symbols)}")

        # バックテスト実行
        results = run_multi_stock_backtest_parallel(
            test_symbols, 
            start_date=args.start_date,
            end_date=args.end_date,
            max_workers=MAX_WORKERS, 
            show_plot=not args.no_plot,
            strategy_name=args.strategy
        )
        
        # 結果をCSVに保存
        results_df = pd.DataFrame([results])
        results_df.to_csv(os.path.join(MULTI_STOCK_DIR, 'multi_stock_backtest_results.csv'), index=False)
        print(f"\\n💾 結果を '{MULTI_STOCK_DIR}/multi_stock_backtest_results.csv' に保存しました")
        
        # 取引履歴の保存
        if 'trades' in results and results['trades']:
            trades_df = pd.DataFrame(results['trades'])
            trades_df.to_csv(os.path.join(MULTI_STOCK_DIR, 'multi_stock_trades_history.csv'), index=False)
            print(f"💾 取引履歴を '{MULTI_STOCK_DIR}/multi_stock_trades_history.csv' に保存しました")
            
            # 詳細統計情報をCSVとして保存
            save_detailed_statistics_to_csv(results['trades'], test_symbols)
        else:
            # 完了した取引がない場合でも統計情報を保存
            print(f"⚠️ 完了した取引がありません（未決済ポジションが存在する可能性があります）")
            # 空の取引履歴でCSVファイルを作成
            empty_trades_df = pd.DataFrame(columns=['entry_date', 'exit_date', 'symbol', 'entry_price', 'exit_price', 'size', 'pnl', 'return', 'exit_reason'])
            empty_trades_df.to_csv(os.path.join(MULTI_STOCK_DIR, 'multi_stock_trades_history.csv'), index=False)
            print(f"💾 空の取引履歴を '{MULTI_STOCK_DIR}/multi_stock_trades_history.csv' に保存しました")
            
            # 空の統計情報を保存
            save_detailed_statistics_to_csv([], test_symbols)
        
        # 最終結果の要約
        print(f"\\n{'='*60}")
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


if __name__ == '__main__':
    main()