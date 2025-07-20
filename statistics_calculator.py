"""
統計計算・結果表示関連の処理
"""
import numpy as np
import pandas as pd
import os

import matplotlib.pyplot as plt

from file_manager import SINGLE_STOCK_DIR


def display_single_stock_results(symbol, start_date, end_date, initial_value, final_value, results, show_plot, cerebro=None):
    """単一銘柄の結果を表示・保存"""
    # 結果の集計
    total_return = (final_value - initial_value) / initial_value * 100
    profit = final_value - initial_value
    
    # 戦略からの取引履歴を取得
    strategy = results[0]
    trades = strategy.trades if hasattr(strategy, 'trades') else []
    
    # 未決済ポジションの確認
    open_positions = 0
    for data in strategy.datas:
        position = strategy.getposition(data)
        if position.size != 0:
            open_positions += 1
    
    print(f'\n{"="*60}')
    print(f'📊 バックテスト結果サマリー ({symbol})')
    print(f'{"="*60}')
    print(f'銘柄: {symbol}')
    print(f'期間: {start_date} から {end_date}')
    print(f'初期ポートフォリオ価値: ${initial_value:,.2f}')
    print(f'最終ポートフォリオ価値: ${final_value:,.2f}')
    print(f'総取引回数: {len(trades)}')
    print(f'未決済ポジション: {open_positions}')
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
    
    # チャート表示（WSL2環境対応）
    if show_plot:
        try:
            display_chart(results, cerebro)
        except KeyboardInterrupt:
            print("\n⚠️ チャート表示が中断されました")
        except Exception as e:
            print(f"\n⚠️ チャート表示エラー: {e}")
            print("GUI環境が利用できないため、チャート表示をスキップします")
    else:
        print(f'\nチャート表示は無効化されています')
    
    # 取引履歴の処理
    display_and_save_trade_history(symbol, trades, results)
    
    return {
        'symbol': symbol,
        'initial_value': initial_value,
        'final_value': final_value,
        'total_return': total_return,
        'profit': profit,
        'total_trades': len(trades) if trades else 0
    }


def display_chart(results, cerebro=None):
    """チャート表示"""
    print(f'\nチャートを表示中...')
    try:
        # 元の動作していたコードと同じ方法
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


def display_and_save_trade_history(symbol, trades, results):
    """取引履歴を表示・保存"""
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
        trades_df.to_csv(os.path.join(SINGLE_STOCK_DIR, f'trades_history_{symbol}.csv'), index=False)
        print(f'\n💾 取引履歴を "{SINGLE_STOCK_DIR}/trades_history_{symbol}.csv" に保存しました')
        
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
        stats_df.to_csv(os.path.join(SINGLE_STOCK_DIR, f'detailed_statistics_{symbol}.csv'), index=False)
        print(f'💾 詳細統計を "{SINGLE_STOCK_DIR}/detailed_statistics_{symbol}.csv" に保存しました')
        
        # 取引理由別統計をCSVに保存
        reason_stats.to_csv(os.path.join(SINGLE_STOCK_DIR, f'reason_statistics_{symbol}.csv'))
        print(f'💾 取引理由別統計を "{SINGLE_STOCK_DIR}/reason_statistics_{symbol}.csv" に保存しました')
    else:
        print(f'\n{"="*60}')
        print(f'📈 取引履歴 ({symbol})')
        print(f'{"="*60}')
        print(f'❌ 取引が実行されませんでした')
        print(f'💡 戦略の条件を調整するか、より長い期間でテストしてみてください')


def display_results(initial_cash, final_cash, total_return, profit, num_symbols, num_trades, elapsed_time, all_trades, successful_symbols, failed_symbols, show_plot):
    """複数銘柄バックテストの結果を表示"""
    print("\n==== バックテスト結果サマリー ====")
    print(f"初期資産: ${initial_cash:,.2f}")
    print(f"最終資産: ${final_cash:,.2f}")
    print(f"総リターン: {total_return:.2%}")
    print(f"総損益: ${profit:,.2f}")
    print(f"テスト銘柄数: {num_symbols}")
    print(f"総取引回数: {num_trades}")
    print(f"処理時間: {elapsed_time:.1f}秒")
    print(f"成功した銘柄数: {len(successful_symbols)}")
    if successful_symbols:
        print(f"成功した銘柄例: {successful_symbols[:10]} ... (他{max(0, len(successful_symbols)-10)}件)")
    else:
        print("成功した銘柄なし")
    print(f"失敗した銘柄数: {len(failed_symbols)}")
    if failed_symbols:
        print(f"失敗した銘柄例: {failed_symbols[:10]} ... (他{max(0, len(failed_symbols)-10)}件)")
    else:
        print("失敗した銘柄なし")
    
    # 詳細統計情報を計算・表示
    if all_trades and len(all_trades) > 0:
        print("\n==== 詳細統計情報 ====")
        calculate_and_display_statistics(all_trades, successful_symbols)


def calculate_and_display_statistics(all_trades, successful_symbols):
    """取引データから詳細統計を計算・表示"""
    try:
        # 取引データからリターンを抽出
        returns = []
        profits = []
        symbols_performance = {}
        
        for trade in all_trades:
            if 'return' in trade and trade['return'] is not None and not pd.isna(trade['return']):
                returns.append(float(trade['return']))
            
            if 'pnl' in trade and trade['pnl'] is not None and not pd.isna(trade['pnl']):
                profits.append(float(trade['pnl']))
            
            # 銘柄別パフォーマンス集計
            symbol = trade.get('symbol', 'Unknown')
            if symbol not in symbols_performance:
                symbols_performance[symbol] = {'trades': 0, 'total_pnl': 0, 'returns': []}
            
            symbols_performance[symbol]['trades'] += 1
            if 'pnl' in trade and trade['pnl'] is not None:
                symbols_performance[symbol]['total_pnl'] += float(trade['pnl'])
            if 'return' in trade and trade['return'] is not None and not pd.isna(trade['return']):
                symbols_performance[symbol]['returns'].append(float(trade['return']))
        
        # 基本統計
        if returns:
            returns_array = np.array(returns)
            print(f"📊 リターン統計:")
            print(f"  - 平均リターン: {np.mean(returns_array):.2%}")
            print(f"  - リターン標準偏差: {np.std(returns_array):.2%}")
            print(f"  - 最大リターン: {np.max(returns_array):.2%}")
            print(f"  - 最小リターン: {np.min(returns_array):.2%}")
            print(f"  - リターン中央値: {np.median(returns_array):.2%}")
        
        if profits:
            profits_array = np.array(profits)
            winning_trades = profits_array[profits_array > 0]
            losing_trades = profits_array[profits_array < 0]
            
            print(f"\n💰 損益統計:")
            print(f"  - 平均損益: ${np.mean(profits_array):.2f}")
            print(f"  - 損益標準偏差: ${np.std(profits_array):.2f}")
            print(f"  - 最大利益: ${np.max(profits_array):.2f}")
            print(f"  - 最大損失: ${np.min(profits_array):.2f}")
            print(f"  - 勝率: {len(winning_trades)/len(profits_array):.1%}")
            
            if len(winning_trades) > 0:
                print(f"  - 平均利益: ${np.mean(winning_trades):.2f}")
            if len(losing_trades) > 0:
                print(f"  - 平均損失: ${np.mean(losing_trades):.2f}")
        
        # 銘柄別統計
        if symbols_performance:
            symbol_pnls = []
            symbol_trade_counts = []
            
            for symbol, perf in symbols_performance.items():
                if perf['trades'] > 0:
                    symbol_pnls.append(perf['total_pnl'])
                    symbol_trade_counts.append(perf['trades'])
            
            if symbol_pnls:
                symbol_pnls_array = np.array(symbol_pnls)
                print(f"\n🏢 銘柄別統計:")
                print(f"  - 銘柄別損益平均: ${np.mean(symbol_pnls_array):.2f}")
                print(f"  - 銘柄別損益標準偏差: ${np.std(symbol_pnls_array):.2f}")
                print(f"  - 最高パフォーマンス銘柄: ${np.max(symbol_pnls_array):.2f}")
                print(f"  - 最低パフォーマンス銘柄: ${np.min(symbol_pnls_array):.2f}")
                
                # 変動係数の計算
                if np.mean(symbol_pnls_array) != 0:
                    cv = np.std(symbol_pnls_array) / abs(np.mean(symbol_pnls_array))
                    print(f"  - 銘柄間変動係数: {cv:.2f}")
                    
                    # 安定性評価
                    if cv < 0.5:
                        print(f"  - 安定性評価: ✅ 非常に安定 (CV < 0.5)")
                    elif cv < 1.0:
                        print(f"  - 安定性評価: 🟡 やや安定 (CV < 1.0)")
                    elif cv < 2.0:
                        print(f"  - 安定性評価: 🟠 不安定 (CV < 2.0)")
                    else:
                        print(f"  - 安定性評価: ❌ 非常に不安定 (CV >= 2.0)")
            
            # 取引回数統計
            if symbol_trade_counts:
                print(f"  - 平均取引回数/銘柄: {np.mean(symbol_trade_counts):.1f}")
                print(f"  - 取引回数標準偏差: {np.std(symbol_trade_counts):.1f}")
        
        # 外れ値分析
        if symbol_pnls and len(symbol_pnls) > 4:
            Q1 = np.percentile(symbol_pnls, 25)
            Q3 = np.percentile(symbol_pnls, 75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            outliers = [pnl for pnl in symbol_pnls if pnl < lower_bound or pnl > upper_bound]
            if outliers:
                print(f"\n⚠️ 外れ値分析:")
                print(f"  - 外れ値数: {len(outliers)}/{len(symbol_pnls)} 銘柄")
                print(f"  - 外れ値率: {len(outliers)/len(symbol_pnls):.1%}")
    
    except Exception as e:
        print(f"統計計算中にエラーが発生しました: {e}")
        print("基本的な取引統計のみ表示されています")