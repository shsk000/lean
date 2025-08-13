"""
ファイル保存関連の処理
"""
import os
import pandas as pd
import numpy as np

# 出力ディレクトリの設定
OUTPUT_DIR = 'output'
SINGLE_STOCK_DIR = 'output/single_stocks'
MULTI_STOCK_DIR = 'output/multi_stocks'


def ensure_output_dir():
    """出力ディレクトリが存在しない場合は作成"""
    for dir_path in [OUTPUT_DIR, SINGLE_STOCK_DIR, MULTI_STOCK_DIR]:
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
            print(f"出力ディレクトリ '{dir_path}' を作成しました")


def save_detailed_statistics_to_csv(all_trades, symbols):
    """詳細統計情報をCSVファイルに保存"""
    try:
        # 統計データを収集
        stats_data = {}
        
        # 取引がない場合の処理
        if not all_trades:
            print(f"[DEBUG] 取引データが空のため、デフォルト統計を作成します")
            # デフォルト統計値を設定
            stats_data = {
                'return_mean': 0, 'return_std': 0, 'return_max': 0, 'return_min': 0, 'return_median': 0,
                'profit_mean': 0, 'profit_std': 0, 'profit_max': 0, 'profit_min': 0,
                'win_rate': 0, 'avg_win': 0, 'avg_loss': 0,
                'symbol_pnl_mean': 0, 'symbol_pnl_std': 0, 'symbol_pnl_max': 0, 'symbol_pnl_min': 0,
                'coefficient_of_variation': 0, 'avg_trades_per_symbol': 0, 'trades_std': 0,
                'outlier_count': 0, 'outlier_rate': 0
            }
            
            # 空の銘柄別データを作成
            symbol_details = []
            for symbol in symbols:
                symbol_details.append({
                    'symbol': symbol,
                    'total_pnl': 0,
                    'trade_count': 0,
                    'avg_return': 0
                })
                
            # CSVファイルに保存（取引がない場合）
            _save_statistics_files(stats_data, symbol_details)
            return  # 早期リターンで複雑な統計計算をスキップ
        else:
            # 取引がある場合の統計計算
            stats_data, symbol_details = _calculate_statistics(all_trades, symbols)
            _save_statistics_files(stats_data, symbol_details)
        
        # コンソールに統計サマリーを表示
        _display_statistics_summary(stats_data)
        
    except Exception as e:
        print(f"統計データのCSV保存中にエラー: {e}")


def _calculate_statistics(all_trades, symbols):
    """取引データから統計を計算"""
    # 基本統計
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
    
    stats_data = {}
    
    # リターン統計
    if returns:
        returns_array = np.array(returns)
        stats_data['return_mean'] = np.mean(returns_array)
        stats_data['return_std'] = np.std(returns_array)
        stats_data['return_max'] = np.max(returns_array)
        stats_data['return_min'] = np.min(returns_array)
        stats_data['return_median'] = np.median(returns_array)
    else:
        stats_data.update({
            'return_mean': 0, 'return_std': 0, 'return_max': 0, 
            'return_min': 0, 'return_median': 0
        })
    
    # 損益統計
    if profits:
        profits_array = np.array(profits)
        winning_trades = profits_array[profits_array > 0]
        losing_trades = profits_array[profits_array < 0]
        
        stats_data['profit_mean'] = np.mean(profits_array)
        stats_data['profit_std'] = np.std(profits_array)
        stats_data['profit_max'] = np.max(profits_array)
        stats_data['profit_min'] = np.min(profits_array)
        stats_data['win_rate'] = len(winning_trades)/len(profits_array) if len(profits_array) > 0 else 0
        stats_data['avg_win'] = np.mean(winning_trades) if len(winning_trades) > 0 else 0
        stats_data['avg_loss'] = np.mean(losing_trades) if len(losing_trades) > 0 else 0
    else:
        stats_data.update({
            'profit_mean': 0, 'profit_std': 0, 'profit_max': 0, 'profit_min': 0,
            'win_rate': 0, 'avg_win': 0, 'avg_loss': 0
        })
    
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
            stats_data['symbol_pnl_mean'] = np.mean(symbol_pnls_array)
            stats_data['symbol_pnl_std'] = np.std(symbol_pnls_array)
            stats_data['symbol_pnl_max'] = np.max(symbol_pnls_array)
            stats_data['symbol_pnl_min'] = np.min(symbol_pnls_array)
            
            # 変動係数
            if np.mean(symbol_pnls_array) != 0:
                stats_data['coefficient_of_variation'] = np.std(symbol_pnls_array) / abs(np.mean(symbol_pnls_array))
            else:
                stats_data['coefficient_of_variation'] = float('inf')
            
            # 取引回数統計
            stats_data['avg_trades_per_symbol'] = np.mean(symbol_trade_counts)
            stats_data['trades_std'] = np.std(symbol_trade_counts)
            
            # 外れ値分析
            if len(symbol_pnls) > 4:
                Q1 = np.percentile(symbol_pnls, 25)
                Q3 = np.percentile(symbol_pnls, 75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outliers = [pnl for pnl in symbol_pnls if pnl < lower_bound or pnl > upper_bound]
                stats_data['outlier_count'] = len(outliers)
                stats_data['outlier_rate'] = len(outliers)/len(symbol_pnls)
            else:
                stats_data['outlier_count'] = 0
                stats_data['outlier_rate'] = 0
        else:
            stats_data.update({
                'symbol_pnl_mean': 0, 'symbol_pnl_std': 0, 'symbol_pnl_max': 0, 
                'symbol_pnl_min': 0, 'coefficient_of_variation': 0,
                'avg_trades_per_symbol': 0, 'trades_std': 0,
                'outlier_count': 0, 'outlier_rate': 0
            })
    
    # 銘柄別詳細データ
    symbol_details = []
    for symbol in symbols:
        if symbol in symbols_performance:
            perf = symbols_performance[symbol]
            symbol_details.append({
                'symbol': symbol,
                'total_pnl': perf['total_pnl'],
                'trade_count': perf['trades'],
                'avg_return': np.mean(perf['returns']) if perf['returns'] else 0
            })
        else:
            symbol_details.append({
                'symbol': symbol,
                'total_pnl': 0,
                'trade_count': 0,
                'avg_return': 0
            })
    
    return stats_data, symbol_details


def _save_statistics_files(stats_data, symbol_details):
    """統計データをCSVファイルに保存"""
    try:
        stats_df = pd.DataFrame([stats_data])
        stats_file_path = os.path.join(MULTI_STOCK_DIR, 'detailed_statistics.csv')
        stats_df.to_csv(stats_file_path, index=False)
        print(f"💾 詳細統計を '{stats_file_path}' に保存しました")
        print(f"   データサイズ: {len(stats_df)} 行, {len(stats_df.columns)} 列")
    except Exception as e:
        print(f"❌ 詳細統計CSVの保存に失敗: {e}")
    
    try:
        symbol_details_df = pd.DataFrame(symbol_details)
        symbol_file_path = os.path.join(MULTI_STOCK_DIR, 'symbol_performance.csv')
        symbol_details_df.to_csv(symbol_file_path, index=False)
        print(f"💾 銘柄別パフォーマンスを '{symbol_file_path}' に保存しました")
        print(f"   データサイズ: {len(symbol_details_df)} 行, {len(symbol_details_df.columns)} 列")
    except Exception as e:
        print(f"❌ 銘柄別パフォーマンスCSVの保存に失敗: {e}")


def _display_statistics_summary(stats_data):
    """統計サマリーをコンソールに表示"""
    print(f"\n{'='*80}")
    print(f"📊 **詳細統計サマリー** (CSVファイルとして保存済み)")
    print(f"{'='*80}")
    
    # 主要統計をコンソールに表示
    if stats_data['coefficient_of_variation'] != float('inf'):
        cv_status = "✅ 安定" if stats_data['coefficient_of_variation'] < 1.0 else "❌ 不安定"
        print(f"🎯 **安定性評価**: {cv_status} (変動係数: {stats_data['coefficient_of_variation']:.2f})")
    
    print(f"💰 **勝率**: {stats_data['win_rate']:.1%}")
    print(f"📈 **リターン標準偏差**: {stats_data['return_std']:.2%}")
    print(f"💵 **損益標準偏差**: ${stats_data['profit_std']:,.2f}")
    print(f"🏢 **銘柄別損益標準偏差**: ${stats_data['symbol_pnl_std']:,.2f}")
    print(f"⚠️ **外れ値率**: {stats_data['outlier_rate']:.1%}")
    # 外れ値率の意味: 0-10%(安定) 10-20%(概ね安定) 20-30%(やや不安定) 30%+(課題あり)
    print(f"{'='*80}")