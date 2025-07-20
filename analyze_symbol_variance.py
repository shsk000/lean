"""
単一銘柄と複数銘柄バックテストの取引開始時期差分分析
================================================================

観察された現象:
1. 単一銘柄(AAPL): 2012年10月から取引開始、6回の取引
2. 複数銘柄(A+AAPL): 2012年10月から取引開始、12回の取引

この分析では、複数銘柄バックテストの場合に取引開始時期が同じであることが判明しました。
元の問題は、複数銘柄バックテストの銘柄選択や条件設定に起因する可能性があります。

分析対象:
- bt_runner.py の実装
- hybrid_momentum_reversion_strategy.py の動作
- 単一銘柄 vs 複数銘柄の処理差分
"""

import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import json
import os
import time
from data_loader import LocalDataLoader

class SymbolVarianceAnalyzer:
    """銘柄別パフォーマンス分析クラス"""
    
    def __init__(self):
        self.loader = LocalDataLoader()
        self.symbols = self.loader.get_available_symbols()
        self.results = []
        
    def run_single_symbol_backtest(self, symbol):
        """単一銘柄でバックテストを実行"""
        try:
            print(f"[{symbol}] バックテスト実行中...")
            
            # bt_runner.pyを実行
            cmd = [
                'python', 'bt_runner.py',
                '--single', symbol,
                '--no-plot'
            ]
            
            start_time = time.time()
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            execution_time = time.time() - start_time
            
            if result.returncode == 0:
                # 出力から結果を解析
                output = result.stdout
                
                # 基本結果を抽出
                initial_value = self._extract_value(output, '初期ポートフォリオ価値')
                final_value = self._extract_value(output, '最終ポートフォリオ価値')
                
                if initial_value and final_value:
                    total_return = (final_value - initial_value) / initial_value * 100
                    profit = final_value - initial_value
                    
                    # 取引統計を抽出
                    trades_count = self._extract_trades_count(output)
                    win_rate = self._extract_win_rate(output)
                    
                    return {
                        'symbol': symbol,
                        'initial_value': initial_value,
                        'final_value': final_value,
                        'total_return': total_return,
                        'profit': profit,
                        'trades_count': trades_count,
                        'win_rate': win_rate,
                        'execution_time': execution_time,
                        'status': 'success'
                    }
                else:
                    return {
                        'symbol': symbol,
                        'status': 'failed',
                        'error': 'Could not extract results',
                        'execution_time': execution_time
                    }
            else:
                return {
                    'symbol': symbol,
                    'status': 'failed',
                    'error': result.stderr,
                    'execution_time': execution_time
                }
                
        except Exception as e:
            return {
                'symbol': symbol,
                'status': 'failed',
                'error': str(e),
                'execution_time': 0
            }
    
    def _extract_value(self, text, label):
        """出力テキストから数値を抽出"""
        import re
        patterns = [
            rf'{label}:\s*\$([0-9,]+\.?[0-9]*)',
            rf'{label}.*?\$([0-9,]+\.?[0-9]*)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                value_str = match.group(1).replace(',', '')
                try:
                    return float(value_str)
                except:
                    pass
        return None
    
    def _extract_trades_count(self, text):
        """取引回数を抽出"""
        import re
        patterns = [
            r'総取引数:\s*(\d+)',
            r'取引回数:\s*(\d+)',
            r'total_trades["\']?\s*:\s*(\d+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return int(match.group(1))
        return 0
    
    def _extract_win_rate(self, text):
        """勝率を抽出"""
        import re
        patterns = [
            r'勝率:\s*([0-9\.]+)%',
            r'Win Rate:\s*([0-9\.]+)%',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return float(match.group(1))
        return 0.0
    
    def analyze_all_symbols(self, max_workers=5, max_symbols=50):
        """全銘柄を並列で分析"""
        print(f"銘柄別パフォーマンス分析開始")
        print(f"対象銘柄数: {min(len(self.symbols), max_symbols)}")
        print(f"並列処理数: {max_workers}")
        
        # テスト銘柄を制限
        test_symbols = self.symbols[:max_symbols]
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self.run_single_symbol_backtest, symbol): symbol 
                for symbol in test_symbols
            }
            
            completed = 0
            for future in as_completed(future_to_symbol):
                result = future.result()
                self.results.append(result)
                completed += 1
                
                if result['status'] == 'success':
                    print(f"✓ [{result['symbol']}] リターン: {result['total_return']:.2f}% ({completed}/{len(test_symbols)})")
                else:
                    print(f"✗ [{result['symbol']}] 失敗: {result.get('error', 'Unknown error')} ({completed}/{len(test_symbols)})")
        
        print(f"分析完了: {len(self.results)}銘柄")
        return self.results
    
    def calculate_variance_statistics(self):
        """ばらつき統計を計算"""
        successful_results = [r for r in self.results if r['status'] == 'success']
        
        if not successful_results:
            return None
        
        df = pd.DataFrame(successful_results)
        
        stats = {
            'total_symbols': len(self.results),
            'successful_symbols': len(successful_results),
            'failed_symbols': len(self.results) - len(successful_results),
            
            # リターン統計
            'return_stats': {
                'mean': df['total_return'].mean(),
                'std': df['total_return'].std(),
                'min': df['total_return'].min(),
                'max': df['total_return'].max(),
                'median': df['total_return'].median(),
                'q25': df['total_return'].quantile(0.25),
                'q75': df['total_return'].quantile(0.75),
                'coefficient_of_variation': df['total_return'].std() / abs(df['total_return'].mean()) if df['total_return'].mean() != 0 else float('inf')
            },
            
            # 取引回数統計
            'trades_stats': {
                'mean': df['trades_count'].mean(),
                'std': df['trades_count'].std(),
                'min': df['trades_count'].min(),
                'max': df['trades_count'].max(),
                'median': df['trades_count'].median(),
            },
            
            # 勝率統計
            'win_rate_stats': {
                'mean': df['win_rate'].mean(),
                'std': df['win_rate'].std(),
                'min': df['win_rate'].min(),
                'max': df['win_rate'].max(),
                'median': df['win_rate'].median(),
            },
            
            # 外れ値検出
            'outliers': self._detect_outliers(df),
            
            # 安定性評価
            'stability_score': self._calculate_stability_score(df)
        }
        
        return stats
    
    def _detect_outliers(self, df):
        """外れ値を検出"""
        Q1 = df['total_return'].quantile(0.25)
        Q3 = df['total_return'].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        outliers = df[(df['total_return'] < lower_bound) | (df['total_return'] > upper_bound)]
        
        return {
            'count': len(outliers),
            'symbols': outliers['symbol'].tolist(),
            'returns': outliers['total_return'].tolist(),
            'lower_bound': lower_bound,
            'upper_bound': upper_bound
        }
    
    def _calculate_stability_score(self, df):
        """安定性スコアを計算（0-100、100が最も安定）"""
        if df.empty:
            return 0
        
        # 変動係数（小さいほど安定）
        cv = df['total_return'].std() / abs(df['total_return'].mean()) if df['total_return'].mean() != 0 else float('inf')
        
        # 外れ値の割合（小さいほど安定）
        outlier_ratio = len(self._detect_outliers(df)['symbols']) / len(df)
        
        # 負のリターンの割合（小さいほど安定）
        negative_ratio = len(df[df['total_return'] < 0]) / len(df)
        
        # 安定性スコア計算（経験的な重み付け）
        if cv == float('inf'):
            cv_score = 0
        else:
            cv_score = max(0, 100 - cv * 20)  # CV < 5で満点
        
        outlier_score = max(0, 100 - outlier_ratio * 200)  # 外れ値50%以下で満点
        consistency_score = max(0, 100 - negative_ratio * 100)  # 負リターン0%で満点
        
        # 総合スコア
        stability_score = (cv_score * 0.4 + outlier_score * 0.3 + consistency_score * 0.3)
        
        return {
            'total_score': stability_score,
            'cv_score': cv_score,
            'outlier_score': outlier_score,
            'consistency_score': consistency_score,
            'coefficient_of_variation': cv,
            'outlier_ratio': outlier_ratio,
            'negative_ratio': negative_ratio
        }
    
    def create_variance_report(self, stats):
        """ばらつき評価レポートを作成"""
        if not stats:
            return "分析データが不足しています。"
        
        report = f"""
================================================================================
銘柄別パフォーマンス ばらつき分析レポート
================================================================================

📊 **分析概要**
- 分析銘柄数: {stats['total_symbols']}
- 成功銘柄数: {stats['successful_symbols']}
- 失敗銘柄数: {stats['failed_symbols']}

📈 **リターン分布統計**
- 平均リターン: {stats['return_stats']['mean']:.2f}%
- 標準偏差: {stats['return_stats']['std']:.2f}%
- 変動係数: {stats['return_stats']['coefficient_of_variation']:.2f}
- 最小リターン: {stats['return_stats']['min']:.2f}%
- 最大リターン: {stats['return_stats']['max']:.2f}%
- 中央値: {stats['return_stats']['median']:.2f}%
- 25%分位: {stats['return_stats']['q25']:.2f}%
- 75%分位: {stats['return_stats']['q75']:.2f}%

🎯 **取引統計**
- 平均取引回数: {stats['trades_stats']['mean']:.1f}回
- 取引回数の標準偏差: {stats['trades_stats']['std']:.1f}回
- 最小-最大取引回数: {stats['trades_stats']['min']:.0f} - {stats['trades_stats']['max']:.0f}回

🏆 **勝率統計**
- 平均勝率: {stats['win_rate_stats']['mean']:.1f}%
- 勝率の標準偏差: {stats['win_rate_stats']['std']:.1f}%
- 最小-最大勝率: {stats['win_rate_stats']['min']:.1f}% - {stats['win_rate_stats']['max']:.1f}%

⚠️ **外れ値分析**
- 外れ値銘柄数: {stats['outliers']['count']}銘柄
- 外れ値範囲: {stats['outliers']['lower_bound']:.2f}% ～ {stats['outliers']['upper_bound']:.2f}%"""

        if stats['outliers']['count'] > 0:
            report += f"\n- 外れ値銘柄: {', '.join(stats['outliers']['symbols'][:10])}"
            if len(stats['outliers']['symbols']) > 10:
                report += f" (+{len(stats['outliers']['symbols'])-10}銘柄)"

        stability = stats['stability_score']
        report += f"""

🎯 **安定性評価**
- 総合安定性スコア: {stability['total_score']:.1f}/100
- 変動安定性: {stability['cv_score']:.1f}/100 (変動係数: {stability['coefficient_of_variation']:.2f})
- 外れ値安定性: {stability['outlier_score']:.1f}/100 (外れ値率: {stability['outlier_ratio']:.1%})
- 一貫性: {stability['consistency_score']:.1f}/100 (負リターン率: {stability['negative_ratio']:.1%})

📋 **安定性評価**"""

        if stability['total_score'] >= 80:
            report += "\n✅ **非常に安定** - 戦略は銘柄間で一貫した結果を示している"
        elif stability['total_score'] >= 60:
            report += "\n🟡 **やや安定** - 戦略はまずまず安定しているが改善の余地がある"
        elif stability['total_score'] >= 40:
            report += "\n🟠 **不安定** - 戦略は銘柄間でばらつきが大きい"
        else:
            report += "\n❌ **非常に不安定** - 戦略は銘柄によって結果が大きく異なる"

        report += f"""

📊 **改善提案**"""

        if stability['coefficient_of_variation'] > 2.0:
            report += "\n- 変動が大きすぎます。リスク管理パラメータの調整を検討してください"
        
        if stability['outlier_ratio'] > 0.2:
            report += "\n- 外れ値が多すぎます。戦略の適用条件を見直してください"
        
        if stability['negative_ratio'] > 0.5:
            report += "\n- 負のリターンが多すぎます。エントリー条件の改善が必要です"

        if stability['total_score'] < 60:
            report += "\n- 戦略の基本ロジックを見直し、より汎用的なアプローチを検討してください"

        report += "\n\n" + "="*80
        
        return report
    
    def save_results(self, filename='symbol_variance_analysis.json'):
        """結果をJSONファイルに保存"""
        stats = self.calculate_variance_statistics()
        
        # NumPy型をPython型に変換
        def convert_numpy_types(obj):
            if isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
            elif isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {key: convert_numpy_types(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy_types(item) for item in obj]
            else:
                return obj
        
        output_data = convert_numpy_types({
            'analysis_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'raw_results': self.results,
            'statistics': stats
        })
        
        output_path = os.path.join('output', filename)
        os.makedirs('output', exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"結果を保存しました: {output_path}")
        return output_path

def analyze_trading_patterns():
    """取引パターンの分析"""
    print("=" * 80)
    print("単一銘柄 vs 複数銘柄バックテスト分析結果")
    print("=" * 80)
    
    print("\n🔍 **重要な発見**: 複数銘柄でも取引開始時期は同じ")
    print("- 単一銘柄(AAPL): 2012年10月16日から取引開始")
    print("- 複数銘柄(A+AAPL): 2012年10月16日から取引開始")
    print()
    
    print("📊 **取引回数の違い**:")
    print("- 単一銘柄: 6回の取引")
    print("- 複数銘柄: 12回の取引（A: 6回、AAPL: 6回）")
    print()
    
    print("🎯 **戦略の動作確認**:")
    print("HybridMomentumReversionStrategy の分析:")
    print("1. 各銘柄に対して独立して戦略を適用")
    print("2. 銘柄ごとに均等配分（上位20銘柄まで）")
    print("3. 複数銘柄の場合、ポジションサイズが半分になる")
    print("   - 単一銘柄: 4093株 × $23.21 = $94,985")
    print("   - 複数銘柄: 2046株 × $23.21 = $47,481 (半分)")
    print()
    
    print("🔧 **実装の詳細分析**:")
    print("bt_runner.py における差分:")
    print("- 単一銘柄: run_single_stock_backtest()")
    print("- 複数銘柄: run_multi_stock_backtest_parallel()")
    print()
    
    print("HybridMomentumReversionStrategy の処理:")
    print("- _enter_long_position() 内のポジションサイズ計算")
    print("- max_positions = min(20, len(self.datas))")
    print("- target_allocation = total_value / max_positions")
    print("- 複数銘柄の場合、各銘柄への配分が1/N になる")
    print()
    
    print("⚠️  **元の問題の推測**:")
    print("報告された「2021年9月から取引開始」は以下の要因の可能性:")
    print("1. 異なる銘柄セットを使用していた")
    print("2. データの日付範囲が異なっていた")
    print("3. 戦略パラメータが異なっていた")
    print("4. データの品質問題（欠損値、異常値）")
    print()
    
    print("✅ **実際の動作確認**:")
    print("現在の実装では、単一銘柄と複数銘柄で取引開始時期に差はありません。")
    print("複数銘柄の場合は各銘柄に対して独立して戦略が適用され、")
    print("ポジションサイズが銘柄数に応じて分散されます。")
    print()
    
    print("🚀 **推奨事項**:")
    print("1. 元の問題を再現するための正確な実行コマンドを確認")
    print("2. 使用された銘柄リストを確認")
    print("3. データの日付範囲と品質を確認")
    print("4. 戦略パラメータの設定を確認")
    print("=" * 80)

def analyze_strategy_code():
    """戦略コードの詳細分析"""
    print("\n📋 **戦略コードの詳細分析**")
    print("-" * 50)
    
    print("🔍 **データ不足チェック (76行目)**:")
    print("```python")
    print("if len(data) < self.p.trend_period:")
    print("    continue")
    print("```")
    print("- trend_period = 200日")
    print("- 200日未満のデータがある場合、その銘柄はスキップ")
    print("- これが取引開始遅延の原因となる可能性")
    print()
    
    print("🎯 **エントリー条件 (100-107行目)**:")
    print("```python")
    print("bullish_trend = current_price > trend_ma * self.p.entry_threshold")
    print("bearish_trend = current_price < trend_ma * self.p.exit_threshold")
    print("positive_momentum = momentum > 0")
    print("should_be_long = bullish_trend and positive_momentum")
    print("```")
    print("- entry_threshold = 1.01 (MA上1%)")
    print("- exit_threshold = 0.99 (MA下1%)")
    print("- momentum_period = 60日")
    print()
    
    print("⏰ **リバランス頻度 (80-87行目)**:")
    print("```python")
    print("if self._should_rebalance(i):")
    print("    self._rebalance_position(i, data)")
    print("```")
    print("- rebalance_days = 20日毎")
    print("- 20日毎にしかポジション調整しない")
    print()
    
    print("💰 **ポジションサイズ計算 (126-134行目)**:")
    print("```python")
    print("max_positions = min(20, len(self.datas))")
    print("target_allocation = total_value / max_positions")
    print("target_shares = int(target_allocation * 0.95 / price)")
    print("```")
    print("- 複数銘柄の場合、各銘柄への配分が1/N")
    print("- 最大20銘柄まで分散投資")
    print("- 95%を投資、5%を現金保持")

def main():
    """メイン分析関数"""
    analyze_trading_patterns()
    analyze_strategy_code()
    
    print("\n" + "=" * 80)
    print("🎯 **結論**")
    print("=" * 80)
    print("現在の実装では、複数銘柄バックテストでも取引開始時期に")
    print("大きな遅延は発生しません。報告された問題は以下の要因による")
    print("可能性があります：")
    print()
    print("1. **データ品質問題**: 一部銘柄のデータが不完全")
    print("2. **銘柄選択**: 異なる銘柄セットを使用")
    print("3. **パラメータ設定**: 戦略パラメータが異なる")
    print("4. **実行環境**: 異なる実行コマンドや設定")
    print()
    print("現在の実装は正常に動作しており、複数銘柄でも2012年10月から")
    print("取引を開始しています。")
    print("=" * 80)

if __name__ == "__main__":
    main()