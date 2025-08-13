# Lean Trading System

高性能な Python バックテスト・アルゴリズム取引システム。[Backtrader](https://www.backtrader.com/) フレームワークをベースに、S&P500銘柄を対象とした戦略検証とリアルタイム分析を提供します。

## 🚀 主要機能

### バックテスト機能
- **単一銘柄バックテスト**: 個別銘柄での戦略検証
- **複数銘柄バックテスト**: ポートフォリオレベルでの分析
- **動的戦略選択**: 実行時に戦略を指定可能
- **詳細統計分析**: 勝率、リターン、最大ドローダウンなど包括的な統計情報

### データ管理
- **自動データ収集**: S&P500銘柄の株価データを自動取得・更新
- **ローカルキャッシュ**: 高速アクセスのための効率的なデータストレージ
- **データ整合性チェック**: 欠損データや異常値の自動検出・修正

### 出力・可視化
- **詳細レポート**: HTML/CSV形式での包括的な分析レポート
- **チャート表示**: matplotlib を使用した価格・指標チャート
- **統計エクスポート**: Excel 対応の詳細統計データ

## 📋 システム要件

- Python 3.11+ (推奨: 3.12.11)
- 主要パッケージ: pandas 2.3.0, numpy 2.3.1, backtrader, matplotlib
- OS: macOS, Linux, Windows

## ⚡ クイックスタート

### 1. 環境セットアップ
```bash
# Python仮想環境の作成・有効化
pyenv virtualenv 3.12.11 lean-env
pyenv activate lean-env

# 依存関係のインストール
pip install -r requirements.txt
```

### 2. データ収集（初回のみ）
```bash
# S&P500銘柄データの収集
python data_collector.py
```

### 3. バックテスト実行

#### 単一銘柄テスト
```bash
# AAPL株でハイブリッド戦略をテスト
python bt_runner.py --single AAPL --strategy hybrid_momentum_reversion_strategy

# チャート表示なしで高速実行
python bt_runner.py --single MSFT --strategy hybrid_momentum_reversion_strategy --no-plot
```

#### 複数銘柄テスト
```bash
# 上位5銘柄でポートフォリオテスト
python bt_runner.py --limit 5 --strategy hybrid_momentum_reversion_strategy

# 全銘柄でのフルテスト
python bt_runner.py --strategy hybrid_momentum_reversion_strategy
```

## 📊 出力例

### バックテスト結果
```
============================================================
📊 バックテスト結果サマリー (AAPL)
============================================================
銘柄: AAPL
期間: 2012-01-01 から 2022-12-31
初期ポートフォリオ価値: $100,000.00
最終ポートフォリオ価値: $469,944.37
総取引回数: 6
未決済ポジション: 0
------------------------------------------------------------
💰 利益: +$369,944.37 (+369.94%)
✅ 戦略は利益を上げました！
```

### 取引履歴詳細
```
🟢 取引  1: 2013-09-05 → 2015-08-31
    投資額: $78,056.87 (株数: 4,369)
    価格: $17.87 → $28.19
    損益: $44,307.90 (+56.76%) | 理由: strategy_exit
```

### 出力ファイル
- `output/single_stocks/trades_history_AAPL.csv` - 個別取引履歴
- `output/single_stocks/detailed_statistics_AAPL.csv` - 詳細統計
- `output/multi_stocks/multi_stock_backtest_results.csv` - 複数銘柄結果

## 🎯 利用可能な戦略

### 実装済み戦略

1. **HybridMomentumReversionStrategy** (`hybrid_momentum_reversion_strategy.py`)
   - トレンドフォロー + モメンタムリバージョン
   - 200日移動平均ベースのトレンド判定
   - ボラティリティ調整付きポジションサイジング

2. **RSIGapStrategy** (`rsi_gap_strategy.py`)
   - RSI逆張り戦略
   - ギャップアップ/ダウン時のエントリー
   - 利確・損切り機能付き

3. **USShortTermStrategy** (`us_short_term.py`)
   - 短期取引戦略（基本版）
   - 複数テクニカル指標の組み合わせ
   - リスク管理機能付き

4. **USShortTermImproved** (`us_short_term_improved.py`)
   - 短期取引戦略（改善版）
   - エントリー条件の緩和とエグジット条件の多様化
   - 固定損切り・利確機能付き
   - 市場環境適応型パラメータ調整

## 🛠️ カスタム戦略の追加

### strategies ディレクトリへの戦略追加ガイド

1. **ファイル配置**: `strategies/` ディレクトリ内に Python ファイルを作成
2. **クラス継承**: `backtrader.Strategy` を継承
3. **命名規則**:
   - クラス名: PascalCase (例: `MyCustomStrategy`)
   - ファイル名: スネークケース (例: `my_custom_strategy.py`)

### 基本実装テンプレート
```python
import backtrader as bt

class MyCustomStrategy(bt.Strategy):
    params = (
        ('param1', 10),      # パラメータ定義
        ('param2', 0.02),
    )

    def __init__(self):
        # インジケータの初期化
        self.sma = bt.indicators.SMA(period=self.p.param1)
        self.trades = []     # 取引履歴記録用

    def next(self):
        # 売買ロジック
        if not self.position:
            if self.data.close[0] > self.sma[0]:
                self.buy()
        else:
            if self.data.close[0] < self.sma[0]:
                self.sell()

    def notify_trade(self, trade):
        # 取引完了時の処理
        if trade.isclosed:
            # 取引履歴の記録
            pass
```

## 📁 プロジェクト構造

```
lean/
├── README.md                     # このファイル
├── requirements.txt              # Python依存関係
├── bt_runner.py                 # メインバックテストランナー
├── data_collector.py            # データ収集スクリプト
├── backtest_runner.py           # バックテスト実行エンジン
├── statistics_calculator.py     # 統計計算・レポート生成
├── file_manager.py              # ファイル操作ユーティリティ
├── data/                        # データストレージ
│   ├── stocks/                  # 個別銘柄データ (CSV)
│   └── config/                  # 設定ファイル
├── strategies/                  # 取引戦略実装
│   ├── hybrid_momentum_reversion_strategy.py
│   ├── rsi_gap_strategy.py
│   ├── us_short_term.py
│   └── us_short_term_improved.py
└── output/                      # 実行結果出力
    ├── single_stocks/           # 単一銘柄結果
    └── multi_stocks/            # 複数銘柄結果
```

## 🔧 設定・カスタマイズ

### コマンドライン引数
```bash
python bt_runner.py [OPTIONS]

OPTIONS:
  --single SYMBOL          単一銘柄テスト (例: AAPL, MSFT)
  --limit N               テスト銘柄数を制限 (例: --limit 10)
  --strategy STRATEGY     使用戦略指定 (例: hybrid_momentum_reversion_strategy)
  --start-date YYYY-MM-DD バックテスト開始日
  --end-date YYYY-MM-DD   バックテスト終了日
  --no-plot               チャート表示を無効化 (高速実行)
```

### 設定ファイル
- データ期間、初期資金、手数料などは各スクリプト内で設定可能
- 銘柄リストは `data_collector.py` で管理

## 📈 パフォーマンス・統計

### 計算される統計指標
- **基本統計**: 総リターン、年率リターン、勝率
- **リスク指標**: 最大ドローダウン、シャープレシオ、標準偏差
- **取引統計**: 平均利益/損失、最大利益/損失、取引回数
- **時系列分析**: 月次・年次リターン、取引期間分析

### 検証健全性機能
- データ整合性の自動チェック
- 日付フォーマットの統一
- 取引サイズの正確な記録
- 価格データの妥当性検証

## 🐛 トラブルシューティング

### よくある問題と解決法

1. **Python バージョンエラー**
   ```bash
   # Python 3.11+ を使用
   pyenv install 3.12.11
   pyenv global 3.12.11
   ```

2. **データ取得エラー**
   ```bash
   # データを再収集
   python data_collector.py
   ```

3. **パッケージ依存エラー**
   ```bash
   # 依存関係を再インストール
   pip install -r requirements.txt --force-reinstall
   ```

4. **メモリ不足エラー**
   ```bash
   # 銘柄数を制限して実行
   python bt_runner.py --limit 5 --strategy [戦略名]
   ```

## 🤝 貢献

1. Issue の報告や新機能の提案
2. 新しい取引戦略の実装・共有
3. パフォーマンス改善・バグ修正
4. ドキュメント改善

## 📄 ライセンス

このプロジェクトはオープンソースです。商用・非商用問わず自由に使用・改変できます。

## ⚠️ 免責事項

このシステムは教育・研究目的で開発されています。実際の投資判断には使用せず、投資リスクを十分理解した上でご利用ください。過去の実績は将来の運用成果を保証するものではありません。