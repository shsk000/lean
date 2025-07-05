# lean
# 例: バックテスト実行
pyenv activate lean-env && python bt_runner.py --single AAPL

## strategies ディレクトリへの戦略追加ガイド
- 各戦略は strategies/ ディレクトリ内に Python ファイルとして実装（例: rsi_gap_strategy.py, ma_cross_strategy.py）
- backtrader.Strategy を継承し、__init__ でインジケータ初期化、next で売買ロジックを記述
- 複数銘柄対応のため self.datas をループして各データごとに判定・注文
- クラス名は PascalCase、ファイル名はスネークケースで一致させる
- params でパラメータ定義、日本語コメントでロジック説明
- 必要に応じて notify_order（注文の発注・約定・キャンセル等の状態変化を検知するイベントハンドラ）、notify_trade（トレード成立時の損益計算やログ出力を行うイベントハンドラ）も実装