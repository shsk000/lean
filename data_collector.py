"""
データ収集・管理ツール (Data Collector)
=====================================

10年間の高出来高株式データをローカルに保存し、
バックテスト実行時の信頼性を確保するためのツールです。

機能:
1. S&P500銘柄から高出来高株を選定
2. 2012-2022年の10年間OHLCV データをダウンロード
3. CSV形式でローカルに保存
4. 設定ファイル（config.json）とシンボルリスト（high_volume_symbols.json）を生成

実行方法:
--------
python data_collector.py [--max-symbols 300] [--min-volume 1000000]

出力ファイル構造:
--------------
data/
├── config.json                    # データ設定情報
├── high_volume_symbols.json       # 高出来高銘柄リスト
└── stocks/                        # 個別銘柄CSVファイル
    ├── AAPL.csv
    ├── MSFT.csv
    └── ...
"""

import yfinance as yf
import pandas as pd
import json
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# データ保存設定
DATA_DIR = 'data'
STOCKS_DIR = os.path.join(DATA_DIR, 'stocks')
CONFIG_FILE = os.path.join(DATA_DIR, 'config.json')
SYMBOLS_FILE = os.path.join(DATA_DIR, 'high_volume_symbols.json')

# データ期間設定（10年間）
START_DATE = '2012-01-01'
END_DATE = '2022-12-31'

def ensure_directories():
    """必要なディレクトリを作成"""
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(STOCKS_DIR, exist_ok=True)
    print(f"データディレクトリを確認: {DATA_DIR}")
    print(f"株式データディレクトリを確認: {STOCKS_DIR}")

def get_sp500_symbols():
    """S&P500銘柄リストを取得（完全決定的版）"""
    print("S&P 500銘柄リストを取得中...")
    
    try:
        # SP500のティッカー情報を取得
        sp500 = yf.Ticker("^GSPC")
        # より確実な方法として、Wikipediaから取得
        tables = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        sp500_table = tables[0]
        symbols = sp500_table['Symbol'].tolist()
        
        # ティッカーシンボルをクリーンアップ
        cleaned_symbols = []
        for symbol in symbols:
            # BRK.B → BRK-B のような変換
            cleaned_symbol = symbol.replace('.', '-')
            cleaned_symbols.append(cleaned_symbol)
        
        # アルファベット順でソート（決定的結果のため）
        cleaned_symbols = sorted(cleaned_symbols)
        
        print(f"S&P 500銘柄数: {len(cleaned_symbols)}")
        return cleaned_symbols
        
    except Exception as e:
        print(f"S&P 500銘柄取得エラー: {e}")
        # フォールバック：主要銘柄のリスト
        fallback_symbols = [
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META', 'BRK-B',
            'V', 'JNJ', 'WMT', 'XOM', 'UNH', 'JPM', 'PG', 'MA', 'HD', 'CVX',
            'ABBV', 'BAC', 'PFE', 'KO', 'AVGO', 'PEP', 'TMO', 'COST', 'MRK',
            'DHR', 'VZ', 'ABT', 'ACN', 'LLY', 'TXN', 'WFC', 'NEE', 'NKE',
            'AMD', 'DIS', 'ADBE', 'CRM', 'BMY', 'T', 'UNP', 'PM', 'RTX'
        ]
        print(f"フォールバック銘柄リストを使用: {len(fallback_symbols)}銘柄")
        return sorted(fallback_symbols)

def download_stock_data(symbol, start_date, end_date, max_retries=3):
    """個別銘柄の株価データをダウンロード"""
    for attempt in range(max_retries):
        try:
            print(f"[{symbol}] データダウンロード中... (試行 {attempt + 1}/{max_retries})")
            
            ticker = yf.Ticker(symbol)
            data = ticker.history(start=start_date, end=end_date, auto_adjust=False)
            
            if data.empty:
                print(f"[{symbol}] ✗ データが空です")
                continue
                
            if len(data) < 100:  # 最低100日のデータが必要
                print(f"[{symbol}] ✗ データが不足 ({len(data)}日)")
                continue
            
            # カラム名を標準化
            data = data.rename(columns={
                'Open': 'Open',
                'High': 'High', 
                'Low': 'Low',
                'Close': 'Close',
                'Volume': 'Volume',
                'Adj Close': 'Adj_Close'
            })
            
            # 必要なカラムが存在するかチェック
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            if not all(col in data.columns for col in required_columns):
                print(f"[{symbol}] ✗ 必要なカラムが不足")
                continue
            
            # 出来高の統計を計算
            avg_volume = data['Volume'].mean()
            median_volume = data['Volume'].median()
            
            # データをCSVファイルに保存
            csv_path = os.path.join(STOCKS_DIR, f"{symbol}.csv")
            data.to_csv(csv_path)
            
            print(f"[{symbol}] ✓ 保存完了 ({len(data)}日, 平均出来高: {avg_volume:,.0f})")
            
            return {
                'symbol': symbol,
                'data_points': len(data),
                'start_date': data.index.min().strftime('%Y-%m-%d'),
                'end_date': data.index.max().strftime('%Y-%m-%d'),
                'avg_volume': float(avg_volume),
                'median_volume': float(median_volume),
                'csv_path': csv_path
            }
            
        except Exception as e:
            print(f"[{symbol}] ✗ エラー (試行 {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)  # 2秒待機してリトライ
            
    print(f"[{symbol}] ✗ 最大試行回数に達しました。スキップします。")
    return None

def filter_high_volume_stocks_sequential(symbols, min_avg_volume=1000000, max_symbols=300):
    """逐次処理で高出来高株を選定"""
    print(f"\n高出来高株の選定中... (最小平均出来高: {min_avg_volume:,}, 最大銘柄数: {max_symbols})")
    
    high_volume_stocks = []
    processed_count = 0
    
    for symbol in symbols:
        if len(high_volume_stocks) >= max_symbols:
            break
            
        processed_count += 1
        result = download_stock_data(symbol, START_DATE, END_DATE)
        
        if result and result['avg_volume'] >= min_avg_volume:
            high_volume_stocks.append(result)
            print(f"[進捗] {len(high_volume_stocks)}/{max_symbols} 銘柄選定完了")
        
        # 進捗表示
        if processed_count % 10 == 0:
            print(f"[進捗] {processed_count}/{len(symbols)} 銘柄を処理済み")
    
    print(f"\n選定完了: {len(high_volume_stocks)} 銘柄")
    return high_volume_stocks

def save_configuration(symbols_data):
    """設定ファイルとシンボルリストを保存"""
    # config.json を作成
    config = {
        'data_collection_date': datetime.now().isoformat(),
        'data_period': {
            'start_date': START_DATE,
            'end_date': END_DATE
        },
        'total_symbols': len(symbols_data),
        'data_directory': STOCKS_DIR,
        'description': '10年間の高出来高株式OHLCV データ (2012-2022)',
        'version': '1.0'
    }
    
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    print(f"✓ 設定ファイル保存: {CONFIG_FILE}")
    
    # high_volume_symbols.json を作成
    symbols_list = [data['symbol'] for data in symbols_data]
    
    symbols_info = {
        'symbols': symbols_list,
        'total_count': len(symbols_list),
        'selection_criteria': {
            'min_avg_volume': 1000000,
            'data_period': f"{START_DATE} to {END_DATE}",
            'source': 'S&P 500'
        },
        'data_details': symbols_data
    }
    
    with open(SYMBOLS_FILE, 'w', encoding='utf-8') as f:
        json.dump(symbols_info, f, indent=2, ensure_ascii=False)
    
    print(f"✓ シンボルリスト保存: {SYMBOLS_FILE}")

def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description='株式データ収集ツール')
    parser.add_argument('--max-symbols', type=int, default=300, help='最大銘柄数 (デフォルト: 300)')
    parser.add_argument('--min-volume', type=int, default=1000000, help='最小平均出来高 (デフォルト: 1,000,000)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("株式データ収集ツール")
    print("=" * 60)
    print(f"データ期間: {START_DATE} から {END_DATE} (10年間)")
    print(f"最大銘柄数: {args.max_symbols}")
    print(f"最小平均出来高: {args.min_volume:,}")
    print("=" * 60)
    
    start_time = time.time()
    
    try:
        # 1. ディレクトリ作成
        ensure_directories()
        
        # 2. S&P500銘柄取得
        symbols = get_sp500_symbols()
        
        # 3. 高出来高株の選定とダウンロード
        high_volume_data = filter_high_volume_stocks_sequential(
            symbols, 
            min_avg_volume=args.min_volume,
            max_symbols=args.max_symbols
        )
        
        if not high_volume_data:
            print("✗ 選定された銘柄がありません。終了します。")
            return
        
        # 4. 設定ファイル保存
        save_configuration(high_volume_data)
        
        # 5. 結果サマリー
        total_time = time.time() - start_time
        
        print("\n" + "=" * 60)
        print("データ収集完了")
        print("=" * 60)
        print(f"収集銘柄数: {len(high_volume_data)}")
        print(f"データ期間: {START_DATE} から {END_DATE}")
        print(f"処理時間: {total_time:.1f}秒")
        print(f"保存場所: {STOCKS_DIR}")
        print("=" * 60)
        
        # 出来高統計
        volumes = [data['avg_volume'] for data in high_volume_data]
        print(f"平均出来高統計:")
        print(f"  - 最小: {min(volumes):,.0f}")
        print(f"  - 最大: {max(volumes):,.0f}")
        print(f"  - 平均: {sum(volumes)/len(volumes):,.0f}")
        
    except Exception as e:
        print(f"✗ データ収集中にエラーが発生しました: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())